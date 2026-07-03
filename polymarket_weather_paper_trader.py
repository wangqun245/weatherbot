#!/usr/bin/env python3
"""
Deterministic Polymarket weather trader.

Only keeps the current strategy:
- Receive Jack AI Solutions airport weather records by websocket.
- Use returned observation time and temperature records as the trigger signal.
- Verify changed observed extremes with TWC historical observations.
- Buy NO when an option is already impossible, buy YES when the extreme bucket is reached.
- Sell held NO only if corrected observations make that NO possible again.

By default this runs as a paper trader. If live trading is explicitly enabled
in config, real Polymarket orders are posted through executor.py and confirmed
through the authenticated Polymarket user websocket, with REST polling as a
fallback. The public market websocket manages model-AWC hourly accumulation
orders; unrelated price-momentum trading remains disabled.
"""

from __future__ import annotations

import argparse
import atexit
import concurrent.futures
import csv
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import queue
import re
import threading
import time
import warnings
from collections import Counter
from dataclasses import MISSING, asdict, dataclass, fields
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import joblib
import requests
import websocket
from featurize_metar_history import (
    MetarRow as FeatureMetarRow,
    STATION_IDS as FEATURE_STATION_IDS,
    add_daily_temperature_context_features,
    add_observation_context_features,
    daily_temperature_context_series,
    decode_metar,
    nearest_lag_value,
)

BASE_POLY = "https://polymarket.com"
DEFAULT_CONFIG_PATH = "polymarket_weather_config.json"
LOGGER = logging.getLogger("weatherbot")
IO_LOCK = threading.RLock()

US_WEATHER_CITIES = {
    "Atlanta", "Austin", "Chicago", "Dallas", "Denver", "Houston",
    "Los Angeles", "Miami", "NYC", "San Francisco", "Seattle",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
TEMP_TITLE_RE = re.compile(r"^(Highest|Lowest)\s+temperature\s+in\s+(.+?)\s+on\s+([A-Za-z]+\s+\d{1,2})(?:\?)?$", re.I)
TEMP_NUMBER_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:deg(?:rees?)?|°)?\s*([FC])?", re.I)
WU_URL_RE = re.compile(r"https?://(?:www\.)?wunderground\.com/[^\s\"'<)]+", re.I)
EXTREMES_BY_EVENT: dict[tuple[str, str, str], dict[str, Any]] = {}
WEBSOCKET_AVIATION_OBS_BY_EVENT: dict[tuple[str, str, str], dict[str, Any]] = {}
LIVE_TRADER: Optional[Any] = None
TELEGRAM_NOTIFIER: Optional[Any] = None
STATION_REPORT_TIMING: dict[tuple[str, str], dict[str, Any]] = {}
TGFTP_VALIDATION_THREADS: set[str] = set()
TGFTP_OBS_BY_STATION: dict[str, dict[str, Any]] = {}
PRICE_MOMENTUM_WINDOWS: dict[str, dict[str, Any]] = {}
PRICE_RECORDING_WINDOWS: dict[str, dict[str, Any]] = {}
STATION_BY_EVENT_URL: dict[str, str] = {}
SOURCE_STATION_DISABLED_CITIES_BY_DATE: dict[str, set[str]] = {}
SOURCE_STATION_GUARD_LOCK = threading.RLock()
WEBSOCKET_ASSET_REFRESH_REQUESTS: "queue.Queue[dict[str, Any]]" = queue.Queue()
WEBSOCKET_ASSET_UPDATES: "queue.Queue[dict[str, Any]]" = queue.Queue()
WEBSOCKET_ASSET_REFRESH_DEDUP: set[tuple[str, str, str]] = set()
WEBSOCKET_ASSET_REFRESH_LOCK = threading.RLock()
WEBSOCKET_ASSET_REFRESH_THREADS: list[threading.Thread] = []
WEATHER_RECORD_POINTS_BY_STATION_EVENT: dict[tuple[str, str], dict[str, tuple[datetime, float]]] = {}
WEATHER_RECORD_UPDATES: "queue.Queue[list[dict[str, Any]]]" = queue.Queue(maxsize=1000)
WEATHER_RECORD_ACTIVE_WINDOWS: dict[tuple[str, str], dict[str, Any]] = {}
TWC_VERIFY_NEXT_AT: dict[str, float] = {}
MODEL_AWC_MODEL: Any = None
MODEL_AWC_MODEL_PATH: str = ""
HTTP_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="weather-http")
TGFTP_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="tgftp-http")
CLOB_POLL_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="clob-poll")
TWC_VERIFY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="twc-verify")


@dataclass
class TemperatureMarket:
    """Normalized representation of one Polymarket weather temperature market.
    
    Args:
        event_id (str): Polymarket event id.
        market_id (str): Polymarket market id.
        condition_id (str): Polymarket condition id for websocket/user subscriptions.
        city (str): Event city.
        kind (str): Temperature type, usually Highest or Lowest.
        event_date (str): Event date in ISO format.
        event_title (str): Event title from Polymarket.
        market_question (str): Market question text.
        polymarket_url (str): Public Polymarket event URL.
        yes_price (Optional[float]): Current YES price from Gamma, if available.
        rule_min (Optional[float]): Parsed lower inclusive/exclusive temperature bound, if present.
        rule_max (Optional[float]): Parsed upper inclusive/exclusive temperature bound, if present.
        unit (str): Temperature unit used by the market.
        closed (bool): Whether the market is closed.
        raw_market_json (str): Serialized raw Gamma market payload.
    """
    event_id: str
    market_id: str
    condition_id: str
    city: str
    kind: str
    event_date: str
    event_title: str
    market_question: str
    polymarket_url: str
    yes_price: Optional[float]
    rule_min: Optional[float]
    rule_max: Optional[float]
    unit: str
    closed: bool = False
    raw_market_json: str = ""


@dataclass
class PaperTrade:
    """Persisted trade row used for both paper trades and live-order lifecycle tracking.
    
    Args:
        trade_id (str): Unique local trade identifier.
        created_at (str): Local timestamp when the trade row was created.
        cycle_id (str): Strategy cycle id that generated the trade.
        strategy (str): Strategy name.
        event_id (str): Polymarket event id.
        market_id (str): Polymarket market id.
        condition_id (str): Polymarket condition id.
        city (str): Event city.
        kind (str): Temperature type, usually Highest or Lowest.
        event_date (str): Event date in ISO format.
        event_title (str): Event title from Polymarket.
        market_question (str): Market question text.
        polymarket_url (str): Public Polymarket event URL.
        wunderground_source_url (str): Weather Underground station source URL.
        forecast_source (str): Strategy signal source.
        forecast_observed_at (str): Timestamp when the weather signal was observed.
        forecast_station (str): Aviation or weather station id.
        forecast_temp (Optional[float]): Signal temperature used by the market kind.
        forecast_high (Optional[float]): Observed high temperature.
        forecast_low (Optional[float]): Observed low temperature.
        forecast_first_valid_time_local (str): First local forecast validity timestamp.
        forecast_last_valid_time_local (str): Last local forecast validity timestamp.
        forecast_unit (str): Forecast temperature unit.
        rule_min (Optional[float]): Raw market lower bound.
        rule_max (Optional[float]): Raw market upper bound.
        market_unit (str): Market temperature unit.
        comparable_rule_min (Optional[float]): Rule lower bound converted to comparable unit.
        comparable_rule_max (Optional[float]): Rule upper bound converted to comparable unit.
        comparable_unit (str): Unit used for comparable bounds.
        yes_price (Optional[float]): Entry price for the bought side, including NO as NO token price.
        mispricing_price_threshold (float): Strategy price threshold retained for report compatibility.
        pricing_edge (float): Strategy edge retained for report compatibility.
        notional_usdc (float): Filled or simulated entry notional.
        shares (float): Filled or simulated share count.
        taker_fee_rate (float): Fee rate used by the simulator.
        buy_fee_usdc (float): Entry fee estimate.
        total_cost_usdc (float): Entry notional plus entry fee.
    """
    trade_id: str
    created_at: str
    cycle_id: str
    strategy: str
    event_id: str
    market_id: str
    condition_id: str
    city: str
    kind: str
    event_date: str
    event_title: str
    market_question: str
    polymarket_url: str
    wunderground_source_url: str
    forecast_source: str
    forecast_observed_at: str
    forecast_station: str
    forecast_temp: Optional[float]
    forecast_high: Optional[float]
    forecast_low: Optional[float]
    forecast_first_valid_time_local: str
    forecast_last_valid_time_local: str
    forecast_unit: str
    rule_min: Optional[float]
    rule_max: Optional[float]
    market_unit: str
    comparable_rule_min: Optional[float]
    comparable_rule_max: Optional[float]
    comparable_unit: str
    yes_price: Optional[float]
    mispricing_price_threshold: float
    pricing_edge: float
    notional_usdc: float
    shares: float
    taker_fee_rate: float
    buy_fee_usdc: float
    total_cost_usdc: float
    position_side: str = "YES"
    exit_at: str = ""
    exit_reason: str = ""
    exit_action: str = ""
    exit_yes_price: Optional[float] = None
    exit_no_price: Optional[float] = None
    exit_fee_usdc: float = 0.0
    exit_hedge_cost_usdc: float = 0.0
    exit_proceeds_usdc: float = 0.0
    monitor_last_yes_price: Optional[float] = None
    monitor_last_checked_at: str = ""
    monitor_price_trigger: float = 0.0
    monitor_peak_pnl_usdc: float = 0.0
    status: str = "OPEN"
    settlement_source: str = ""
    winning_outcome: str = ""
    payout_usdc: float = 0.0
    pnl_usdc: float = 0.0
    error: str = ""
    asset_id: str = ""
    execution_mode: str = "PAPER"
    live_buy_order_id: str = ""
    live_sell_order_id: str = ""
    live_order_status: str = ""
    live_order_error: str = ""


@dataclass
class LivePendingOrder:
    """Track a submitted live order while waiting for websocket or polling confirmation.
    
    Args:
        kind (str): Order kind, either BUY or SELL.
        trade_id (str): Local trade row identifier associated with the order.
        order_id (str): Polymarket CLOB order id returned by executor.py.
        token_id (str): YES or NO outcome token id submitted to the CLOB.
        condition_id (str): Market condition id used for user websocket subscription.
        price (float): Limit price used when submitting the order.
        shares (float): Intended number of shares to match.
        created_ts (float): Local Unix timestamp when the order was posted.
        balance_before (float): USDC balance before order submission, when available.
        token_balance_before (Optional[float]): Token balance before order submission, when available.
    """
    kind: str
    trade_id: str
    order_id: str
    token_id: str
    condition_id: str
    price: float
    shares: float
    created_ts: float
    balance_before: float = 0.0
    token_balance_before: Optional[float] = None


@dataclass
class ModelAwcHourlyBatch:
    """One city/hour live accumulation target managed from CLOB websocket books."""

    batch_id: str
    city: str
    station: str
    event_date: str
    local_hour: int
    mode: str
    markets: tuple[TemperatureMarket, ...]
    sides: tuple[str, ...]
    token_ids: tuple[str, ...]
    target_shares: float
    target_notional_usd: float
    predicted_high_f: float
    cycle_id: str
    reason: str
    baseline_balances: dict[str, float]
    acquired_shares: dict[str, float]
    acquired_cost_usd: dict[str, float]
    average_prices: dict[str, float]
    open_order_ids: dict[str, str]
    expires_ts: float
    repair_token_id: str = ""
    next_action_ts: float = 0.0
    closed: bool = False


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge an override dictionary into a base dictionary without mutating the original base object.
    
    Args:
        base (dict[str, Any]): Base dictionary that provides default values.
        override (dict[str, Any]): Dictionary whose values take precedence over base values.
    
    Returns:
        dict[str, Any]: Merged dictionary with nested overrides applied.
    """
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def default_config() -> dict[str, Any]:
    """Build the default runtime configuration used when no config file value overrides it.
    
    Args:
        None.
    
    Returns:
        dict[str, Any]: Default bot configuration dictionary.
    """
    allowed = sorted(US_WEATHER_CITIES)
    city_timezones = {
        "Atlanta": "America/New_York", "Austin": "America/Chicago", "Chicago": "America/Chicago",
        "Dallas": "America/Chicago", "Denver": "America/Denver", "Houston": "America/Chicago",
        "Los Angeles": "America/Los_Angeles", "Miami": "America/New_York", "NYC": "America/New_York",
        "San Francisco": "America/Los_Angeles", "Seattle": "America/Los_Angeles",
    }
    city_stations = {
        "Atlanta": "KATL", "Austin": "KAUS", "Chicago": "KMDW", "Dallas": "KDAL",
        "Denver": "KBKF", "Houston": "KHOU", "Los Angeles": "KLAX", "Miami": "KMIA",
        "NYC": "KLGA", "San Francisco": "KSFO", "Seattle": "KSEA",
    }
    trading = {
        "strategy_name": "deterministic_harvest",
        "strategy_mode": "deterministic_harvest",
        "buy_notional_usdc": 10.0,
        "fee_rate": 0.05,
        "fee_enabled": True,
        "deterministic_min_yes_price": 0.01,
        "deterministic_no_max_price": 0.98,
        "deterministic_yes_max_price": 0.98,
        "monitor_price_change_pct": 0.03,
        "twc_post_entry_verify_seconds": 7200,
        "aviation_poll_after_observation_minutes": 15,
        "aviation_poll_interval_seconds": 60,
        "aviation_refresh_probe_seconds": 180,
        "weather_record_websocket_reconnect_seconds": 5,
        "weather_record_websocket_heartbeat_seconds": 20,
        "weather_record_websocket_read_timeout_seconds": 5,
        "weather_record_pre_window_seconds": 120,
        "weather_record_receive_window_seconds": 300,
        "weather_record_timing_refresh_seconds": 60,
        "weather_record_timing_stagger_seconds": 60,
        "tgftp_window_start_delay_seconds": 120,
        "tgftp_window_poll_min_interval_seconds": 0.05,
        "tgftp_window_max_inflight_per_station": 10,
        "allowed_cities": allowed,
        "websocket_enabled": False,
        "live_trading_enabled": False,
        "live_trading_dry_run": False,
        "live_order_timeout_seconds": 20,
        "live_order_check_seconds": 5,
        "max_buy_price": 0.85,
        "source_station_check_enabled": True,
        "source_station_check_hour_ct": 3,
        "source_station_check_timezone": "America/Chicago",
        "depth_price_notional_multiplier": 1.0,
        "depth_price_extra_levels": 0,
        "model_awc_enabled": True,
        "model_awc_model_path": "models/lightgbm_rolling_6y_holdout_24h_lag10_speci_context_regular_prevday_20260630/lightgbm_metar_high_rolling_6y_best.pkl",
        "model_awc_live_stations": [
            "KATL", "KAUS", "KDAL", "KBKF", "KHOU", "KLAX",
            "KLGA", "KMIA", "KORD", "KSEA", "KSFO",
        ],
        "model_awc_buy_start_hour": 12,
        "model_awc_buy_end_hour": 17,
        "model_awc_station_buy_hours": {
            "KATL": [14, 17],
            "KAUS": [14, 17],
            "KHOU": [14, 17],
            "KORD": [14, 17],
            "KDAL": [15, 17],
            "KBKF": [15, 17],
            "KLGA": [14, 17],
            "KLAX": [12, 16],
            "KSEA": [14, 17],
            "KSFO": [13, 16],
            "KMIA": [12, 16],
        },
        "model_awc_metar_lookback_hours": 48,
        "model_awc_observation_minute": 53,
        "model_awc_station_observation_minutes": {
            "KORD": 51,
            "KATL": 52,
            "KLGA": 51,
            "KSFO": 56,
            "KBKF": 58,
            "KAUS": 53,
            "KDAL": 53,
            "KHOU": 53,
            "KLAX": 53,
            "KSEA": 53,
            "KMIA": 53,
        },
        "model_awc_station_poll_stagger_seconds": 5,
        "model_awc_lag_tolerance_minutes": 30,
        "model_awc_poll_delay_seconds": 180,
        "model_awc_poll_interval_seconds": 60,
        "model_awc_poll_attempts": 5,
        "model_awc_tgftp_enabled": True,
        "model_awc_tgftp_start_delay_seconds": 60,
        "model_awc_tgftp_poll_interval_seconds": 2,
        "model_awc_tgftp_request_timeout_seconds": 2,
        "model_awc_tgftp_poll_timeout_seconds": 300,
        "model_awc_tgftp_awc_history_hours": 48,
        "model_awc_awc_max_attempts": 3,
        "model_awc_awc_retry_interval_seconds": 3,
        "model_awc_adjacent_yes_max_total_price": 0.9,
        "model_awc_adjacent_yes_shares": 10,
        "model_awc_order_management_window_minutes": 40,
        "model_awc_interval_snap_tolerance_f": 0.15,
    }
    return {
    "api": {
        "polymarket_gamma_base": "https://gamma-api.polymarket.com",
        "polymarket_data_base": "https://data-api.polymarket.com",
        "polymarket_clob_base": "https://clob.polymarket.com",
        "weather_record_websocket_url": "wss://jackaisolutions.us/ws/weatherrecord",
        "weather_company_base": "https://api.weather.com",
            "twc_api_key_env": "TWC_API_KEY",
            "twc_api_key": "",
            "twc_units": "e",
            "request_timeout_seconds": 30,
            "per_request_delay_seconds": 0.25,
        },
        "events": {"target_dates": ["today"], "city_filter": "", "allowed_cities": allowed, "include_closed": False, "max_offsets": 1200, "city_timezones": city_timezones, "city_stations": city_stations},
        "trading": trading,
        "scheduler": {"poll_interval_minutes": 15, "run_once": False, "max_cycles": 0, "settle_after_each_cycle": True},
        "outputs": {
            "trades_csv": "polymarket_weather_trades.csv",
            "positions_json": "polymarket_weather_positions.json",
            "settled_trades_csv": "polymarket_weather_trades_settled.csv",
            "performance_by_cycle_csv": "polymarket_weather_performance_by_cycle.csv",
            "performance_by_event_csv": "polymarket_weather_performance_by_event.csv",
            "state_json": "polymarket_weather_state.json",
            "aviation_metar_history_jsonl": "aviation_metar_history.jsonl",
            "price_window_ticks_jsonl": "price_window_ticks.jsonl",
            "price_window_raw_jsonl": "price_window_raw.jsonl",
            "log_file": "bot.log",
            "log_level": "INFO",
            "console_log_enabled": False,
            "log_max_mb": 150,
            "log_file_count": 3,
        },
        "account": {
            "polymarket_user_address": "",
            "polymarket_user_address_env": "POLYMARKET_USER_ADDRESS",
            "private_key_env": "PRIVATE_KEY",
            "safe_address_env": "SAFE_ADDRESS",
            "funder_address_env": "FUNDER_ADDRESS",
            "signature_type_env": "SIGNATURE_TYPE",
            "signature_type": 3,
            "sync_positions_on_start": True,
            "sync_positions_on_stop": True,
        },
        "notifications": {
            "telegram_enabled": True,
            "telegram_notify_order_submitted": True,
            "telegram_notify_order_filled": True,
        },
        "price_momentum": {
            "enabled": True,
            "awc_extreme_harvest_enabled": False,
            "move_to_one_fraction": 0.30,
            "no_change_pct": 0.30,
            "yes_change_pct": 0.30,
            "report_window_seconds": 210,
            "price_window_record_seconds": 300,
            "no_max_price": 0.99,
            "yes_max_price": 0.99,
            "clob_poll_enabled": True,
            "clob_poll_interval_seconds": 0.025,
            "clob_no_change_pct": 0.10,
            "clob_poll_max_inflight_per_market": 32,
            "tgftp_verify_interval_seconds": 10,
            "tgftp_verify_timeout_seconds": 180,
            "twc_verify_interval_seconds": 900,
            "twc_verify_stagger_seconds": 300,
            "twc_verify_scheduler_tick_seconds": 10,
        },
        "strategies": [{"name": "deterministic_harvest", "enabled": True, "events": {"target_dates": ["today"]}, "trading": trading}],
    }


def active_config(config: dict[str, Any]) -> dict[str, Any]:
    """Select the enabled deterministic_harvest strategy and merge its overrides into the top-level config.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        dict[str, Any]: Configuration scoped to the enabled deterministic strategy.
    """
    strategies = [s for s in config.get("strategies", []) if s.get("enabled", True)]
    supported_modes = {"deterministic_harvest", "model_awc_high"}
    deterministic = [s for s in strategies if s.get("trading", {}).get("strategy_mode") in supported_modes]
    if not deterministic:
        if config.get("trading", {}).get("strategy_mode") in supported_modes:
            return config
        raise RuntimeError("No enabled weather strategy found in config.")
    selected = deterministic[0]
    merged = deep_merge(config, selected)
    merged["strategies"] = [selected]
    merged["trading"]["strategy_name"] = selected.get("name") or selected.get("trading", {}).get("strategy_name", "deterministic_harvest")
    return merged


def load_config(path: str) -> dict[str, Any]:
    """Load JSON configuration from disk, merge it with defaults, and return the active strategy config.
    
    Args:
        path (str): Filesystem path to read from or write to.
    
    Returns:
        dict[str, Any]: Loaded, merged, active configuration.
    """
    with open(path, "r", encoding="utf-8") as f:
        user_config = json.load(f)
    config = active_config(deep_merge(default_config(), user_config))
    user_trading = user_config.get("trading", {}) if isinstance(user_config.get("trading", {}), dict) else {}
    if "buy_notional_usdc" in user_trading:
        config.setdefault("trading", {})["buy_notional_usdc"] = user_trading["buy_notional_usdc"]
        for strategy in config.get("strategies", []) or []:
            if isinstance(strategy, dict):
                strategy.setdefault("trading", {})["buy_notional_usdc"] = user_trading["buy_notional_usdc"]
    return config


def redacted_config(config: dict[str, Any]) -> dict[str, Any]:
    """Create a logging-safe copy of the configuration with sensitive API keys redacted.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        dict[str, Any]: Copy of config safe to emit in logs.
    """
    def redact(value: Any, key: str = "") -> Any:
        """Recursively redact sensitive values from nested config structures.
        
        Args:
            value (Any): Raw value to parse or normalize.
            key (str): Current dictionary key used to identify secrets, tokens, and API keys.
        
        Returns:
            Any: Redacted value with the same container shape as the input.
        """
        if isinstance(value, dict):
            return {k: redact(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v, key) for v in value]
        if "key" in key.lower() or "secret" in key.lower() or "token" in key.lower():
            text = str(value or "")
            return "" if not text else text[:4] + "..." + text[-4:]
        return value
    return redact(config)


def setup_logging(config: dict[str, Any]) -> None:
    """Configure file and optional console logging for the bot process.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        None: This function is executed for its side effects.
    """
    outputs = config["outputs"]
    for output_path in outputs.values():
        if isinstance(output_path, str) and output_path:
            parent = os.path.dirname(os.path.abspath(output_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
    level = getattr(logging, str(outputs.get("log_level", "INFO")).upper(), logging.INFO)
    max_bytes = max(1, int(outputs.get("log_max_mb", 150))) * 1024 * 1024
    file_count = max(1, int(outputs.get("log_file_count", 3)))
    handlers: list[logging.Handler] = [
        RotatingFileHandler(
            outputs["log_file"],
            maxBytes=max_bytes,
            backupCount=file_count - 1,
            encoding="utf-8",
        )
    ]
    if outputs.get("console_log_enabled", False):
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers, force=True)


def city_timezone(config: dict[str, Any], city: str) -> ZoneInfo:
    """Resolve the IANA timezone configured for a city.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        city (str): Canonical city name parsed from a Polymarket weather event.
    
    Returns:
        ZoneInfo: Timezone object for the city.
    """
    tz_name = config["events"].get("city_timezones", {}).get(city, "UTC")
    return ZoneInfo(tz_name)


def local_date_for_timezone(tz_name: str, *, offset_days: int = 0) -> date:
    """Return the local date for a timezone, optionally offset by a number of days.
    
    Args:
        tz_name (str): IANA timezone name such as America/Chicago.
        offset_days (int): Number of local days to add before returning a date.
    
    Returns:
        date: Local date in the requested timezone.
    """
    return (datetime.now(ZoneInfo(tz_name)) + timedelta(days=offset_days)).date()


def stable_stagger_seconds(key: str, spread_seconds: int) -> float:
    """Return a stable per-key offset inside a scheduling spread."""
    spread = max(1, int(spread_seconds))
    total = 0
    for idx, char in enumerate(str(key)):
        total += (idx + 1) * ord(char)
    return float(total % spread)


def resolve_date(value: str, *, base_date: Optional[date] = None) -> date:
    """Resolve a date token such as today/tomorrow or an ISO date string to a date object.
    
    Args:
        value (str): Raw value to parse or normalize.
        base_date (Optional[date]): Reference date used when resolving relative date strings.
    
    Returns:
        date: Resolved absolute date.
    """
    today = base_date or datetime.now().date()
    text = value.strip().lower()
    if text == "today":
        return today
    if text == "tomorrow":
        return today + timedelta(days=1)
    return date.fromisoformat(value)


def resolve_event_target_dates(config: dict[str, Any]) -> list[date]:
    """Resolve all configured target date tokens across the configured city timezones.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        list[date]: Sorted list of distinct target dates.
    """
    cities = set(config["events"].get("allowed_cities") or config["trading"].get("allowed_cities") or [])
    tz_by_city = config["events"].get("city_timezones", {})
    timezone_names = {tz_by_city.get(city, "UTC") for city in cities} or {"UTC"}
    targets: set[date] = set()
    for value in config["events"]["target_dates"]:
        text = str(value).strip().lower()
        if text in {"today", "tomorrow"}:
            offset = 1 if text == "tomorrow" else 0
            for tz_name in timezone_names:
                targets.add(local_date_for_timezone(tz_name, offset_days=offset))
        else:
            targets.add(date.fromisoformat(str(value)))
    return sorted(targets)


def resolve_event_target_dates_for_city(config: dict[str, Any], city: str) -> set[date]:
    """Resolve configured target date tokens for one city using that city timezone.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        city (str): Canonical city name parsed from a Polymarket weather event.
    
    Returns:
        set[date]: Target dates for the city.
    """
    tz_name = config["events"].get("city_timezones", {}).get(city, "UTC")
    targets: set[date] = set()
    for value in config["events"]["target_dates"]:
        text = str(value).strip().lower()
        if text in {"today", "tomorrow"}:
            offset = 1 if text == "tomorrow" else 0
            targets.add(local_date_for_timezone(tz_name, offset_days=offset))
        else:
            targets.add(date.fromisoformat(str(value)))
    return targets


def infer_year(month_day_text: str, today: Optional[date] = None) -> date:
    """Infer the event year for a month/day title fragment relative to a reference date.
    
    Args:
        month_day_text (str): Month/day text parsed from an event title, for example June 6.
        today (Optional[date]): Reference date used for year inference.
    
    Returns:
        date: Month/day converted to a full date.
    """
    base = today or datetime.now().date()
    parsed = datetime.strptime(f"{month_day_text} {base.year}", "%B %d %Y").date()
    if parsed < base - timedelta(days=180):
        parsed = datetime.strptime(f"{month_day_text} {base.year + 1}", "%B %d %Y").date()
    return parsed


def http_get_json(url: str, params: Optional[dict[str, Any]], timeout: int) -> Any:
    """Perform an HTTP GET request and parse the response body as JSON.
    
    Args:
        url (str): Absolute URL to request or parse.
        params (Optional[dict[str, Any]]): Query-string parameters sent with the HTTP request.
        timeout (int): Request timeout in seconds.
    
    Returns:
        Any: Decoded JSON response body.
    
    Raises:
        requests.HTTPError: If the HTTP response status is not successful.
    """
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_post_json(url: str, payload: Any, timeout: int) -> Any:
    """Perform an HTTP POST request with JSON and parse the response body as JSON."""
    r = requests.post(url, headers={**HEADERS, "Content-Type": "application/json"}, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_get_text(url: str, timeout: int) -> str:
    """Perform an HTTP GET request and return the response body as text.
    
    Args:
        url (str): Absolute URL to request or parse.
        timeout (int): Request timeout in seconds.
    
    Returns:
        str: Response text body.
    
    Raises:
        requests.HTTPError: If the HTTP response status is not successful.
    """
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def gamma_get(config: dict[str, Any], path: str, params: Optional[dict[str, Any]] = None) -> Any:
    """Call the Polymarket Gamma API with configured base URL, timeout, and rate delay.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        path (str): Filesystem path to read from or write to.
        params (Optional[dict[str, Any]]): Query-string parameters sent with the HTTP request.
    
    Returns:
        Any: Polymarket Gamma API response payload.
    """
    return http_get_json(f"{config['api']['polymarket_gamma_base']}{path}", params, int(config["api"]["request_timeout_seconds"]))


def twc_get(config: dict[str, Any], path: str, params: dict[str, Any]) -> Any:
    """Call the Weather Company API with configured base URL, API key, timeout, and rate delay.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        path (str): Filesystem path to read from or write to.
        params (dict[str, Any]): Query-string parameters sent with the HTTP request.
    
    Returns:
        Any: Weather Company API response payload.
    
    Raises:
        RuntimeError: If no Weather Company API key is configured.
        requests.HTTPError: If the API response status is not successful.
    """
    env_name = str(config["api"].get("twc_api_key_env", "TWC_API_KEY")).strip()
    api_key = str(config["api"].get("twc_api_key", "")).strip() or (os.environ.get(env_name, "").strip() if env_name else "")
    if not api_key:
        raise RuntimeError(f"Missing Weather Company API key. Set config api.twc_api_key or {env_name}.")
    return http_get_json(f"{config['api']['weather_company_base']}{path}", {"apiKey": api_key, **params}, int(config["api"]["request_timeout_seconds"]))


def polymarket_data_get(config: dict[str, Any], path: str, params: Optional[dict[str, Any]] = None) -> Any:
    """Call the Polymarket Data API with configured base URL, timeout, and rate delay.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        path (str): Filesystem path to read from or write to.
        params (Optional[dict[str, Any]]): Query-string parameters sent with the HTTP request.
    
    Returns:
        Any: Polymarket Data API response payload.
    """
    return http_get_json(f"{config['api']['polymarket_data_base']}{path}", params, int(config["api"]["request_timeout_seconds"]))


def clob_get(config: dict[str, Any], path: str, params: Optional[dict[str, Any]] = None) -> Any:
    """Call the Polymarket CLOB API with configured base URL and timeout.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        path (str): API path appended to the configured CLOB base URL.
        params (Optional[dict[str, Any]]): Query-string parameters sent with the HTTP request.
    
    Returns:
        Any: Polymarket CLOB API response payload.
    """
    base = str(config["api"].get("polymarket_clob_base") or "https://clob.polymarket.com").rstrip("/")
    return http_get_json(f"{base}{path}", params, int(config["api"]["request_timeout_seconds"]))


def clob_post(config: dict[str, Any], path: str, payload: Any) -> Any:
    """Call the Polymarket CLOB API with a JSON POST body."""
    base = str(config["api"].get("polymarket_clob_base") or "https://clob.polymarket.com").rstrip("/")
    return http_post_json(f"{base}{path}", payload, int(config["api"]["request_timeout_seconds"]))


def configured_polymarket_user(config: dict[str, Any]) -> str:
    """Resolve the Polymarket account address from config, user env, funder env, or safe env.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        str: Configured account address, or an empty string.
    """
    account = config.get("account", {})
    explicit = str(account.get("polymarket_user_address") or "").strip()
    if explicit:
        return explicit
    env_name = str(account.get("polymarket_user_address_env") or "POLYMARKET_USER_ADDRESS").strip()
    user = os.environ.get(env_name, "").strip() if env_name else ""
    if user:
        return user
    for fallback_key in ("funder_address_env", "safe_address_env"):
        fallback_env = str(account.get(fallback_key) or "").strip()
        fallback_value = os.environ.get(fallback_env, "").strip() if fallback_env else ""
        if fallback_value:
            return fallback_value
    return ""


def parse_jsonish(value: Any, default: Any) -> Any:
    """Parse a value that may already be structured JSON or may be a JSON-encoded string.
    
    Args:
        value (Any): Raw value to parse or normalize.
        default (Any): Fallback value returned when parsing fails.
    
    Returns:
        Any: Parsed structured value or the default fallback.
    """
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def parse_bool(value: Any) -> bool:
    """Normalize common boolean-like values into a Python bool.
    
    Args:
        value (Any): Raw value to parse or normalize.
    
    Returns:
        bool: Normalized boolean value.
    """
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def parse_event_title(title: str) -> Optional[tuple[str, str, str]]:
    """Parse a Polymarket weather event title into kind, city, and month/day components.
    
    Args:
        title (str): Polymarket event title text.
    
    Returns:
        Optional[tuple[str, str, str]]: Tuple of kind, city, and month/day, or None if the title does not match.
    """
    m = TEMP_TITLE_RE.match(title.strip())
    if not m:
        return None
    return m.group(1).title(), m.group(2).strip(), m.group(3).strip()


def poly_url_from_event(event: dict[str, Any]) -> str:
    """Build a public Polymarket event URL from an event payload.
    
    Args:
        event (dict[str, Any]): Polymarket event payload.
    
    Returns:
        str: Public event URL.
    """
    slug = event.get("slug") or event.get("ticker") or event.get("id", "")
    return urljoin(BASE_POLY, f"/event/{slug}") if slug else BASE_POLY


def discover_temperature_events(config: dict[str, Any], target: date) -> list[dict[str, Any]]:
    """Discover active Polymarket temperature events for a target date and configured city filters.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        target (date): Target local event date to match against parsed Polymarket event titles.
    
    Returns:
        list[dict[str, Any]]: Temperature event payloads matching filters.
    
    Side effects:
        Calls the Polymarket Gamma API and logs the number of matching events.
    """
    found: dict[str, dict[str, Any]] = {}
    city_filter = str(config["events"].get("city_filter") or "").lower()
    allowed = set(config["events"].get("allowed_cities") or config["trading"].get("allowed_cities") or [])
    for base_params in ({"tag_slug": "weather"}, {"q": "Highest temperature"}, {"q": "Lowest temperature"}):
        for offset in range(0, int(config["events"].get("max_offsets", 1200)), 100):
            params = {"limit": 100, "offset": offset, **base_params}
            if not config["events"].get("include_closed", False):
                params.update({"closed": "false", "archived": "false"})
            batch = gamma_get(config, "/events", params)
            if not isinstance(batch, list) or not batch:
                break
            for event in batch:
                title = event.get("title") or event.get("question") or ""
                parsed = parse_event_title(title)
                if not parsed:
                    continue
                kind, city, md = parsed
                event_date = infer_year(md, today=target)
                if event_date != target or event_date not in resolve_event_target_dates_for_city(config, city) or (city_filter and city_filter not in city.lower()) or (allowed and city not in allowed):
                    continue
                event["_parsed_kind"] = kind
                event["_parsed_city"] = city
                event["_parsed_event_date"] = event_date.isoformat()
                found[str(event.get("id") or event.get("slug"))] = event
            if len(batch) < 100:
                break
            time.sleep(float(config["api"].get("per_request_delay_seconds", 0.25)))
    return list(found.values())


def fetch_event_by_url(config: dict[str, Any], event_url: str, city: str, kind: str, event_date: str) -> Optional[dict[str, Any]]:
    """Fetch one Polymarket event by slug URL and validate it matches the expected city, kind, and date.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        event_url (str): Public Polymarket event URL.
        city (str): Canonical city name parsed from a Polymarket weather event.
        kind (str): Weather event kind, expected to be Highest or Lowest.
        event_date (str): Event date in ISO yyyy-mm-dd format.
    
    Returns:
        Optional[dict[str, Any]]: Matching event payload, or None when not found.
    
    Side effects:
        Calls the Polymarket Gamma API when a slug is available.
    """
    slug = event_url.rstrip("/").split("/")[-1]
    if slug:
        try:
            detail = gamma_get(config, f"/events/slug/{slug}")
            candidates = [detail] if isinstance(detail, dict) else []
        except Exception:
            LOGGER.exception("event fetch by slug failed url=%s", event_url)
            candidates = []
    else:
        candidates = []
    if not candidates:
        candidates = discover_temperature_events(config, date.fromisoformat(event_date))
    for event in candidates:
        parsed = parse_event_title(event.get("title") or event.get("question") or "")
        if parsed:
            event["_parsed_kind"], event["_parsed_city"], md = parsed
            event["_parsed_event_date"] = infer_year(md, today=date.fromisoformat(event_date)).isoformat()
        if event.get("_parsed_city") == city and event.get("_parsed_kind") == kind and event.get("_parsed_event_date") == event_date:
            return event
    return None


def extract_wunderground_source(config: dict[str, Any], event_url: str) -> str:
    """Fetch an event page and extract the Weather Underground source URL referenced in the page.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        event_url (str): Public Polymarket event URL.
    
    Returns:
        str: Weather Underground URL, or an empty string if unavailable.
    
    Side effects:
        Downloads the Polymarket event page HTML.
    """
    html = http_get_text(event_url, int(config["api"]["request_timeout_seconds"]))
    urls = [u.rstrip(".,") for u in WU_URL_RE.findall(html)]
    return urls[0] if urls else ""


def station_from_wu_url(url: str) -> str:
    """Extract a four-character station code from a Weather Underground URL.
    
    Args:
        url (str): Absolute URL to request or parse.
    
    Returns:
        str: Station code, or an empty string if parsing fails.
    """
    parts = [p for p in url.split("?")[0].rstrip("/").split("/") if p]
    if "date" in parts:
        parts = parts[: parts.index("date")]
    station = parts[-1].upper() if parts else ""
    return station if re.fullmatch(r"[A-Z0-9]{4}", station) else ""


def configured_station_for_city(config: dict[str, Any], city: str) -> str:
    """Return a configured station code for a city, if one is available."""
    station = str(config.get("events", {}).get("city_stations", {}).get(city) or "").strip().upper()
    return station if re.fullmatch(r"[A-Z0-9]{4}", station) else ""


def station_for_event(config: dict[str, Any], city: str, event_url: str, allow_network: bool = True) -> str:
    """Resolve an event station from config first, then cached/page Weather Underground source."""
    configured = configured_station_for_city(config, city)
    if configured:
        return configured
    if event_url in STATION_BY_EVENT_URL:
        return STATION_BY_EVENT_URL[event_url]
    if not allow_network or not event_url:
        return ""
    try:
        station = station_from_wu_url(extract_wunderground_source(config, event_url))
    except requests.RequestException as exc:
        LOGGER.warning("station discovery page request failed city=%s url=%s error=%s", city, event_url, exc)
        return ""
    except Exception:
        LOGGER.exception("station discovery failed city=%s url=%s", city, event_url)
        return ""
    if station:
        STATION_BY_EVENT_URL[event_url] = station
    return station


def source_station_for_event(config: dict[str, Any], city: str, event_url: str) -> str:
    """Resolve the Polymarket settlement source station from the event page."""
    if not event_url:
        return ""
    try:
        return station_from_wu_url(extract_wunderground_source(config, event_url))
    except requests.RequestException as exc:
        LOGGER.warning("source station check request failed city=%s url=%s error=%s", city, event_url, exc)
        return ""
    except Exception:
        LOGGER.exception("source station check failed city=%s url=%s", city, event_url)
        return ""


def source_station_block_city(event_date: str, city: str) -> None:
    """Block live trading for one city/date after source-station mismatch."""
    if not event_date or not city:
        return
    with SOURCE_STATION_GUARD_LOCK:
        SOURCE_STATION_DISABLED_CITIES_BY_DATE.setdefault(event_date, set()).add(city)


def source_station_city_blocked(event_date: str, city: str) -> bool:
    """Return whether source-station guard has blocked live trading for a city/date."""
    if not event_date or not city:
        return False
    with SOURCE_STATION_GUARD_LOCK:
        return city in SOURCE_STATION_DISABLED_CITIES_BY_DATE.get(event_date, set())


def prune_source_station_blocks(today: date) -> None:
    """Drop stale source-station guard blocks from previous event dates."""
    with SOURCE_STATION_GUARD_LOCK:
        for event_date in list(SOURCE_STATION_DISABLED_CITIES_BY_DATE):
            try:
                parsed = date.fromisoformat(event_date)
            except ValueError:
                SOURCE_STATION_DISABLED_CITIES_BY_DATE.pop(event_date, None)
                continue
            if parsed < today:
                SOURCE_STATION_DISABLED_CITIES_BY_DATE.pop(event_date, None)


def check_polymarket_source_stations(config: dict[str, Any], target: date) -> dict[str, Any]:
    """Compare Polymarket weather source stations with configured city stations."""
    mismatches: list[dict[str, str]] = []
    checked: dict[str, dict[str, str]] = {}
    events = discover_temperature_events(config, target)
    for event in events:
        city = str(event.get("_parsed_city") or "")
        event_date = str(event.get("_parsed_event_date") or target.isoformat())
        event_url = poly_url_from_event(event)
        configured = configured_station_for_city(config, city)
        if not city or not configured:
            continue
        key = f"{event_date}:{city}"
        if key in checked:
            continue
        source_station = source_station_for_event(config, city, event_url)
        if not source_station:
            LOGGER.warning(
                "source station check unavailable city=%s event_date=%s configured_station=%s url=%s",
                city,
                event_date,
                configured,
                event_url,
            )
            continue
        checked[key] = {
            "city": city,
            "event_date": event_date,
            "configured_station": configured,
            "source_station": source_station,
            "polymarket_url": event_url,
        }
        if source_station != configured:
            source_station_block_city(event_date, city)
            mismatch = checked[key]
            mismatches.append(mismatch)
            LOGGER.error(
                "source station mismatch blocking live trades city=%s event_date=%s configured_station=%s source_station=%s url=%s",
                city,
                event_date,
                configured,
                source_station,
                event_url,
            )
        else:
            LOGGER.info(
                "source station check ok city=%s event_date=%s station=%s",
                city,
                event_date,
                configured,
            )
    LOGGER.info(
        "source station check complete target=%s events=%s checked_cities=%s mismatches=%s",
        target.isoformat(),
        len(events),
        len(checked),
        len(mismatches),
    )
    return {
        "target": target.isoformat(),
        "events": len(events),
        "checked": list(checked.values()),
        "mismatches": mismatches,
    }


def next_source_station_check_time(config: dict[str, Any], now: Optional[datetime] = None) -> datetime:
    """Return the next configured source-station check time."""
    trading = config.get("trading", {})
    tz_name = str(trading.get("source_station_check_timezone", "America/Chicago"))
    tz = ZoneInfo(tz_name)
    now_local = (now or datetime.now(timezone.utc)).astimezone(tz)
    check_hour = int(trading.get("source_station_check_hour_ct", 3))
    candidate = now_local.replace(hour=check_hour, minute=0, second=0, microsecond=0)
    if now_local >= candidate:
        candidate += timedelta(days=1)
    return candidate


def source_station_guard_supervisor(config: dict[str, Any]) -> None:
    """Run the Polymarket source-station guard once daily at the configured CT hour."""
    if not bool(config.get("trading", {}).get("source_station_check_enabled", True)):
        LOGGER.info("source station guard disabled")
        return
    tz_name = str(config.get("trading", {}).get("source_station_check_timezone", "America/Chicago"))
    tz = ZoneInfo(tz_name)
    check_hour = int(config.get("trading", {}).get("source_station_check_hour_ct", 3))
    last_checked_date = ""
    LOGGER.info("source station guard started timezone=%s check_hour=%s", tz_name, check_hour)
    while True:
        try:
            now_local = datetime.now(tz)
            prune_source_station_blocks(now_local.date())
            if now_local.hour >= check_hour and last_checked_date != now_local.date().isoformat():
                check_polymarket_source_stations(config, now_local.date())
                last_checked_date = now_local.date().isoformat()
            next_check = next_source_station_check_time(config)
            sleep_seconds = max(30.0, min(900.0, (next_check - datetime.now(tz)).total_seconds()))
            time.sleep(sleep_seconds)
        except Exception:
            LOGGER.exception("source station guard loop failed")
            time.sleep(300)


def start_source_station_guard_thread(config: dict[str, Any]) -> threading.Thread:
    """Start the daily source-station guard in a daemon thread."""
    thread = threading.Thread(target=source_station_guard_supervisor, args=(config,), name="source-station-guard", daemon=True)
    thread.start()
    return thread


def infer_temperature_unit(text: str, default_unit: str = "F") -> str:
    """Infer Fahrenheit or Celsius from market text, falling back to a supplied default unit.
    
    Args:
        text (str): Market or source text used for parsing.
        default_unit (str): Fallback temperature unit when the text does not specify one.
    
    Returns:
        str: Temperature unit code F or C.
    """
    normalized = text.lower()
    if "celsius" in normalized or "°c" in normalized:
        return "C"
    if "fahrenheit" in normalized or "°f" in normalized:
        return "F"
    return default_unit.upper()


def parse_temperature_rule(text: str, default_unit: str = "F") -> tuple[Optional[float], Optional[float], str]:
    """Parse a market question into lower/upper temperature rule bounds and unit.
    
    Args:
        text (str): Market or source text used for parsing.
        default_unit (str): Fallback temperature unit when the text does not specify one.
    
    Returns:
        tuple[Optional[float], Optional[float], str]: Tuple of lower bound, upper bound, and unit.
    """
    normalized = text.replace("\u2013", "-").replace("\u2014", "-")
    low_text = normalized.lower()
    inferred_unit = infer_temperature_unit(normalized, default_unit)
    nums = [(float(n), (u or inferred_unit).upper()) for n, u in TEMP_NUMBER_RE.findall(normalized)]
    unit = nums[0][1] if nums else inferred_unit
    values = [n for n, _ in nums]
    if len(values) >= 2 and re.search(r"\d\s*-\s*\d", normalized) and values[0] >= 0 and values[1] < 0:
        values[1] = abs(values[1])
    if not values:
        return None, None, unit
    if len(values) >= 2 and (re.search(r"\bbetween\b|\bfrom\b|\bto\b|\bthrough\b|\brange\b", low_text) or re.search(r"\d\s*-\s*\d", low_text)):
        return min(values[0], values[1]), max(values[0], values[1]), unit
    v = values[0]
    if re.search(r"\b(at\s+or\s+above|or\s+higher|or\s+more|at\s+least|above|over|greater\s+than)\b", low_text):
        return v, None, unit
    if re.search(r"\b(at\s+or\s+below|or\s+lower|or\s+less|at\s+most|below|under|less\s+than)\b", low_text):
        return None, v, unit
    return v, v, unit


def outcome_price(market: dict[str, Any], outcome_name: str) -> Optional[float]:
    """Find the price for a named outcome in a Polymarket market payload.
    
    Args:
        market (dict[str, Any]): Normalized TemperatureMarket object or raw market payload being evaluated.
        outcome_name (str): Outcome label to find, usually Yes or No.
    
    Returns:
        Optional[float]: Outcome price as float, or None if missing.
    """
    outcomes = parse_jsonish(market.get("outcomes"), [])
    prices = parse_jsonish(market.get("outcomePrices"), [])
    for idx, outcome in enumerate(outcomes):
        if str(outcome).strip().lower() == outcome_name.lower() and idx < len(prices):
            try:
                return float(prices[idx])
            except (TypeError, ValueError):
                return None
    return None


def markets_for_event(config: dict[str, Any], event: dict[str, Any]) -> list[TemperatureMarket]:
    """Convert a Polymarket event payload into normalized TemperatureMarket objects.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        event (dict[str, Any]): Polymarket event payload.
    
    Returns:
        list[TemperatureMarket]: Normalized temperature markets for the event.
    """
    markets = event.get("markets") or []
    if not markets and event.get("slug"):
        detail = gamma_get(config, f"/events/slug/{event['slug']}")
        markets = detail.get("markets") or []
    title = event.get("title") or event.get("question") or ""
    parsed: list[TemperatureMarket] = []
    for market in markets:
        question = market.get("question") or market.get("title") or title
        unit_context = " ".join(str(market.get(field) or "") for field in ("question", "title", "description", "resolutionSource", "rules"))
        rule_min, rule_max, unit = parse_temperature_rule(question, infer_temperature_unit(unit_context or question))
        parsed.append(TemperatureMarket(
            event_id=str(event.get("id") or event.get("slug") or ""),
            market_id=str(market.get("id") or market.get("slug") or ""),
            condition_id=str(market.get("conditionId") or ""),
            city=str(event.get("_parsed_city") or ""),
            kind=str(event.get("_parsed_kind") or ""),
            event_date=str(event.get("_parsed_event_date") or ""),
            event_title=title,
            market_question=question,
            polymarket_url=poly_url_from_event(event),
            yes_price=outcome_price(market, "Yes"),
            rule_min=rule_min,
            rule_max=rule_max,
            unit=unit,
            closed=parse_bool(market.get("closed")),
            raw_market_json=json.dumps(market, ensure_ascii=False, sort_keys=True),
        ))
    return parsed


def event_market_unit(markets: list[TemperatureMarket]) -> str:
    """Return the first explicit market unit in an event, defaulting to Fahrenheit.
    
    Args:
        markets (list[TemperatureMarket]): Temperature markets belonging to one Polymarket event.
    
    Returns:
        str: Event temperature unit.
    """
    for market in markets:
        if market.unit:
            return market.unit.upper()
    return "F"


def convert_temperature(value: Optional[float], from_unit: str, to_unit: str) -> Optional[float]:
    """Convert a temperature value between Fahrenheit and Celsius.
    
    Args:
        value (Optional[float]): Raw value to parse or normalize.
        from_unit (str): Source temperature unit.
        to_unit (str): Destination temperature unit.
    
    Returns:
        Optional[float]: Converted value, or None if the input value is None.
    """
    if value is None:
        return None
    src = from_unit.upper()
    dst = to_unit.upper()
    if src == dst:
        return float(value)
    if src == "C" and dst == "F":
        return float(value) * 9.0 / 5.0 + 32.0
    if src == "F" and dst == "C":
        return (float(value) - 32.0) * 5.0 / 9.0
    return float(value)


def comparable_rule_bounds(market: TemperatureMarket, target_unit: str) -> tuple[Optional[float], Optional[float], str]:
    """Convert a market rule bounds into a target unit for comparison.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        target_unit (str): Temperature unit used for comparisons.
    
    Returns:
        tuple[Optional[float], Optional[float], str]: Converted lower bound, upper bound, and target unit.
    """
    return convert_temperature(market.rule_min, market.unit, target_unit), convert_temperature(market.rule_max, market.unit, target_unit), target_unit.upper()


def market_contains_temperature(market: TemperatureMarket, target_temp: float, target_unit: str) -> bool:
    """Check whether a target temperature falls within a market rule range.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        target_temp (float): Observed temperature to compare against a market rule.
        target_unit (str): Temperature unit used for comparisons.
    
    Returns:
        bool: True when the temperature satisfies the market bounds.
    """
    lo, hi, _ = comparable_rule_bounds(market, target_unit)
    return (lo is None or target_temp >= lo) and (hi is None or target_temp <= hi)


def sorted_markets_for_unit(markets: list[TemperatureMarket], target_unit: str) -> list[TemperatureMarket]:
    """Sort open temperature markets by comparable lower and upper rule bounds.
    
    Args:
        markets (list[TemperatureMarket]): Temperature markets belonging to one Polymarket event.
        target_unit (str): Temperature unit used for comparisons.
    
    Returns:
        list[TemperatureMarket]: Open markets sorted by comparable rule bounds.
    """
    def sort_key(market: TemperatureMarket) -> tuple[float, float, str]:
        """Build a stable sort key from comparable temperature bounds and market id.
        
        Args:
            market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        
        Returns:
            tuple[float, float, str]: Lower-bound key, upper-bound key, and market id tie breaker.
        """
        lo, hi, _ = comparable_rule_bounds(market, target_unit)
        return lo if lo is not None else -9999.0, hi if hi is not None else 9999.0, market.market_id
    return sorted([m for m in markets if not m.closed], key=sort_key)


def websocket_relevant_markets_for_observed_extreme(
    markets: list[TemperatureMarket],
    kind: str,
    event_unit: str,
    observed_high: Optional[float],
    observed_low: Optional[float],
) -> list[TemperatureMarket]:
    """Keep only temperature ranges still reachable from today's observed high/low."""
    normalized_kind = kind.strip().lower()
    if normalized_kind == "highest":
        if observed_high is None:
            return markets
        relevant = []
        for market in markets:
            _, hi, _ = comparable_rule_bounds(market, event_unit)
            if hi is None or observed_high <= hi:
                relevant.append(market)
        return relevant
    if normalized_kind == "lowest":
        if observed_low is None:
            return markets
        relevant = []
        for market in markets:
            lo, _, _ = comparable_rule_bounds(market, event_unit)
            if lo is None or observed_low >= lo:
                relevant.append(market)
        return relevant
    return markets


def market_no_price(market: TemperatureMarket) -> Optional[float]:
    """Derive the NO price for a market from explicit outcome prices or the complement of YES.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
    
    Returns:
        Optional[float]: NO price, or None if it cannot be derived.
    """
    raw = parse_jsonish(market.raw_market_json, {})
    no_price = outcome_price(raw, "No") if isinstance(raw, dict) else None
    if no_price is not None:
        return no_price
    if market.yes_price is not None:
        return max(0.0, min(1.0, 1.0 - float(market.yes_price)))
    return None


def asset_id_for_market_side(market: TemperatureMarket, side: str) -> str:
    """Find the CLOB token id for a market side such as YES or NO.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        side (str): Position side, expected to be YES or NO.
    
    Returns:
        str: CLOB token id, or an empty string if unavailable.
    """
    raw = parse_jsonish(market.raw_market_json, {})
    if not isinstance(raw, dict):
        return ""
    wanted = side.strip().lower()
    for asset_id, outcome in market_clob_tokens(raw):
        if outcome.strip().lower() == wanted:
            return asset_id
    return ""


def market_neg_risk(market: TemperatureMarket) -> bool:
    """Return whether a Polymarket market requires neg-risk order signing."""
    raw = parse_jsonish(market.raw_market_json, {})
    if not isinstance(raw, dict):
        return False
    for key in ("negRisk", "neg_risk", "negativeRisk", "negative_risk"):
        if key not in raw:
            continue
        value = raw.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes"}
        return bool(value)
    return False


def parse_clob_levels(levels: Any) -> list[tuple[float, float]]:
    """Parse CLOB book levels into ``(price, size)`` tuples."""
    parsed: list[tuple[float, float]] = []
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price"))
            size = float(level.get("size") or level.get("quantity") or level.get("qty") or 0.0)
        except (TypeError, ValueError):
            continue
        if price > 0 and size > 0:
            parsed.append((price, size))
    return parsed


def clob_book_levels(config: dict[str, Any], asset_id: str) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """Fetch parsed bid and ask levels for one CLOB token id."""
    if not asset_id:
        return [], []
    payload = clob_get(config, "/book", {"token_id": asset_id})
    bids = payload.get("bids") if isinstance(payload, dict) else []
    asks = payload.get("asks") if isinstance(payload, dict) else []
    return parse_clob_levels(bids), parse_clob_levels(asks)


def clob_book_prices(config: dict[str, Any], asset_id: str) -> tuple[Optional[float], Optional[float], int, int]:
    """Fetch best bid and ask prices for one CLOB token id.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        asset_id (str): CLOB token id to query.
    
    Returns:
        tuple[Optional[float], Optional[float], int, int]: Best bid, best ask, bid level count, and ask level count.
    
    Side effects:
        Calls the Polymarket CLOB API.
    """
    bids, asks = clob_book_levels(config, asset_id)
    bid_prices = [price for price, _ in bids]
    ask_prices = [price for price, _ in asks]
    return (max(bid_prices) if bid_prices else None), (min(ask_prices) if ask_prices else None), len(bids), len(asks)


def clamp_price(value: Optional[float]) -> Optional[float]:
    """Clamp a probability price into the inclusive [0, 1] range.
    
    Args:
        value (Optional[float]): Raw price value to clamp.
    
    Returns:
        Optional[float]: Clamped price, or None if input is None.
    """
    if value is None:
        return None
    return max(0.0, min(1.0, float(value)))


def opposite_side(side: str) -> str:
    """Return the opposite binary Polymarket side.
    
    Args:
        side (str): Position side, expected to be YES or NO.
    
    Returns:
        str: NO for YES input, otherwise YES.
    """
    return "YES" if side.strip().upper() == "NO" else "NO"


def depth_target_notional(config: dict[str, Any]) -> float:
    """Return the notional depth to inspect when selecting a live buy price."""
    trading = config.get("trading", {})
    notional = float(trading.get("buy_notional_usdc", 10.0))
    multiplier = max(1.0, float(trading.get("depth_price_notional_multiplier", 1.0)))
    return max(notional, notional * multiplier)


def depth_extra_levels(config: dict[str, Any]) -> int:
    """Return how many levels beyond the target depth to step for buy aggressiveness."""
    try:
        return max(0, int(float(config.get("trading", {}).get("depth_price_extra_levels", 0))))
    except (TypeError, ValueError):
        return 0


def price_for_buy_notional(asks: list[tuple[float, float]], target_notional: float, extra_levels: int = 0) -> Optional[float]:
    """Return the ask price that covers target notional, optionally one or more levels higher."""
    if target_notional <= 0:
        return None
    ordered = [(price, size) for price, size in sorted(asks or [], key=lambda item: item[0]) if price > 0 and size > 0]
    cumulative = 0.0
    for idx, (price, size) in enumerate(ordered):
        cumulative += price * size
        if cumulative >= target_notional:
            return ordered[min(idx + max(0, extra_levels), len(ordered) - 1)][0]
    return None


def price_for_complement_bid_notional(bids: list[tuple[float, float]], target_notional: float, extra_levels: int = 0) -> Optional[float]:
    """Infer a buy price from the opposite token's bid depth."""
    if target_notional <= 0:
        return None
    levels = sorted(
        [(clamp_price(1.0 - bid), size) for bid, size in (bids or []) if bid > 0 and size > 0],
        key=lambda item: float(item[0] or 0.0),
    )
    cumulative = 0.0
    clean: list[tuple[float, float]] = []
    for price, size in levels:
        if price is None or price <= 0:
            continue
        clean.append((price, size))
    for idx, (price, size) in enumerate(clean):
        cumulative += price * size
        if cumulative >= target_notional:
            return clean[min(idx + max(0, extra_levels), len(clean) - 1)][0]
    return None


def price_for_buy_shares(
    asks: list[tuple[float, float]], target_shares: float
) -> Optional[float]:
    """Return the highest ask needed to fill exactly the requested share quantity."""
    if target_shares <= 0:
        return None
    cumulative = 0.0
    for price, size in sorted(asks or [], key=lambda item: item[0]):
        if price <= 0 or size <= 0:
            continue
        cumulative += size
        if cumulative >= target_shares:
            return price
    return None


def price_for_complement_bid_shares(
    bids: list[tuple[float, float]], target_shares: float
) -> Optional[float]:
    """Return the complement price backed by the requested opposite-side shares."""
    if target_shares <= 0:
        return None
    cumulative = 0.0
    levels = sorted(
        [
            (float(clamp_price(1.0 - bid) or 0.0), size)
            for bid, size in (bids or [])
            if bid > 0 and size > 0
        ],
        key=lambda item: item[0],
    )
    for price, size in levels:
        if price <= 0:
            continue
        cumulative += size
        if cumulative >= target_shares:
            return price
    return None


def best_buy_offer(
    config: dict[str, Any], market: TemperatureMarket, side: str
) -> Optional[tuple[float, float]]:
    """Return the cheapest top-of-book buy price and immediately available shares."""
    normalized = side.strip().upper()
    direct_asset_id = asset_id_for_market_side(market, normalized)
    complement_asset_id = asset_id_for_market_side(
        market, opposite_side(normalized)
    )
    if not direct_asset_id:
        return None
    direct_bids, direct_asks = clob_book_levels(config, direct_asset_id)
    complement_bids, _complement_asks = clob_book_levels(
        config, complement_asset_id
    )
    offers: list[tuple[float, float]] = []
    if direct_asks:
        price, size = min(direct_asks, key=lambda item: item[0])
        if price > 0 and size > 0:
            offers.append((float(price), float(size)))
    if complement_bids:
        bid, size = max(complement_bids, key=lambda item: item[0])
        price = clamp_price(1.0 - bid)
        if price is not None and price > 0 and size > 0:
            offers.append((float(price), float(size)))
    if not offers:
        return None
    return min(offers, key=lambda item: item[0])


def partial_buy_fillable_now(
    config: dict[str, Any],
    market: TemperatureMarket,
    side: str,
    target_notional: float,
) -> Optional[dict[str, float]]:
    """Return the most notional immediately fillable below the max buy price."""
    if target_notional <= 0:
        return None
    normalized = side.strip().upper()
    direct_asset_id = asset_id_for_market_side(market, normalized)
    complement_asset_id = asset_id_for_market_side(
        market, opposite_side(normalized)
    )
    if not direct_asset_id:
        return None
    max_price = configured_max_buy_price(config)
    direct_bids, direct_asks = clob_book_levels(config, direct_asset_id)
    complement_bids, _complement_asks = clob_book_levels(
        config, complement_asset_id
    )

    def build_candidate(levels: Iterable[tuple[float, float]]) -> Optional[dict[str, float]]:
        shares = 0.0
        amount = 0.0
        limit_price = 0.0
        for price, size in sorted(levels, key=lambda item: item[0]):
            if price <= 0 or price > max_price or size <= 0:
                continue
            remaining = target_notional - amount
            if remaining <= 0:
                break
            take = min(float(size), remaining / float(price))
            if take <= 0:
                continue
            shares += take
            amount += take * float(price)
            limit_price = max(limit_price, float(price))
        if shares < 1 or amount <= 0 or limit_price <= 0:
            return None
        order_shares = int(min(shares, target_notional / limit_price))
        if order_shares < 1:
            return None
        return {
            "price": round(limit_price, 2),
            "shares": float(order_shares),
            "amount_usd": round(float(order_shares) * limit_price, 2),
        }

    complement_buy_levels = []
    for bid, size in complement_bids:
        price = clamp_price(1.0 - bid)
        if price is not None:
            complement_buy_levels.append((float(price), float(size)))
    candidates = [
        candidate
        for candidate in (
            build_candidate(direct_asks),
            build_candidate(complement_buy_levels),
        )
        if candidate is not None
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            float(item["amount_usd"]),
            -float(item["price"]),
        ),
    )


def best_buy_price(
    config: dict[str, Any],
    market: TemperatureMarket,
    side: str,
    *,
    target_notional: Optional[float] = None,
    target_shares: Optional[float] = None,
) -> Optional[float]:
    """Return the best currently executable buy price for a YES/NO market side.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        side (str): Position side, expected to be YES or NO.
    
    Returns:
        Optional[float]: Best buy price from CLOB direct ask or opposite bid complement, or None if unavailable.
    
    Side effects:
        Calls the Polymarket CLOB API.
    """
    normalized = side.strip().upper()
    direct_asset_id = asset_id_for_market_side(market, normalized)
    complement_asset_id = asset_id_for_market_side(market, opposite_side(normalized))
    if not direct_asset_id:
        LOGGER.warning("clob buy unavailable missing asset_id side=%s market=%s question=%r", normalized, market.market_id, market.market_question)
        return None
    try:
        if target_shares is not None and target_shares > 0:
            depth_type = "shares"
            depth_target = float(target_shares)
        else:
            depth_type = "notional"
            depth_target = (
                float(target_notional)
                if target_notional is not None and target_notional > 0
                else depth_target_notional(config)
            )
        direct_bids, direct_asks = clob_book_levels(config, direct_asset_id)
        complement_bids, complement_asks = clob_book_levels(config, complement_asset_id)
        direct_bid = max((price for price, _ in direct_bids), default=None)
        direct_ask = min((price for price, _ in direct_asks), default=None)
        complement_bid = max((price for price, _ in complement_bids), default=None)
        complement_ask = min((price for price, _ in complement_asks), default=None)
        if depth_type == "shares":
            direct_depth_price = price_for_buy_shares(direct_asks, depth_target)
            complement_depth_price = price_for_complement_bid_shares(
                complement_bids, depth_target
            )
        else:
            direct_depth_price = price_for_buy_notional(
                direct_asks, depth_target, extra_levels=0
            )
            complement_depth_price = price_for_complement_bid_notional(
                complement_bids, depth_target, extra_levels=0
            )
        complement_bid_as_buy = clamp_price(1.0 - complement_bid) if complement_bid is not None else None
        prices = [p for p in (direct_depth_price, complement_depth_price) if p is not None]
        if not prices:
            LOGGER.info(
                "clob buy unavailable insufficient 1:1 depth side=%s market=%s direct_asset=%s direct_bids=%s direct_asks=%s direct_best_bid=%s direct_best_ask=%s complement_asset=%s complement_bids=%s complement_asks=%s complement_best_bid=%s complement_best_ask=%s depth_type=%s depth_target=%s question=%r",
                normalized,
                market.market_id,
                direct_asset_id,
                len(direct_bids),
                len(direct_asks),
                direct_bid,
                direct_ask,
                complement_asset_id,
                len(complement_bids),
                len(complement_asks),
                complement_bid,
                complement_ask,
                depth_type,
                depth_target,
                market.market_question,
            )
            return None
        selected = min(prices)
        LOGGER.info(
            "clob 1:1 depth buy price side=%s market=%s selected=%s direct_depth=%s complement_depth=%s direct_best_ask=%s complement_bid_as_buy=%s depth_type=%s depth_target=%s",
            normalized,
            market.market_id,
            selected,
            direct_depth_price,
            complement_depth_price,
            direct_ask,
            complement_bid_as_buy,
            depth_type,
            depth_target,
        )
        return selected
    except Exception:
        LOGGER.exception("clob buy query failed side=%s market=%s question=%r", normalized, market.market_id, market.market_question)
        return None


def clob_asset_buy_price(config: dict[str, Any], asset_id: str) -> Optional[float]:
    """Return a direct executable buy price for one CLOB asset id."""
    if not asset_id:
        return None
    try:
        target_notional = depth_target_notional(config)
        _, asks = clob_book_levels(config, asset_id)
        return price_for_buy_notional(asks, target_notional, extra_levels=0)
    except Exception:
        LOGGER.exception("clob asset buy query failed asset=%s", asset_id)
        return None


def clob_asset_sell_prices(config: dict[str, Any], asset_ids: list[str]) -> dict[str, Optional[float]]:
    """Batch query best ask prices for CLOB asset ids via /prices SELL."""
    unique_assets = list(dict.fromkeys(str(asset_id or "") for asset_id in asset_ids if str(asset_id or "")))
    if not unique_assets:
        return {}
    payload = [{"token_id": asset_id, "side": "SELL"} for asset_id in unique_assets]
    try:
        raw = clob_post(config, "/prices", payload)
    except Exception:
        LOGGER.exception("clob batch prices query failed assets=%s", len(unique_assets))
        return {asset_id: None for asset_id in unique_assets}
    if not isinstance(raw, dict):
        return {asset_id: None for asset_id in unique_assets}
    prices: dict[str, Optional[float]] = {}
    for asset_id in unique_assets:
        value = raw.get(asset_id)
        if isinstance(value, dict):
            value = value.get("SELL") or value.get("sell")
        try:
            prices[asset_id] = float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            prices[asset_id] = None
    return prices


def best_sell_price(config: dict[str, Any], market: TemperatureMarket, side: str) -> Optional[float]:
    """Return the best currently executable sell price for a YES/NO market side.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        side (str): Position side, expected to be YES or NO.
    
    Returns:
        Optional[float]: Best sell price from CLOB direct bid or opposite ask complement, or None if unavailable.
    
    Side effects:
        Calls the Polymarket CLOB API.
    """
    normalized = side.strip().upper()
    direct_asset_id = asset_id_for_market_side(market, normalized)
    complement_asset_id = asset_id_for_market_side(market, opposite_side(normalized))
    if not direct_asset_id:
        LOGGER.warning("clob sell unavailable missing asset_id side=%s market=%s question=%r", normalized, market.market_id, market.market_question)
        return None
    try:
        direct_bid, direct_ask, _, _ = clob_book_prices(config, direct_asset_id)
        complement_bid, complement_ask, _, _ = clob_book_prices(config, complement_asset_id)
        complement_ask_as_sell = clamp_price(1.0 - complement_ask) if complement_ask is not None else None
        prices = [p for p in (direct_bid, complement_ask_as_sell) if p is not None]
        return max(prices) if prices else None
    except Exception:
        LOGGER.exception("clob sell query failed side=%s market=%s question=%r", normalized, market.market_id, market.market_question)
        return None


def deterministic_ordered_markets(markets: list[TemperatureMarket], event_unit: str, kind: str) -> list[TemperatureMarket]:
    """Order markets in the direction the deterministic strategy evaluates for Highest or Lowest events.
    
    Args:
        markets (list[TemperatureMarket]): Temperature markets belonging to one Polymarket event.
        event_unit (str): Temperature unit used by the event markets.
        kind (str): Weather event kind, expected to be Highest or Lowest.
    
    Returns:
        list[TemperatureMarket]: Markets ordered for deterministic strategy evaluation.
    """
    ordered = sorted_markets_for_unit(markets, event_unit)
    return ordered if kind == "Highest" else list(reversed(ordered))


def outer_boundary_market(markets: list[TemperatureMarket], event_unit: str, kind: str) -> Optional[TemperatureMarket]:
    """Return the outside boundary market for a Highest or Lowest event."""
    ordered = sorted_markets_for_unit(markets, event_unit)
    if not ordered:
        return None
    return ordered[-1] if kind == "Highest" else ordered[0]


def deterministic_market_impossible(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    """Determine whether observed extremes make a market impossible to resolve YES.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        kind (str): Weather event kind, expected to be Highest or Lowest.
        observed_high (Optional[float]): Observed high temperature for the event day, if available.
        observed_low (Optional[float]): Observed low temperature for the event day, if available.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        bool: True when the market can no longer resolve YES.
    """
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        return observed_high is not None and hi is not None and float(observed_high) > hi
    return observed_low is not None and lo is not None and float(observed_low) < lo


def deterministic_impossible_markets_by_proximity(
    markets: list[TemperatureMarket],
    kind: str,
    observed_high: Optional[float],
    observed_low: Optional[float],
    unit: str,
) -> list[TemperatureMarket]:
    """Return impossible markets ordered from nearest to the observed extreme outward."""
    normalized_kind = kind.strip()
    candidates: list[tuple[float, float, str, TemperatureMarket]] = []
    for market in markets:
        if market.closed or not deterministic_market_impossible(market, normalized_kind, observed_high, observed_low, unit):
            continue
        lo, hi, _ = comparable_rule_bounds(market, unit)
        if normalized_kind == "Highest":
            if observed_high is None or hi is None:
                continue
            distance = float(observed_high) - float(hi)
            width = (float(hi) - float(lo)) if lo is not None else 9999.0
        else:
            if observed_low is None or lo is None:
                continue
            distance = float(lo) - float(observed_low)
            width = (float(hi) - float(lo)) if hi is not None else 9999.0
        candidates.append((distance, width, market.market_id, market))
    return [market for _, _, _, market in sorted(candidates, key=lambda item: (item[0], item[1], item[2]))]


def adjacent_no_momentum_market(
    markets: list[TemperatureMarket],
    kind: str,
    observed_high: Optional[float],
    observed_low: Optional[float],
    unit: str,
) -> Optional[TemperatureMarket]:
    """Pick the nearest NO market to watch for active CLOB momentum."""
    impossible = deterministic_impossible_markets_by_proximity(markets, kind, observed_high, observed_low, unit)
    if impossible:
        return impossible[0]
    ordered = sorted_markets_for_unit(markets, unit)
    if not ordered:
        return None
    normalized_kind = kind.strip()
    candidates: list[tuple[float, float, str, TemperatureMarket]] = []
    if normalized_kind == "Highest":
        if observed_high is None:
            return ordered[0]
        for market in ordered:
            lo, hi, _ = comparable_rule_bounds(market, unit)
            if hi is None or float(hi) < float(observed_high):
                continue
            distance = float(hi) - float(observed_high)
            width = (float(hi) - float(lo)) if lo is not None else 9999.0
            candidates.append((distance, width, market.market_id, market))
    else:
        if observed_low is None:
            return ordered[-1]
        for market in ordered:
            lo, hi, _ = comparable_rule_bounds(market, unit)
            if lo is None or float(lo) > float(observed_low):
                continue
            distance = float(observed_low) - float(lo)
            width = (float(hi) - float(lo)) if hi is not None else 9999.0
            candidates.append((distance, width, market.market_id, market))
    if not candidates:
        return ordered[0] if normalized_kind == "Highest" else ordered[-1]
    return sorted(candidates, key=lambda item: (item[0], item[1], item[2]))[0][3]


def deterministic_no_possible_again(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    """Determine whether corrected observations make an existing NO position unsafe to keep.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        kind (str): Weather event kind, expected to be Highest or Lowest.
        observed_high (Optional[float]): Observed high temperature for the event day, if available.
        observed_low (Optional[float]): Observed low temperature for the event day, if available.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        bool: True when an existing NO should be unwound because the market may be possible again.
    """
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        return hi is None or observed_high is None or float(observed_high) <= hi
    return lo is None or observed_low is None or float(observed_low) >= lo


def deterministic_extreme_market_reached(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    """Determine whether the extreme bucket has been reached by current observations.
    
    Args:
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        kind (str): Weather event kind, expected to be Highest or Lowest.
        observed_high (Optional[float]): Observed high temperature for the event day, if available.
        observed_low (Optional[float]): Observed low temperature for the event day, if available.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        bool: True when observations reached the extreme market bucket.
    """
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        if observed_high is None:
            return False
        return market_contains_temperature(market, float(observed_high), unit) if hi is not None else lo is None or float(observed_high) >= lo
    if observed_low is None:
        return False
    return market_contains_temperature(market, float(observed_low), unit) if lo is not None else hi is None or float(observed_low) <= hi


def trade_observation_verified(trade: PaperTrade, market: TemperatureMarket, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    """Check whether current observations support the side held by an open trade.
    
    Args:
        trade (PaperTrade): PaperTrade record being evaluated or mutated.
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        observed_high (Optional[float]): Observed high temperature for the event day, if available.
        observed_low (Optional[float]): Observed low temperature for the event day, if available.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        bool: True when observations still support the held side.
    """
    side = (trade.position_side or "YES").upper()
    if side == "NO":
        return deterministic_market_impossible(market, trade.kind, observed_high, observed_low, unit)
    return deterministic_extreme_market_reached(market, trade.kind, observed_high, observed_low, unit)


def twc_units_for_temperature_unit(unit: str) -> str:
    """Map market temperature units to Weather Company API unit codes.
    
    Args:
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        str: Weather Company unit code.
    """
    return "m" if unit.upper() == "C" else "e"


def twc_historical_observations_by_icao(config: dict[str, Any], icao_code: str, units: str, event_date: str) -> dict[str, Any]:
    """Fetch Weather Company historical observations for one ICAO station and event date.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        icao_code (str): ICAO station code used by the Weather Company historical endpoint.
        units (str): Weather Company API unit code.
        event_date (str): Event date in ISO yyyy-mm-dd format.
    
    Returns:
        dict[str, Any]: Weather Company historical observation payload.
    
    Side effects:
        Calls the Weather Company API.
    """
    ymd = event_date.replace("-", "")
    return twc_get(config, f"/v1/location/{icao_code}:9:US/observations/historical.json", {"units": units, "startDate": ymd, "endDate": ymd})


def parse_twc_obs_time(row: dict[str, Any]) -> Optional[datetime]:
    """Parse an observation timestamp from a Weather Company observation row.
    
    Args:
        row (dict[str, Any]): Raw CSV, position, or observation row.
    
    Returns:
        Optional[datetime]: Observation datetime, or None if no valid timestamp exists.
    """
    value = row.get("valid_time_gmt") or row.get("expire_time_gmt") or row.get("obsTime")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def observed_twc_observation_points(payload: dict[str, Any], event_date: str, current_local: Optional[datetime]) -> list[tuple[datetime, float]]:
    """Filter Weather Company observations to points on the event day up to the current local time.
    
    Args:
        payload (dict[str, Any]): Decoded JSON response payload.
        event_date (str): Event date in ISO yyyy-mm-dd format.
        current_local (Optional[datetime]): Current local datetime used to discard future observations.
    
    Returns:
        list[tuple[datetime, float]]: Filtered datetime/temperature observations.
    """
    rows = payload.get("observations") or payload.get("observation") or []
    points: list[tuple[datetime, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        temp = row.get("temp") if row.get("temp") is not None else row.get("temperature")
        obs_dt = parse_twc_obs_time(row)
        if temp is None or obs_dt is None:
            continue
        local_dt = obs_dt.astimezone(current_local.tzinfo) if current_local and current_local.tzinfo else obs_dt
        if local_dt.date().isoformat() != event_date or (current_local and local_dt > current_local):
            continue
        try:
            points.append((local_dt, float(temp)))
        except (TypeError, ValueError):
            continue
    return points


def summarize_points(points: list[tuple[datetime, float]]) -> tuple[Optional[float], Optional[float], str, str, list[str], list[Any]]:
    """Summarize observation points into high, low, first/last time, and value arrays.
    
    Args:
        points (list[tuple[datetime, float]]): Observation points as datetime/temperature pairs.
    
    Returns:
        tuple[Optional[float], Optional[float], str, str, list[str], list[Any]]: High, low, first time, last time, timestamp list, and temperature list.
    """
    if not points:
        return None, None, "", "", [], []
    sorted_points = sorted(points, key=lambda item: item[0])
    temps = [temp for _, temp in sorted_points]
    times = [dt.isoformat() for dt, _ in sorted_points]
    return max(temps), min(temps), times[0], times[-1], times, temps


def deterministic_observed_extremes_from_twc(config: dict[str, Any], station: str, event_date: str, city_local_dt: Optional[datetime], unit: str) -> tuple[Optional[float], Optional[float], list[tuple[datetime, float]], dict[str, Any]]:
    """Fetch and summarize TWC observations used to verify METAR-triggered positions.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        station (str): Weather station identifier, usually a four-character ICAO code.
        event_date (str): Event date in ISO yyyy-mm-dd format.
        city_local_dt (Optional[datetime]): Current city-local datetime used to bound historical observations.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        tuple[Optional[float], Optional[float], list[tuple[datetime, float]], dict[str, Any]]: Observed high, low, filtered points, and raw payload.
    """
    if city_local_dt is not None:
        try:
            target_date = date.fromisoformat(event_date)
        except ValueError:
            target_date = None
        if target_date is not None and city_local_dt.date() < target_date:
            LOGGER.info(
                "twc historical skip before local event day station=%s event_date=%s city_local=%s",
                station,
                event_date,
                city_local_dt.isoformat(timespec="seconds"),
            )
            return None, None, [], {}
    try:
        payload = twc_historical_observations_by_icao(config, station, twc_units_for_temperature_unit(unit), event_date)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 400:
            LOGGER.warning(
                "twc historical unavailable station=%s event_date=%s unit=%s status=400 url=%s",
                station,
                event_date,
                unit,
                exc.response.url if exc.response is not None else "",
            )
            return None, None, [], {}
        raise
    points = observed_twc_observation_points(payload, event_date, city_local_dt)
    high, low, _, _, _, _ = summarize_points(points)
    return high, low, points, payload


def aviation_metar_observations(station: str, hours: int = 24) -> list[dict[str, Any]]:
    """Fetch recent METAR observations from AviationWeather for one station.
    
    Args:
        station (str): Weather station identifier, usually a four-character ICAO code.
        hours (int): METAR lookback window in hours.
    
    Returns:
        list[dict[str, Any]]: METAR observation rows.
    
    Side effects:
        Calls the AviationWeather METAR API.
    """
    r = requests.get("https://aviationweather.gov/api/data/metar", headers=HEADERS, params={"ids": station, "hours": hours, "format": "json"}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, list) else []


def model_awc_fetch_history_with_retry(
    config: dict[str, Any],
    station: str,
    hours: int,
    reason: str,
) -> list[dict[str, Any]]:
    """Fetch AWC model history with the configured bounded retry policy."""
    trading = config["trading"]
    max_attempts = max(1, int(trading.get("model_awc_awc_max_attempts", 3)))
    retry_interval = max(0.0, float(trading.get("model_awc_awc_retry_interval_seconds", 3)))
    for attempt in range(1, max_attempts + 1):
        try:
            rows = aviation_metar_observations(station, hours)
            append_aviation_metar_history(config, station, rows, reason)
            if rows:
                return rows
            LOGGER.warning(
                "model awc history empty station=%s hours=%s reason=%s attempt=%s/%s",
                station,
                hours,
                reason,
                attempt,
                max_attempts,
            )
        except Exception as exc:
            LOGGER.warning(
                "model awc history fetch failed station=%s hours=%s reason=%s attempt=%s/%s error=%s",
                station,
                hours,
                reason,
                attempt,
                max_attempts,
                exc,
            )
        if attempt < max_attempts and retry_interval > 0:
            time.sleep(retry_interval)
    return []


def append_aviation_metar_history(config: dict[str, Any], station: str, rows: list[dict[str, Any]], reason: str) -> None:
    """Raw AviationWeather audit writes are disabled to keep disk usage low.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including output paths.
        station (str): Weather station identifier.
        rows (list[dict[str, Any]]): Raw AviationWeather METAR rows.
        reason (str): Caller reason for the fetch.
    
    Returns:
        None: This function is executed for its side effects.
    """
    return


def append_price_window_record(config: dict[str, Any], record: dict[str, Any]) -> None:
    """Append one Polymarket websocket price record captured during an observation window."""
    path = str(config.get("outputs", {}).get("price_window_ticks_jsonl") or "price_window_ticks.jsonl")
    with IO_LOCK, open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def append_price_window_raw_record(config: dict[str, Any], message: Any) -> None:
    """Append exactly one Polymarket websocket payload as received."""
    path = str(config.get("outputs", {}).get("price_window_raw_jsonl") or "price_window_raw.jsonl")
    raw = message if isinstance(message, str) else json.dumps(message, separators=(",", ":"), ensure_ascii=False)
    with IO_LOCK, open(path, "a", encoding="utf-8") as f:
        f.write(raw.rstrip("\r\n") + "\n")


def parse_aviation_obs_time(value: Any) -> Optional[datetime]:
    """Parse a METAR observation timestamp into UTC datetime.
    
    Args:
        value (Any): Raw value to parse or normalize.
    
    Returns:
        Optional[datetime]: UTC datetime, or None if parsing fails.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def city_local_now(config: dict[str, Any], city: str) -> tuple[datetime, str, str]:
    """Return the current local datetime and timezone metadata for a city.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        city (str): Canonical city name parsed from a Polymarket weather event.
    
    Returns:
        tuple[datetime, str, str]: Current local datetime, timezone name, and ISO timestamp.
    """
    tz_name = config["events"].get("city_timezones", {}).get(city, "UTC")
    value = datetime.now(city_timezone(config, city))
    return value, tz_name, value.isoformat(timespec="seconds")


def aviation_hours_for_local_event_day(city_tz: ZoneInfo, event_date: str) -> int:
    """Choose a METAR lookback window that covers the local event day.
    
    Args:
        city_tz (ZoneInfo): City timezone used to determine the local event day.
        event_date (str): Event date in ISO yyyy-mm-dd format.
    
    Returns:
        int: Lookback hours for the METAR request.
    """
    target_date = date.fromisoformat(event_date)
    local_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=city_tz)
    hours_since_local_start = (datetime.now(timezone.utc) - local_start.astimezone(timezone.utc)).total_seconds() / 3600.0
    return max(24, min(48, int(hours_since_local_start) + 3))


def aviation_observed_extremes(config: dict[str, Any], station: str, city: str, event_date: str, unit: str) -> tuple[Optional[float], Optional[float], Optional[datetime], list[tuple[datetime, float]]]:
    """Fetch METAR observations and compute high/low extremes for the event local day.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        station (str): Weather station identifier, usually a four-character ICAO code.
        city (str): Canonical city name parsed from a Polymarket weather event.
        event_date (str): Event date in ISO yyyy-mm-dd format.
        unit (str): Temperature unit for observations and comparisons.
    
    Returns:
        tuple[Optional[float], Optional[float], Optional[datetime], list[tuple[datetime, float]]]: Observed high, low, latest observation time, and filtered points.
    """
    city_tz = city_timezone(config, city)
    target_date = date.fromisoformat(event_date)
    now_local = datetime.now(city_tz)
    if now_local.date() < target_date:
        LOGGER.info(
            "aviation metar skip before local event day station=%s city=%s event_date=%s city_local=%s",
            station,
            city,
            event_date,
            now_local.isoformat(timespec="seconds"),
        )
        return None, None, None, []
    points: list[tuple[datetime, float]] = []
    rows = aviation_metar_observations(station, aviation_hours_for_local_event_day(city_tz, event_date))
    append_aviation_metar_history(config, station, rows, "observed_extremes")
    for row in rows:
        obs_dt_utc = parse_aviation_obs_time(row.get("obsTime") or row.get("reportTime") or row.get("receiptTime"))
        raw_temp = row.get("temp")
        if obs_dt_utc is None or raw_temp is None:
            continue
        local_dt = obs_dt_utc.astimezone(city_tz)
        if local_dt.date().isoformat() != event_date:
            continue
        temp_f = convert_temperature(float(raw_temp), "C", "F")
        temp = convert_temperature(temp_f, "F", unit) if unit.upper() != "F" else temp_f
        if temp is not None:
            points.append((local_dt, round(float(temp))))
    high, low, _, _, _, _ = summarize_points(points)
    latest_dt = max((dt for dt, _ in points), default=None)
    return high, low, latest_dt, points


def model_awc_enabled(config: dict[str, Any]) -> bool:
    """Return whether the AWC METAR model strategy should run."""
    return bool(config.get("trading", {}).get("model_awc_enabled", True))


def model_awc_live_stations(config: dict[str, Any]) -> set[str]:
    """Return stations allowed to place real live orders."""
    trading = config.get("trading", {})
    raw_stations = trading.get("model_awc_live_stations", trading.get("model_awc_live_station", "KAUS"))
    if isinstance(raw_stations, str):
        stations = [part.strip() for part in raw_stations.split(",")]
    elif isinstance(raw_stations, (list, tuple, set)):
        stations = [str(part).strip() for part in raw_stations]
    else:
        stations = ["KAUS"]
    return {station.upper() for station in stations if station}


def model_awc_live_station(config: dict[str, Any]) -> str:
    """Return a display value for model AWC live stations."""
    stations = sorted(model_awc_live_stations(config))
    return ",".join(stations) if stations else "KAUS"


def configured_max_buy_price(config: dict[str, Any]) -> float:
    """Return the configured max live buy price, clamped to a valid probability range."""
    raw = config.get("trading", {}).get("max_buy_price", 0.85)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0.85
    if value <= 0 or value >= 1:
        return 0.85
    return value


def model_awc_load_model(config: dict[str, Any]) -> Any:
    """Load and cache the latest trained LightGBM model."""
    global MODEL_AWC_MODEL, MODEL_AWC_MODEL_PATH
    path = str(config.get("trading", {}).get("model_awc_model_path") or "").strip()
    if not path:
        raise RuntimeError("trading.model_awc_model_path is required")
    if os.sep == "/" and "\\" in path:
        path = path.replace("\\", "/")
    abs_path = os.path.abspath(path)
    if MODEL_AWC_MODEL is None or MODEL_AWC_MODEL_PATH != abs_path:
        MODEL_AWC_MODEL = joblib.load(abs_path)
        MODEL_AWC_MODEL_PATH = abs_path
        LOGGER.info("model awc loaded path=%s features=%s", abs_path, len(getattr(MODEL_AWC_MODEL, "feature_name_", [])))
    return MODEL_AWC_MODEL


def model_awc_model_unit(config: dict[str, Any]) -> str:
    """Return the temperature unit produced by the configured model."""
    configured = str(config.get("trading", {}).get("model_awc_model_unit") or "").upper()
    if configured in {"C", "F"}:
        return configured
    feature_names = {str(name) for name in getattr(model_awc_load_model(config), "feature_name_", [])}
    return "C" if "temp_c" in feature_names and "temp_f" not in feature_names else "F"


def model_awc_station_id(config: dict[str, Any], station: str) -> Optional[int]:
    """Resolve the numeric station category used during model training."""
    configured = config.get("trading", {}).get("model_awc_station_ids", {})
    if isinstance(configured, dict) and station.upper() in configured:
        return int(configured[station.upper()])
    return FEATURE_STATION_IDS.get(station.upper())


def model_awc_required_lag_hours(config: dict[str, Any]) -> int:
    """Return the largest hourly temperature lag required by the loaded model."""
    model = model_awc_load_model(config)
    feature_names = list(getattr(model, "feature_name_", []))
    lag_hours = [
        int(match.group(1))
        for name in feature_names
        for match in [re.fullmatch(r"temp_[fc]_lag_(\d+)h", str(name))]
        if match
    ]
    return max(lag_hours, default=3)


def model_awc_required_context_hours(config: dict[str, Any]) -> int:
    """Return the observation/SPECI context window required by the loaded model."""
    model = model_awc_load_model(config)
    feature_names = list(getattr(model, "feature_name_", []))
    context_hours = [
        int(match.group(1))
        for name in feature_names
        for match in [re.search(r"_past_(\d+)h$", str(name))]
        if match
    ]
    return max(context_hours, default=6)


def model_awc_requires_previous_day_context(config: dict[str, Any]) -> bool:
    """Return whether the loaded model expects previous/current local-day context columns."""
    model = model_awc_load_model(config)
    feature_names = {str(name) for name in getattr(model, "feature_name_", [])}
    return bool(
        {
            "previous_local_day_high_f",
            "previous_local_day_low_f",
            "current_local_day_min_temp_f_so_far",
            "previous_local_day_high_c",
            "previous_local_day_low_c",
            "current_local_day_min_temp_c_so_far",
        }
        & feature_names
    )


def model_awc_required_history_hours(config: dict[str, Any]) -> int:
    """Return the minimum AWC lookback needed by the loaded model features."""
    required = max(model_awc_required_lag_hours(config), model_awc_required_context_hours(config)) + 1
    if model_awc_requires_previous_day_context(config):
        required = max(required, 48)
    return required


def model_awc_parse_row(row: dict[str, Any], station: str) -> Optional[FeatureMetarRow]:
    """Convert one AviationWeather JSON row into the feature pipeline row shape."""
    raw_metar = str(row.get("rawOb") or row.get("raw") or row.get("metar") or "").strip()
    if not raw_metar or " AUTO " in f" {raw_metar} ":
        return None
    obs_dt = parse_aviation_obs_time(row.get("obsTime") or row.get("reportTime") or row.get("receiptTime"))
    if obs_dt is None:
        return None
    short_station = station[1:] if station.upper().startswith("K") else station.upper()
    return FeatureMetarRow(
        daily_high_f="",
        station=short_station,
        valid_utc=obs_dt,
        valid_text=obs_dt.strftime("%Y-%m-%d %H:%M"),
        metar=raw_metar,
    )


def model_awc_station_observation_minutes(config: dict[str, Any], station: str) -> tuple[int, ...]:
    """Return configured regular METAR minutes for a station."""
    station = station.upper()
    fallback = int(config["trading"].get("model_awc_observation_minute", 53))
    station_minutes = config["trading"].get("model_awc_station_observation_minutes", {})
    if isinstance(station_minutes, dict):
        try:
            raw = station_minutes.get(station, fallback)
            values = raw if isinstance(raw, (list, tuple, set)) else [raw]
            minutes = tuple(sorted({min(59, max(0, int(value))) for value in values}))
            return minutes or (min(59, max(0, fallback)),)
        except (TypeError, ValueError):
            pass
    return (min(59, max(0, fallback)),)


def model_awc_station_observation_minute(config: dict[str, Any], station: str) -> int:
    """Return the first configured METAR observation minute (legacy helper)."""
    return model_awc_station_observation_minutes(config, station)[0]


def model_awc_station_buy_hours(config: dict[str, Any], station: str) -> tuple[int, int]:
    """Return the inclusive local-hour trading window for a station."""
    trading = config.get("trading", {})
    start_hour = int(trading.get("model_awc_buy_start_hour", 12))
    end_hour = int(trading.get("model_awc_buy_end_hour", 16))
    overrides = trading.get("model_awc_station_buy_hours", {})
    station_hours = overrides.get(station.upper()) if isinstance(overrides, dict) else None
    if isinstance(station_hours, (list, tuple)) and len(station_hours) == 2:
        start_hour, end_hour = int(station_hours[0]), int(station_hours[1])
    if not 0 <= start_hour <= end_hour <= 23:
        raise ValueError(f"invalid model AWC buy hours for {station}: {start_hour}-{end_hour}")
    return start_hour, end_hour


def model_awc_station_stagger_seconds(config: dict[str, Any], station: str) -> int:
    """Stagger stations sharing the same observation minute to reduce API bursts."""
    station = station.upper()
    station_minutes = config["trading"].get("model_awc_station_observation_minutes", {})
    if not isinstance(station_minutes, dict) or station not in station_minutes:
        return 0
    try:
        minute = int(station_minutes[station])
    except (TypeError, ValueError):
        return 0
    same_minute = []
    for candidate, candidate_minute in station_minutes.items():
        try:
            if int(candidate_minute) == minute:
                same_minute.append(str(candidate).upper())
        except (TypeError, ValueError):
            continue
    if station not in same_minute:
        return 0
    stagger = max(0, int(config["trading"].get("model_awc_station_poll_stagger_seconds", 5)))
    return same_minute.index(station) * stagger


def model_awc_is_extra_metar_row(row: FeatureMetarRow, regular_minute: int | Iterable[int]) -> bool:
    """Return whether a parsed AWC row should be used only as context, not as a model trigger row."""
    regular_minutes = {int(regular_minute)} if isinstance(regular_minute, int) else {int(v) for v in regular_minute}
    return row.valid_utc.minute not in regular_minutes or " COR " in f" {row.metar} "


def model_awc_observed_high_f_so_far(
    rows: list[FeatureMetarRow],
    temp_values: list[object | None],
    tz: ZoneInfo,
    as_of_utc: datetime,
) -> Optional[float]:
    """Return the factual local-day METAR high through the trigger observation."""
    target_date = as_of_utc.astimezone(tz).date()
    observed = []
    for row, value in zip(rows, temp_values):
        if row.valid_utc > as_of_utc or row.valid_utc.astimezone(tz).date() != target_date:
            continue
        try:
            if value not in (None, ""):
                observed.append(float(value))
        except (TypeError, ValueError):
            continue
    return max(observed) if observed else None


def model_awc_feature_row(
    config: dict[str, Any],
    city: str,
    station: str,
    rows: list[dict[str, Any]],
    event_date: str,
) -> Optional[tuple[dict[str, Any], FeatureMetarRow, datetime]]:
    """Build one current model feature row from recent AWC METAR rows."""
    station = station.upper()
    tz = city_timezone(config, city)
    target_date = date.fromisoformat(event_date)
    previous_date = target_date - timedelta(days=1)
    parsed = []
    for row in rows:
        item = model_awc_parse_row(row, station)
        if item is None:
            continue
        local_dt = item.valid_utc.astimezone(tz)
        if local_dt.date() not in {previous_date, target_date}:
            continue
        parsed.append(item)
    parsed.sort(key=lambda item: item.valid_utc)
    if not parsed:
        return None

    regular_minutes = model_awc_station_observation_minutes(config, station)
    extra_flags = [model_awc_is_extra_metar_row(item, regular_minutes) for item in parsed]
    regular_inputs = [
        item
        for item, is_extra in zip(parsed, extra_flags)
        if not is_extra and item.valid_utc.astimezone(tz).date() == target_date
    ]
    if not regular_inputs:
        LOGGER.info("model awc skip no regular input rows station=%s rows=%s regular_minutes=%s", station, len(parsed), regular_minutes)
        return None

    latest = regular_inputs[-1]
    latest_local = latest.valid_utc.astimezone(tz)
    start_hour, end_hour = model_awc_station_buy_hours(config, station)
    if latest_local.hour < start_hour or latest_local.hour > end_hour:
        return None

    features = decode_metar(latest, station, tz)
    model_unit = model_awc_model_unit(config)
    features["station_id"] = model_awc_station_id(config, station)
    if model_unit == "C":
        features["temp_c_equivalent"] = features.get("temp_c")
        features["dewpoint_c_equivalent"] = features.get("dewpoint_c")
        heat_index_f = features.get("heat_index_f")
        wind_chill_f = features.get("wind_chill_f")
        features["heat_index_c"] = None if heat_index_f is None else (float(heat_index_f) - 32.0) * 5.0 / 9.0
        features["wind_chill_c"] = None if wind_chill_f is None else (float(wind_chill_f) - 32.0) * 5.0 / 9.0
    iso = latest_local.isocalendar()
    features["local_week_of_year"] = float(iso.week)
    valid_times = [item.valid_utc for item in parsed]
    decoded_rows = [decode_metar(item, station, tz) for item in parsed]
    temp_key = "temp_c" if model_unit == "C" else "temp_f"
    temp_values = [decoded.get(temp_key) for decoded in decoded_rows]
    features["_observed_local_day_high_model_unit_so_far"] = model_awc_observed_high_f_so_far(
        parsed,
        temp_values,
        tz,
        latest.valid_utc,
    )
    previous_highs, previous_lows, current_mins = daily_temperature_context_series(parsed, temp_values, tz)
    tolerance = timedelta(minutes=int(config["trading"].get("model_awc_lag_tolerance_minutes", 30)))
    current_temp = features.get(temp_key)
    for hours in range(1, model_awc_required_lag_hours(config) + 1):
        lag_value = nearest_lag_value(latest.valid_utc - timedelta(hours=hours), valid_times, temp_values, tolerance)
        features[f"temp_{model_unit.lower()}_lag_{hours}h"] = lag_value
        features[f"temp_{model_unit.lower()}_change_{hours}h"] = (
            None if current_temp is None or lag_value is None else float(current_temp) - float(lag_value)
        )
    latest_idx = parsed.index(latest)
    daily_context: dict[str, Any] = {}
    add_daily_temperature_context_features(daily_context, latest_idx, previous_highs, previous_lows, current_mins)
    suffix = model_unit.lower()
    if model_unit == "C":
        features["previous_local_day_high_c"] = daily_context.get("previous_local_day_high_f")
        features["previous_local_day_low_c"] = daily_context.get("previous_local_day_low_f")
        features["current_local_day_min_temp_c_so_far"] = daily_context.get(
            "current_local_day_min_temp_f_so_far"
        )
    else:
        features.update(daily_context)
    context_hours = model_awc_required_context_hours(config)
    observation_context = dict(features)
    if model_unit == "C":
        observation_context["temp_f"] = current_temp
    add_observation_context_features(
        features=observation_context,
        row=latest,
        row_idx=latest_idx,
        valid_times=valid_times,
        temp_values=temp_values,
        extra_flags=extra_flags,
        window=timedelta(hours=context_hours),
        context_hours=context_hours,
    )
    for key, value in observation_context.items():
        if key not in features or key.startswith(("metar_obs_", "extra_metar_", "has_extra_", "is_extra_", "temp_f_change_from_")):
            output_key = key.replace("temp_f", "temp_c") if model_unit == "C" else key
            output_key = output_key.replace("range_f", "range_c") if model_unit == "C" else output_key
            features[output_key] = value

    # The NONUS model contains ten additional half-hour delta features.
    feature_names = {str(name) for name in getattr(model_awc_load_model(config), "feature_name_", [])}
    for half_step in range(1, 20, 2):
        hours_back = half_step / 2.0
        whole = int(hours_back)
        name = f"temp_{suffix}_change_{whole}_5h"
        if name not in feature_names:
            continue
        lag_value = nearest_lag_value(
            latest.valid_utc - timedelta(minutes=half_step * 30),
            valid_times,
            temp_values,
            tolerance,
        )
        features[name] = None if current_temp is None or lag_value is None else float(current_temp) - float(lag_value)
    return features, latest, latest_local


def model_awc_predict_high(config: dict[str, Any], features: dict[str, Any]) -> float:
    """Return the model high, bounded below by the factual observed high."""
    model = model_awc_load_model(config)
    feature_names = list(getattr(model, "feature_name_", []))
    if not feature_names:
        raise RuntimeError("Loaded model does not expose feature_name_")
    row = []
    for name in feature_names:
        value = features.get(name)
        if value in (None, ""):
            row.append(float("nan"))
        else:
            try:
                row.append(float(value))
            except (TypeError, ValueError):
                row.append(float("nan"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        prediction = model.predict([row], num_iteration=getattr(model, "best_iteration_", None))
    raw_prediction = float(prediction[0])
    observed_high = features.get(
        "_observed_local_day_high_model_unit_so_far",
        features.get("_observed_local_day_high_f_so_far"),
    )
    try:
        factual_floor = None if observed_high in (None, "") else float(observed_high)
    except (TypeError, ValueError):
        factual_floor = None
    if factual_floor is not None and raw_prediction < factual_floor:
        LOGGER.info(
            "model awc prediction raised to observed high raw_prediction_f=%r "
            "observed_high_f=%r",
            raw_prediction,
            factual_floor,
        )
        raw_prediction = factual_floor
    return (
        convert_temperature(raw_prediction, "C", "F")
        if model_awc_model_unit(config) == "C"
        else raw_prediction
    )


def model_awc_trade_exists_for_window(
    trades: list[PaperTrade],
    strategy_name: str,
    city: str,
    station: str,
    event_date: str,
    local_hour: int,
    local_minute: Optional[int] = None,
) -> bool:
    """Prevent duplicate entries for the same model observation window."""
    marker = f"model_awc_high:{city}:{station}:{event_date}:hour_{local_hour:02d}"
    if local_minute is not None:
        marker += f"_minute_{int(local_minute):02d}"
    active_statuses = {"OPEN", "BUY_PENDING", "SELL_PENDING"}
    return any(t.strategy == strategy_name and t.status in active_statuses and marker in t.cycle_id for t in trades)


def model_awc_active_event_positions(trades: list[PaperTrade], strategy_name: str, city: str, event_date: str) -> list[PaperTrade]:
    """Return active model-relevant positions for one city and event date."""
    active_statuses = {"OPEN", "BUY_PENDING", "SELL_PENDING", "HEDGED"}
    return [
        trade
        for trade in trades
        if trade.status in active_statuses
        and trade.strategy == strategy_name
        and trade.city == city
        and trade.kind == "Highest"
        and trade.event_date == event_date
    ]


def model_awc_entry_unit_price(trade: PaperTrade) -> float:
    """Return the held unit entry price, preferring the recorded fill price over fee-inclusive cost."""
    price = safe_float(trade.yes_price, 0.0)
    if price > 0:
        return price
    shares = safe_float(trade.shares, 0.0)
    if shares <= 0:
        return 0.0
    return safe_float(trade.total_cost_usdc, 0.0) / shares


def model_awc_adjacent_yes_position_summary(
    active_positions: list[PaperTrade],
    adjacent_markets: tuple[TemperatureMarket, TemperatureMarket],
) -> dict[str, dict[str, float]]:
    """Aggregate held YES shares and weighted entry price for the two adjacent markets."""
    adjacent_ids = {market.market_id for market in adjacent_markets}
    summary: dict[str, dict[str, float]] = {}
    for trade in active_positions:
        if (trade.position_side or "YES").upper() != "YES" or trade.market_id not in adjacent_ids:
            continue
        shares = safe_float(trade.shares, 0.0)
        if shares <= 0:
            continue
        price = model_awc_entry_unit_price(trade)
        row = summary.setdefault(trade.market_id, {"shares": 0.0, "weighted_cost": 0.0})
        row["shares"] += shares
        row["weighted_cost"] += shares * price
    for row in summary.values():
        shares = row["shares"]
        row["avg_price"] = row["weighted_cost"] / shares if shares > 0 else 0.0
    return summary


def model_awc_market_candidate(
    config: dict[str, Any],
    market: TemperatureMarket,
    predicted_high_f: float,
    event_unit: str,
    predicted_yes_market_id: str,
) -> Optional[dict[str, Any]]:
    """Return the YES/NO side and executable price implied by one market range."""
    predicted = convert_temperature(predicted_high_f, "F", event_unit)
    if predicted is None:
        return None
    side = "YES" if market.market_id == predicted_yes_market_id else "NO"
    price = best_buy_price(config, market, side)
    if price is None or price <= 0:
        return None
    return {"market": market, "side": side, "price": float(price)}


def model_awc_predicted_yes_market(
    markets: list[TemperatureMarket],
    predicted_high_f: float,
    event_unit: str,
) -> Optional[TemperatureMarket]:
    """Return the unique market interval that contains the model high prediction."""
    predicted = convert_temperature(predicted_high_f, "F", event_unit)
    if predicted is None:
        return None
    matches = []
    for market in markets:
        if market_contains_temperature(market, float(predicted), event_unit):
            matches.append(market)
    if len(matches) != 1:
        return None
    return matches[0]


def model_awc_prediction_matches(
    markets: list[TemperatureMarket],
    predicted_high_f: float,
    event_unit: str,
) -> list[TemperatureMarket]:
    """Return all market intervals containing the model high prediction."""
    predicted = convert_temperature(predicted_high_f, "F", event_unit)
    if predicted is None:
        return []
    return [market for market in markets if market_contains_temperature(market, float(predicted), event_unit)]


def model_awc_interval_snap_tolerance(config: dict[str, Any], event_unit: str) -> float:
    """Return the configured boundary snap tolerance in the event market unit."""
    if event_unit.upper() == "C" and "model_awc_interval_snap_tolerance_c" in config["trading"]:
        return max(0.0, float(config["trading"]["model_awc_interval_snap_tolerance_c"]))
    tolerance_f = max(0.0, float(config["trading"].get("model_awc_interval_snap_tolerance_f", 0.15)))
    return tolerance_f * 5.0 / 9.0 if event_unit.upper() == "C" else tolerance_f


def model_awc_boundary_snap_market(
    config: dict[str, Any],
    markets: list[TemperatureMarket],
    predicted_high_f: float,
    event_unit: str,
) -> Optional[tuple[TemperatureMarket, float]]:
    """Return a nearby interval when the model prediction is just outside its boundary."""
    predicted = convert_temperature(predicted_high_f, "F", event_unit)
    if predicted is None:
        return None
    tolerance = model_awc_interval_snap_tolerance(config, event_unit)
    if tolerance <= 0:
        return None
    epsilon = 1e-9
    candidates: list[tuple[float, TemperatureMarket]] = []
    for market in markets:
        lo, hi, _ = comparable_rule_bounds(market, event_unit)
        if lo is not None and float(predicted) < lo:
            distance = lo - float(predicted)
            if distance <= tolerance + epsilon:
                candidates.append((distance, market))
        if hi is not None and float(predicted) > hi:
            distance = float(predicted) - hi
            if distance <= tolerance + epsilon:
                candidates.append((distance, market))
    if not candidates:
        return None
    distance, market = min(candidates, key=lambda item: (item[0], str(item[1].market_id)))
    return market, distance


def model_awc_adjacent_prediction_markets(
    markets: list[TemperatureMarket],
    predicted_high_f: float,
    event_unit: str,
) -> Optional[tuple[TemperatureMarket, TemperatureMarket]]:
    """Return two adjacent markets around an imprecise model high prediction."""
    predicted = convert_temperature(predicted_high_f, "F", event_unit)
    if predicted is None:
        return None
    matched = [market for market in markets if market_contains_temperature(market, float(predicted), event_unit)]
    if len(matched) == 2:
        ordered = sorted_markets_for_unit(matched, event_unit)
        return ordered[0], ordered[1]
    if matched:
        return None

    lower_candidates: list[tuple[float, TemperatureMarket]] = []
    upper_candidates: list[tuple[float, TemperatureMarket]] = []
    for market in markets:
        lo, hi, _ = comparable_rule_bounds(market, event_unit)
        if hi is not None and hi < float(predicted):
            lower_candidates.append((hi, market))
        if lo is not None and lo > float(predicted):
            upper_candidates.append((lo, market))
    if not lower_candidates or not upper_candidates:
        return None
    lower = max(lower_candidates, key=lambda item: item[0])[1]
    upper = min(upper_candidates, key=lambda item: item[0])[1]
    return lower, upper


def model_awc_best_non_adjacent_no_candidate(
    config: dict[str, Any],
    markets: list[TemperatureMarket],
    adjacent_markets: tuple[TemperatureMarket, TemperatureMarket],
) -> Optional[dict[str, Any]]:
    """Return the cheapest NO candidate outside the two adjacent prediction markets."""
    adjacent_ids = {market.market_id for market in adjacent_markets}
    candidates: list[dict[str, Any]] = []
    for market in markets:
        if market.market_id in adjacent_ids:
            continue
        price = best_buy_price(config, market, "NO")
        if price is None or price <= 0:
            continue
        candidates.append({"market": market, "side": "NO", "price": round(float(price), 2)})
    if not candidates:
        return None
    candidates.sort(key=lambda item: (float(item["price"]), str(item["market"].market_id)))
    return candidates[0]


def process_model_awc_prediction(
    config: dict[str, Any],
    event: dict[str, Any],
    city: str,
    station: str,
    predicted_high_f: float,
    latest_row: FeatureMetarRow,
    latest_local: datetime,
) -> Optional[PaperTrade]:
    """Choose and persist/submit the model-implied buy.

    A prediction that maps cleanly to one market first tries to buy the
    configured notional directly. If current depth is insufficient, the hourly
    manager tracks the same notional target from websocket books. The adjacent
    two-market fallback uses share targets because it must balance both legs.
    """
    if str(event.get("_parsed_kind") or "") != "Highest":
        return None
    event_date = str(event.get("_parsed_event_date") or "")
    event_url = poly_url_from_event(event)
    if not event_date or latest_local.date().isoformat() != event_date:
        return None

    trades = read_trades(config["outputs"]["trades_csv"])
    strategy_name = str(config["trading"]["strategy_name"])
    local_hour = int(latest_local.hour)
    local_minute = int(latest_local.minute)
    duplicate_minute = (
        local_minute
        if len(model_awc_station_observation_minutes(config, station)) > 1
        else None
    )
    duplicate_window = model_awc_trade_exists_for_window(
        trades, strategy_name, city, station, event_date, local_hour, duplicate_minute
    )
    active_event_positions = model_awc_active_event_positions(trades, strategy_name, city, event_date)

    markets = [m for m in markets_for_event(config, event) if not m.closed and m.kind == "Highest"]
    if not markets:
        LOGGER.info("model awc skip no highest markets city=%s station=%s event_date=%s", city, station, event_date)
        return None
    event_unit = event_market_unit(markets)
    live_trader = get_live_trader() if live_trading_enabled(config) and station.upper() in model_awc_live_stations(config) else None

    def submit_model_awc_trade(
        market: TemperatureMarket,
        side: str,
        price: float,
        reason: str,
        amount_usd: Optional[float] = None,
        shares: Optional[float] = None,
    ) -> Optional[PaperTrade]:
        cycle_id = (
            f"{datetime.now().strftime('%Y%m%dT%H%M%S')}:model_awc_high:{city}:{station}:{event_date}:"
            f"hour_{local_hour:02d}"
        )
        if duplicate_minute is not None:
            cycle_id += f"_minute_{local_minute:02d}"
        trade = (
            live_trader.submit_buy_trade(config, cycle_id, market, "", station, side, price, predicted_high_f, None, reason, amount_usd=amount_usd, shares=shares)
            if live_trader
            else make_trade(config, cycle_id, market, "", station, side, price, predicted_high_f, None, reason, notional_usdc=amount_usd, shares_override=shares)
        )
        if not trade:
            return None
        trade.forecast_source = f"model_awc_high_lightgbm:{MODEL_AWC_MODEL_PATH or config['trading'].get('model_awc_model_path', '')}"
        trade.forecast_observed_at = latest_row.valid_utc.astimezone(timezone.utc).isoformat(timespec="seconds")
        trade.forecast_first_valid_time_local = latest_local.isoformat(timespec="seconds")
        trade.forecast_last_valid_time_local = latest_local.isoformat(timespec="seconds")
        if not live_trader:
            trade.execution_mode = "DRY_RUN"
            notify_trade(config, trade, "BUY", "FILLED", reason)
        trades.append(trade)
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
        LOGGER.info(
            "model awc buy city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r side=%s market=%s price=%s amount_usd=%s shares=%s live=%s question=%r",
            city,
            station,
            event_date,
            local_hour,
            predicted_high_f,
            side,
            market.market_id,
            price,
            amount_usd,
            trade.shares,
            bool(live_trader),
            market.market_question,
        )
        return trade

    def start_single_notional_manager(
        market: TemperatureMarket,
        side: str,
        reason: str,
    ) -> Optional[PaperTrade]:
        if not live_trader:
            return None
        target_notional = float(config["trading"].get("buy_notional_usdc", 10.0))
        partial = partial_buy_fillable_now(config, market, side, target_notional)
        partial_trade = None
        remaining_notional = target_notional
        if partial is not None:
            partial_trade = submit_model_awc_trade(
                market,
                side,
                float(partial["price"]),
                f"model_awc_high_single_partial_before_manager_{market.market_id}_{reason}_local_hour_{local_hour:02d}",
                amount_usd=float(partial["amount_usd"]),
                shares=float(partial["shares"]),
            )
            remaining_notional = max(
                0.0,
                target_notional - float(partial["amount_usd"]),
            )
            LOGGER.info(
                "model awc single interval bought immediate partial before "
                "manager market=%s price=%.4f shares=%s amount_usd=%.2f "
                "remaining_notional_usd=%.2f reason=%s",
                market.market_id,
                float(partial["price"]),
                float(partial["shares"]),
                float(partial["amount_usd"]),
                remaining_notional,
                reason,
            )
        if remaining_notional < 0.01:
            return partial_trade
        batch_id = live_trader.start_model_awc_hourly_batch(
            city,
            station,
            event_date,
            local_hour,
            (market,),
            (side,),
            0.0,
            predicted_high_f,
            "single",
            target_notional_usd=remaining_notional,
            local_minute=local_minute,
        )
        LOGGER.info(
            "model awc single interval delegated to notional websocket manager "
            "batch=%s market=%s target_notional_usd=%.2f reason=%s",
            batch_id,
            market.market_id,
            remaining_notional,
            reason,
        )
        return partial_trade

    predicted_yes_market = model_awc_predicted_yes_market(markets, predicted_high_f, event_unit)
    if predicted_yes_market is not None:
        if duplicate_window:
            LOGGER.info("model awc skip duplicate city=%s station=%s event_date=%s local_hour=%s", city, station, event_date, local_hour)
            return None
        candidates = []
        for market in markets:
            candidate = model_awc_market_candidate(config, market, predicted_high_f, event_unit, predicted_yes_market.market_id)
            if candidate:
                candidates.append(candidate)
        if not candidates:
            partial_trade = start_single_notional_manager(
                predicted_yes_market, "YES", "insufficient_current_depth"
            )
            LOGGER.info("model awc skip no priced candidates city=%s station=%s predicted_high_f=%r", city, station, predicted_high_f)
            return partial_trade
        candidates.sort(key=lambda item: (float(item["price"]), str(item["market"].market_id), str(item["side"])))
        selected = candidates[0]
        market = selected["market"]
        side = str(selected["side"])
        price = round(float(selected["price"]), 2)
        if price > configured_max_buy_price(config):
            return start_single_notional_manager(
                market, side, "current_price_above_configured_max"
            )
        reason = f"model_awc_high_predicted_{predicted_high_f:.2f}_market_{predicted_yes_market.market_id}_local_hour_{local_hour:02d}"
        return submit_model_awc_trade(market, side, price, reason)

    snapped = model_awc_boundary_snap_market(config, markets, predicted_high_f, event_unit)
    if snapped is not None:
        if duplicate_window:
            LOGGER.info("model awc skip duplicate city=%s station=%s event_date=%s local_hour=%s", city, station, event_date, local_hour)
            return None
        snap_market, snap_distance = snapped
        yes_price = best_buy_price(config, snap_market, "YES")
        if yes_price is None or yes_price <= 0:
            partial_trade = start_single_notional_manager(
                snap_market, "YES", "snap_insufficient_current_depth"
            )
            LOGGER.info(
                "model awc skip snapped interval missing yes price city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r market=%s distance=%r%s",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                snap_market.market_id,
                snap_distance,
                event_unit,
            )
            return partial_trade
        price = round(float(yes_price), 2)
        if price > configured_max_buy_price(config):
            return start_single_notional_manager(
                snap_market, "YES", "snap_current_price_above_configured_max"
            )
        tolerance = model_awc_interval_snap_tolerance(config, event_unit)
        reason = (
            f"model_awc_high_predicted_{predicted_high_f:.2f}_boundary_snap_yes_{snap_market.market_id}"
            f"_distance_{snap_distance:.3f}{event_unit}_tolerance_{tolerance:.5f}{event_unit}_local_hour_{local_hour:02d}"
        )
        LOGGER.info(
            "model awc boundary snap city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r market=%s distance=%r%s tolerance=%r%s yes_price=%.4f",
            city,
            station,
            event_date,
            local_hour,
            predicted_high_f,
            snap_market.market_id,
            snap_distance,
            event_unit,
            tolerance,
            event_unit,
            price,
        )
        return submit_model_awc_trade(snap_market, "YES", price, reason)

    matched_prediction_markets = model_awc_prediction_matches(markets, predicted_high_f, event_unit)
    adjacent_markets = model_awc_adjacent_prediction_markets(markets, predicted_high_f, event_unit)
    if adjacent_markets is None:
        LOGGER.info(
            "model awc skip invalid prediction without adjacent market pair city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r event_unit=%s",
            city,
            station,
            event_date,
            local_hour,
            predicted_high_f,
            event_unit,
        )
        return None

    adjacent_shares = float(config["trading"].get("model_awc_adjacent_yes_shares", 10))
    adjacent_prices: list[tuple[TemperatureMarket, float]] = []
    for market in adjacent_markets:
        yes_price = best_buy_price(
            config, market, "YES", target_shares=adjacent_shares
        )
        if yes_price is None or yes_price <= 0:
            LOGGER.info(
                "model awc skip adjacent missing yes price city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r market=%s",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                market.market_id,
            )
            return None
        adjacent_prices.append((market, round(float(yes_price), 2)))
    total_yes_price = sum(price for _market, price in adjacent_prices)
    max_total_price = float(config["trading"].get("model_awc_adjacent_yes_max_total_price", 0.9))
    if duplicate_window:
        LOGGER.info("model awc skip duplicate city=%s station=%s event_date=%s local_hour=%s", city, station, event_date, local_hour)
        return None
    non_adjacent_no = (
        model_awc_best_non_adjacent_no_candidate(config, markets, adjacent_markets)
        if not matched_prediction_markets
        else None
    )
    if non_adjacent_no is not None and float(non_adjacent_no["price"]) < total_yes_price:
        no_market = non_adjacent_no["market"]
        no_price = round(float(non_adjacent_no["price"]), 2)
        max_buy_price = configured_max_buy_price(config)
        if no_price > max_buy_price:
            LOGGER.info(
                "model awc skip cheaper non-adjacent no above max city=%s station=%s "
                "event_date=%s local_hour=%s predicted_high_f=%r no_market=%s "
                "no_price=%.4f no_max=%.4f adjacent_yes_total=%.4f",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                no_market.market_id,
                no_price,
                max_buy_price,
                total_yes_price,
            )
            return None
        reason = (
            f"model_awc_high_predicted_{predicted_high_f:.2f}_non_adjacent_no_{no_market.market_id}"
            f"_cheaper_than_adjacent_yes_{total_yes_price:.2f}_local_hour_{local_hour:02d}"
        )
        if live_trader:
            batch_id = live_trader.start_model_awc_hourly_batch(
                city, station, event_date, local_hour, (no_market,), ("NO",),
                adjacent_shares, predicted_high_f, "single",
                local_minute=local_minute,
            )
            LOGGER.info(
                "model awc non-adjacent no delegated to websocket manager "
                "batch=%s market=%s target_shares=%s no_price=%.4f adjacent_yes_total=%.4f",
                batch_id, no_market.market_id, adjacent_shares, no_price, total_yes_price,
            )
            return None
        return submit_model_awc_trade(
            no_market, "NO", no_price, reason,
            amount_usd=round(adjacent_shares * no_price, 2),
            shares=adjacent_shares,
        )
    if total_yes_price > max_total_price:
        LOGGER.info(
            "model awc skip adjacent yes total above threshold city=%s station=%s "
            "event_date=%s local_hour=%s predicted_high_f=%r total_yes_price=%.4f "
            "max=%.4f non_adjacent_no_price=%s markets=%s",
            city,
            station,
            event_date,
            local_hour,
            predicted_high_f,
            total_yes_price,
            max_total_price,
            None if non_adjacent_no is None else non_adjacent_no["price"],
            [market.market_id for market, _price in adjacent_prices],
        )
        return None
    held_adjacent_yes = model_awc_adjacent_yes_position_summary(active_event_positions, adjacent_markets)
    if held_adjacent_yes:
        held_market_ids = set(held_adjacent_yes)
        if len(held_market_ids) >= len(adjacent_markets):
            LOGGER.info(
                "model awc skip adjacent hedge already has both yes sides city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r markets=%s",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                sorted(held_market_ids),
            )
            return None
        missing = [(market, price) for market, price in adjacent_prices if market.market_id not in held_market_ids]
        if len(missing) != 1:
            LOGGER.info(
                "model awc skip adjacent hedge ambiguous held yes city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r held=%s missing=%s",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                sorted(held_market_ids),
                [market.market_id for market, _price in missing],
            )
            return None
        held_market_id = next(iter(held_market_ids))
        held_summary = held_adjacent_yes[held_market_id]
        held_cost_price = round(float(held_summary.get("avg_price", 0.0)), 4)
        hedge_market, hedge_price = missing[0]
        hedge_total_price = held_cost_price + float(hedge_price)
        held_shares = float(held_summary.get("shares", 0.0))
        hedge_shares = adjacent_shares
        if held_shares <= 0 or hedge_shares <= 0:
            LOGGER.info(
                "model awc skip adjacent hedge invalid shares city=%s station=%s event_date=%s local_hour=%s held_market=%s held_shares=%s hedge_shares=%s",
                city,
                station,
                event_date,
                local_hour,
                held_market_id,
                held_shares,
                hedge_shares,
            )
            return None
        if open_trade_exists(trades, strategy_name, hedge_market.market_id, "YES"):
            LOGGER.info(
                "model awc skip adjacent hedge duplicate target city=%s station=%s event_date=%s local_hour=%s hedge_market=%s",
                city,
                station,
                event_date,
                local_hour,
                hedge_market.market_id,
            )
            return None
        amount_usd = round(hedge_shares * round(float(hedge_price), 2), 2)
        reason = (
            f"model_awc_high_predicted_{predicted_high_f:.2f}_adjacent_yes_hedge_{held_market_id}_with_{hedge_market.market_id}"
            f"_held_cost_{held_cost_price:.2f}_hedge_price_{hedge_price:.2f}_total_yes_{hedge_total_price:.2f}"
            f"_held_shares_{held_shares:g}_hedge_shares_{hedge_shares:g}_local_hour_{local_hour:02d}"
        )
        if live_trader:
            batch_id = live_trader.start_model_awc_hourly_batch(
                city,
                station,
                event_date,
                local_hour,
                (hedge_market,),
                ("YES",),
                hedge_shares,
                predicted_high_f,
                "single",
                local_minute=local_minute,
            )
            LOGGER.info(
                "model awc adjacent hedge delegated to websocket manager batch=%s "
                "held_market=%s hedge_market=%s held_shares=%s target_shares=%s",
                batch_id,
                held_market_id,
                hedge_market.market_id,
                held_shares,
                hedge_shares,
            )
            return None
        trade = submit_model_awc_trade(hedge_market, "YES", hedge_price, reason, amount_usd=amount_usd, shares=hedge_shares)
        if trade:
            LOGGER.info(
                "model awc adjacent hedge buy completed city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r held_market=%s hedge_market=%s held_cost=%.4f hedge_price=%.4f total=%.4f held_shares=%s hedge_shares=%s trade=%s",
                city,
                station,
                event_date,
                local_hour,
                predicted_high_f,
                held_market_id,
                hedge_market.market_id,
                held_cost_price,
                hedge_price,
                hedge_total_price,
                held_shares,
                hedge_shares,
                trade.trade_id,
            )
        return trade
    if live_trader:
        batch_id = live_trader.start_model_awc_hourly_batch(
            city,
            station,
            event_date,
            local_hour,
            tuple(adjacent_markets),
            ("YES", "YES"),
            adjacent_shares,
            predicted_high_f,
            "adjacent",
            local_minute=local_minute,
        )
        LOGGER.info(
            "model awc adjacent delegated to websocket manager batch=%s "
            "markets=%s target_shares_each=%s total_yes_price=%.4f",
            batch_id,
            [market.market_id for market in adjacent_markets],
            adjacent_shares,
            total_yes_price,
        )
        return None

    created: list[PaperTrade] = []
    adjacent_market_ids = "_".join(market.market_id for market, _price in adjacent_prices)
    for market, price in adjacent_prices:
        amount_usd = round(adjacent_shares * round(price, 2), 2)
        reason = (
            f"model_awc_high_predicted_{predicted_high_f:.2f}_adjacent_yes_{adjacent_market_ids}"
            f"_shares_{adjacent_shares:g}_total_yes_{total_yes_price:.2f}_local_hour_{local_hour:02d}"
        )
        trade = submit_model_awc_trade(market, "YES", price, reason, amount_usd=amount_usd, shares=adjacent_shares)
        if trade:
            created.append(trade)
    if len(created) != len(adjacent_prices):
        LOGGER.warning(
            "model awc adjacent buy partially completed city=%s station=%s event_date=%s local_hour=%s created=%s expected=%s",
            city,
            station,
            event_date,
            local_hour,
            len(created),
            len(adjacent_prices),
        )
    LOGGER.info(
        "model awc adjacent buy completed city=%s station=%s event_date=%s local_hour=%s predicted_high_f=%r total_yes_price=%.4f shares_each=%s trades=%s",
        city,
        station,
        event_date,
        local_hour,
        predicted_high_f,
        total_yes_price,
        adjacent_shares,
        [trade.trade_id for trade in created],
    )
    return created[0] if created else None


def parse_weather_record_timestamp(value: Any) -> Optional[datetime]:
    """Parse Jack AI weather record timestamps into UTC datetimes."""
    if value is None:
        return None
    try:
        raw = float(value)
    except (TypeError, ValueError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    if raw > 10_000_000_000:
        raw = raw / 1000.0
    return datetime.fromtimestamp(raw, timezone.utc)


def normalize_weather_record_payload(payload: Any) -> list[dict[str, Any]]:
    """Normalize one Jack AI weatherrecord websocket message to Fahrenheit."""
    rows = payload.get("value") if isinstance(payload, dict) and "value" in payload else payload
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        station = str(row.get("StationCode") or row.get("stationCode") or row.get("station") or "").strip().upper()
        obs_dt = parse_weather_record_timestamp(row.get("TimeStamp") or row.get("timestamp") or row.get("time"))
        temp_raw = row.get("Temperature") if row.get("Temperature") is not None else row.get("temperature")
        if not station or obs_dt is None or temp_raw is None:
            continue
        unit_raw = (
            row.get("TemperatureUnit")
            or row.get("temperatureUnit")
            or row.get("TempUnit")
            or row.get("tempUnit")
            or row.get("Unit")
            or row.get("unit")
            or ""
        )
        temp_text = str(temp_raw).strip()
        match = re.search(r"-?\d+(?:\.\d+)?", temp_text)
        if not match:
            continue
        temp_value = float(match.group(0))
        unit_text = f"{unit_raw} {temp_text}".upper()
        source_unit = "C" if "CELSIUS" in unit_text or re.search(r"°?C\b", unit_text) else "F"
        temp_f = convert_temperature(temp_value, source_unit, "F")
        if temp_f is None:
            continue
        normalized.append({
            "station": station,
            "obs_dt": obs_dt,
            "temp_f": float(temp_f),
            "source_unit": source_unit,
            "raw": row,
        })
    return normalized


def enqueue_weather_record_update(rows: list[dict[str, Any]]) -> None:
    """Queue a websocket update without letting a slow consumer block the feed."""
    if not rows:
        return
    try:
        WEATHER_RECORD_UPDATES.put_nowait(rows)
    except queue.Full:
        try:
            WEATHER_RECORD_UPDATES.get_nowait()
        except queue.Empty:
            pass
        WEATHER_RECORD_UPDATES.put_nowait(rows)


def weather_record_websocket_listener(config: dict[str, Any]) -> None:
    """Receive Jack AI weather records continuously and reconnect after failures."""
    url = str(
        config.get("api", {}).get("weather_record_websocket_url")
        or "wss://jackaisolutions.us/ws/weatherrecord"
    )
    reconnect_seconds = max(1.0, float(config["trading"].get("weather_record_websocket_reconnect_seconds", 5)))
    heartbeat_seconds = max(5.0, float(config["trading"].get("weather_record_websocket_heartbeat_seconds", 20)))
    read_timeout_seconds = max(1.0, float(config["trading"].get("weather_record_websocket_read_timeout_seconds", 5)))
    while True:
        ws = None
        try:
            LOGGER.info("weatherrecord websocket connecting url=%s", url)
            ws = websocket.create_connection(url, timeout=30)
            ws.settimeout(read_timeout_seconds)
            next_ping_at = time.monotonic() + heartbeat_seconds
            LOGGER.info(
                "weatherrecord websocket connected url=%s heartbeat_seconds=%s read_timeout_seconds=%s",
                url,
                heartbeat_seconds,
                read_timeout_seconds,
            )
            while True:
                message = None
                try:
                    message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    pass
                if message is not None:
                    if message == "":
                        raise ConnectionError("weatherrecord websocket closed")
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")
                    rows = normalize_weather_record_payload(json.loads(message))
                    enqueue_weather_record_update(rows)
                    LOGGER.info("weatherrecord websocket received rows=%s", len(rows))
                now_mono = time.monotonic()
                if now_mono >= next_ping_at:
                    ws.ping()
                    LOGGER.debug("weatherrecord websocket ping sent")
                    next_ping_at = now_mono + heartbeat_seconds
        except Exception:
            LOGGER.exception("weatherrecord websocket disconnected; reconnecting in %ss", reconnect_seconds)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
        time.sleep(reconnect_seconds)


def append_weather_record_history(config: dict[str, Any], rows: list[dict[str, Any]], reason: str) -> None:
    """Raw weatherrecord audit writes are disabled to keep disk usage low."""
    return


def weather_record_observed_extremes(
    config: dict[str, Any],
    station: str,
    city: str,
    event_date: str,
    unit: str,
    rows: list[dict[str, Any]],
) -> tuple[Optional[float], Optional[float], Optional[datetime], list[tuple[datetime, float]]]:
    """Update in-memory event-day extremes from weatherrecord websocket rows for one station."""
    station_key = station.strip().upper()
    city_tz = city_timezone(config, city)
    points_by_time = WEATHER_RECORD_POINTS_BY_STATION_EVENT.setdefault((station_key, event_date), {})
    for row in rows:
        if str(row.get("station") or "").upper() != station_key:
            continue
        obs_dt = row.get("obs_dt")
        if not isinstance(obs_dt, datetime):
            continue
        local_dt = obs_dt.astimezone(city_tz)
        if local_dt.date().isoformat() != event_date:
            continue
        if row.get("temp_f") is not None:
            temp_f = float(row["temp_f"])
        elif row.get("temp_c") is not None:
            # Compatibility with records normalized by older API-based versions.
            temp_f = convert_temperature(float(row["temp_c"]), "C", "F")
        else:
            continue
        temp = convert_temperature(temp_f, "F", unit) if unit.upper() != "F" else temp_f
        if temp is None:
            continue
        points_by_time[obs_dt.astimezone(timezone.utc).isoformat()] = (local_dt, round(float(temp)))
    points = sorted(points_by_time.values(), key=lambda item: item[0])
    high, low, _, _, _, _ = summarize_points(points)
    latest_dt = max((dt for dt, _ in points), default=None)
    return high, low, latest_dt, points


def infer_report_schedule_from_times(times: list[datetime], hours: int = 24) -> tuple[Optional[datetime], Optional[int], int, list[int], Optional[datetime]]:
    """Infer regular METAR schedule minutes from recent observation times."""
    clean_times = sorted({dt.astimezone(timezone.utc).replace(second=0, microsecond=0) for dt in times})
    if not clean_times:
        return None, None, 0, [], None

    report_count = len(clean_times)
    reports_per_hour = max(1, int(report_count / max(1, hours)))
    reports_per_hour = min(6, reports_per_hour)
    minute_counts = Counter(dt.minute for dt in clean_times)
    scheduled_minutes = sorted(
        minute
        for minute, _ in sorted(minute_counts.items(), key=lambda item: (-item[1], item[0]))[:reports_per_hour]
    )
    if not scheduled_minutes:
        scheduled_minutes = [clean_times[-1].minute]

    latest = clean_times[-1]
    interval_seconds = max(60, int(3600 / max(1, len(scheduled_minutes))))
    expected_next: Optional[datetime] = None
    for hour_offset in range(0, 3):
        base_hour = latest.replace(minute=0) + timedelta(hours=hour_offset)
        for minute in scheduled_minutes:
            candidate = base_hour + timedelta(minutes=minute)
            if candidate > latest:
                expected_next = candidate
                break
        if expected_next is not None:
            break
    if expected_next is None:
        expected_next = latest + timedelta(seconds=interval_seconds)
    return latest, interval_seconds, report_count, scheduled_minutes, expected_next


def aviation_report_timing(station: str, hours: int = 24, config: Optional[dict[str, Any]] = None) -> tuple[Optional[datetime], Optional[int], int, list[int], Optional[datetime]]:
    """Compute report cadence metadata from recent AviationWeather obsTime values.
    
    Args:
        station (str): Weather station identifier, usually a four-character ICAO code.
        hours (int): METAR lookback window in hours.
    
    Returns:
        tuple[Optional[datetime], Optional[int], int, list[int], Optional[datetime]]: Latest UTC observation time, inferred interval, report count, scheduled UTC minutes, and next expected observation time.
    
    Side effects:
        Calls the AviationWeather METAR API.
    """
    rows = aviation_metar_observations(station, hours)
    if config is not None:
        append_aviation_metar_history(config, station, rows, "report_timing")
    times = sorted({
        parsed
        for row in rows
        if (parsed := parse_aviation_obs_time(row.get("obsTime"))) is not None
    })
    return infer_report_schedule_from_times(times, hours)


def parse_metar_temperature_c(raw_ob: str) -> Optional[float]:
    """Parse Celsius temperature from a raw METAR string.
    
    Args:
        raw_ob (str): Raw METAR observation text.
    
    Returns:
        Optional[float]: Temperature in Celsius, or None when absent.
    """
    exact = re.search(r"\bT([01])(\d{3})([01])(\d{3})\b", raw_ob)
    if exact:
        sign = -1 if exact.group(1) == "1" else 1
        return sign * (int(exact.group(2)) / 10.0)
    for token in raw_ob.split():
        match = re.fullmatch(r"(M?\d{2})/(?:M?\d{2}|//)", token)
        if not match:
            continue
        text = match.group(1)
        return -float(text[1:]) if text.startswith("M") else float(text)
    return None


def parse_tgftp_metar(text: str) -> Optional[dict[str, Any]]:
    """Parse a NOAA TGFTP station METAR TXT response.
    
    Args:
        text (str): Two-line TGFTP station response.
    
    Returns:
        Optional[dict[str, Any]]: Parsed observation fields, or None when unusable.
    """
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    try:
        obs_dt = datetime.strptime(lines[0], "%Y/%m/%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    raw_ob = lines[1]
    temp_c = parse_metar_temperature_c(raw_ob)
    if temp_c is None:
        return None
    return {"obs_dt": obs_dt, "raw_ob": raw_ob, "temp_c": temp_c}


def tgftp_metar_observation(station: str, timeout_seconds: float = 15) -> Optional[dict[str, Any]]:
    """Fetch and parse the latest NOAA TGFTP station METAR.
    
    Args:
        station (str): Weather station identifier, usually a four-character ICAO code.
    
    Returns:
        Optional[dict[str, Any]]: Parsed latest station observation.
    
    Side effects:
        Calls the NOAA TGFTP endpoint.
    """
    url = f"https://tgftp.nws.noaa.gov/data/observations/metar/stations/{station.upper()}.TXT?nocache={int(time.time() * 1000)}"
    headers = dict(HEADERS)
    headers.update({"Cache-Control": "no-cache, no-store, max-age=0", "Pragma": "no-cache"})
    r = requests.get(url, headers=headers, timeout=max(0.5, float(timeout_seconds)))
    r.raise_for_status()
    return parse_tgftp_metar(r.text)


def tgftp_observation_as_aviation_row(obs: dict[str, Any]) -> dict[str, Any]:
    """Convert a parsed TGFTP observation to the AWC row shape used by the model."""
    obs_dt = obs.get("obs_dt")
    return {
        "rawOb": str(obs.get("raw_ob") or ""),
        "obsTime": obs_dt.isoformat() if isinstance(obs_dt, datetime) else obs_dt,
        "temp": obs.get("temp_c"),
        "_source": "tgftp",
    }


def merge_tgftp_into_aviation_rows(
    rows: list[dict[str, Any]],
    obs: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return AWC history with the newest TGFTP report replacing the same observation."""
    obs_dt = obs.get("obs_dt")
    raw_ob = str(obs.get("raw_ob") or "").strip()
    merged = []
    for row in rows:
        row_dt = parse_aviation_obs_time(row.get("obsTime") or row.get("reportTime") or row.get("receiptTime"))
        row_raw = str(row.get("rawOb") or row.get("raw") or row.get("metar") or "").strip()
        if (isinstance(obs_dt, datetime) and row_dt == obs_dt) or (raw_ob and row_raw == raw_ob):
            continue
        merged.append(row)
    merged.append(tgftp_observation_as_aviation_row(obs))
    return merged


def tgftp_observation_changed(previous: Optional[dict[str, Any]], current: Optional[dict[str, Any]]) -> bool:
    """Return True when TGFTP has a new observation timestamp or reported temperature."""
    if not current:
        return False
    if not previous:
        return True
    previous_dt = previous.get("obs_dt")
    current_dt = current.get("obs_dt")
    if isinstance(previous_dt, datetime) and isinstance(current_dt, datetime):
        if previous_dt.astimezone(timezone.utc) != current_dt.astimezone(timezone.utc):
            return True
    elif previous_dt != current_dt:
        return True
    try:
        return abs(float(previous.get("temp_c")) - float(current.get("temp_c"))) > 1e-9
    except (TypeError, ValueError):
        return previous.get("temp_c") != current.get("temp_c")


def cached_tgftp_observation(station: str) -> Optional[dict[str, Any]]:
    """Return the latest in-memory TGFTP baseline for a station."""
    return TGFTP_OBS_BY_STATION.get(station.upper())


def update_cached_tgftp_observation(station: str, obs: dict[str, Any]) -> None:
    """Store the latest TGFTP observation for a station in memory."""
    TGFTP_OBS_BY_STATION[station.upper()] = obs


def initialize_tgftp_observation_cache(config: dict[str, Any]) -> None:
    """Fetch one TGFTP baseline for every station in currently tradable configured cities."""
    stations: set[str] = set()
    for target in resolve_event_target_dates(config):
        for event in discover_temperature_events(config, target):
            event_url = poly_url_from_event(event)
            station = station_for_event(config, event.get("_parsed_city", ""), event_url)
            if station:
                stations.add(station.upper())
    for station in sorted(stations):
        try:
            obs = tgftp_metar_observation(station)
        except Exception:
            LOGGER.exception("tgftp baseline fetch failed station=%s", station)
            continue
        if not obs:
            LOGGER.info("tgftp baseline unavailable station=%s", station)
            continue
        update_cached_tgftp_observation(station, obs)
        LOGGER.info(
            "tgftp baseline saved station=%s obs_utc=%s temp_c=%s raw=%r",
            station,
            obs["obs_dt"].astimezone(timezone.utc).isoformat(),
            obs["temp_c"],
            obs["raw_ob"],
        )


def update_station_report_timing(
    city: str,
    station: str,
    latest_obs_dt: datetime,
    interval_seconds: Optional[int],
    report_count: int,
    scheduled_minutes: Optional[list[int]] = None,
    expected_next_obs_dt: Optional[datetime] = None,
) -> dict[str, Any]:
    """Update in-memory METAR observation cadence for one city/station.
    
    Args:
        city (str): Polymarket city name.
        station (str): ICAO station id.
        latest_obs_dt (datetime): Latest METAR obsTime in UTC.
        interval_seconds (Optional[int]): Estimated observation cadence.
        report_count (int): Number of observations used to infer cadence.
    
    Returns:
        dict[str, Any]: Updated timing state.
    """
    latest_utc = latest_obs_dt.astimezone(timezone.utc)
    effective_interval = max(60, int(interval_seconds or 3600))
    expected_next = (expected_next_obs_dt or (latest_utc + timedelta(seconds=effective_interval))).astimezone(timezone.utc)
    state = {
        "city": city,
        "station": station,
        "latest_obs_utc": latest_utc.isoformat(),
        "interval_seconds": effective_interval,
        "scheduled_minutes_utc": scheduled_minutes or [],
        "expected_next_obs_utc": expected_next.isoformat(),
        "report_count": report_count,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    STATION_REPORT_TIMING[(city, station)] = state
    return state


def in_station_report_window(config: dict[str, Any], city: str, station: str, now_utc: Optional[datetime] = None) -> tuple[bool, dict[str, Any]]:
    """Return whether current time is inside the expected next METAR report window.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including price momentum settings.
        city (str): Polymarket city name.
        station (str): ICAO station id.
        now_utc (Optional[datetime]): Current UTC time override for tests.
    
    Returns:
        tuple[bool, dict[str, Any]]: Whether inside the window and the timing state.
    """
    state = STATION_REPORT_TIMING.get((city, station), {})
    expected_raw = state.get("expected_next_obs_utc")
    if not expected_raw:
        return False, state
    try:
        expected = datetime.fromisoformat(str(expected_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return False, state
    now_value = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window_seconds = max(1, int(config.get("price_momentum", {}).get("report_window_seconds", 210)))
    return expected <= now_value < expected + timedelta(seconds=window_seconds), state


def in_station_weather_record_window(config: dict[str, Any], city: str, station: str, now_utc: Optional[datetime] = None) -> tuple[bool, dict[str, Any]]:
    """Keep a station's weatherrecord window active through five post-observation minutes."""
    state = STATION_REPORT_TIMING.get((city, station), {})
    now_value = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window_key = (city, station)
    active = WEATHER_RECORD_ACTIVE_WINDOWS.get(window_key)
    if active:
        active_until = active.get("active_until_utc")
        if isinstance(active_until, datetime) and now_value < active_until:
            return True, state
        WEATHER_RECORD_ACTIVE_WINDOWS.pop(window_key, None)
    expected_raw = state.get("expected_next_obs_utc")
    if not expected_raw:
        return False, state
    try:
        expected = datetime.fromisoformat(str(expected_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return False, state
    pre_seconds = max(0, int(config.get("trading", {}).get("weather_record_pre_window_seconds", 120)))
    receive_seconds = max(300, int(config.get("trading", {}).get("weather_record_receive_window_seconds", 300)))
    active_until = expected + timedelta(seconds=receive_seconds)
    inside = expected - timedelta(seconds=pre_seconds) <= now_value < active_until
    if inside:
        WEATHER_RECORD_ACTIVE_WINDOWS[window_key] = {
            "expected_obs_utc": expected,
            "active_until_utc": active_until,
        }
        LOGGER.info(
            "weatherrecord station window active city=%s station=%s expected_obs_utc=%s active_until_utc=%s",
            city,
            station,
            expected.isoformat(),
            active_until.isoformat(),
        )
    return inside, state


def momentum_target_price(base_price: float, fraction: float) -> float:
    """Return the price needed after moving a fraction of the remaining path to 1.0."""
    base = min(0.999999, max(0.0, float(base_price)))
    move_fraction = min(1.0, max(0.0, float(fraction)))
    return base + move_fraction * (1.0 - base)


def directional_price_change_fraction(previous_price: float, price: float) -> float:
    """Return directional price movement, using remaining upside space for upward moves."""
    previous = min(0.999999, max(0.0, float(previous_price)))
    current = min(1.0, max(0.0, float(price)))
    if current >= previous:
        return (current - previous) / max(1e-9, 1.0 - previous)
    if previous <= 0:
        return 0.0
    return (previous - current) / previous


def websocket_momentum_signal_fields(config: dict[str, Any]) -> set[str]:
    """Return websocket price fields allowed to drive observation-window momentum buys."""
    fields = config.get("price_momentum", {}).get("websocket_signal_fields")
    if not fields:
        return {"last_price", "best_bid"}
    return {str(field).strip() for field in fields if str(field).strip()}


def price_momentum_window_key(asset_id: str, price_field: str) -> str:
    """Keep independent momentum baselines for trade prices and top-of-book prices."""
    return f"{asset_id}:{price_field or 'price'}"


def taker_fee_usdc(shares: float, price: float, fee_rate: float, fee_enabled: bool) -> float:
    """Compute the Polymarket taker fee for a simulated trade.
    
    Args:
        shares (float): Number of shares in the simulated trade.
        price (float): Trade or market price in USDC probability units.
        fee_rate (float): Configured Polymarket taker fee rate.
        fee_enabled (bool): Whether fee calculation should be applied.
    
    Returns:
        float: Rounded fee amount in USDC.
    """
    return 0.0 if not fee_enabled else round(fee_rate * shares * price * (1.0 - price), 8)


def live_trading_enabled(config: dict[str, Any]) -> bool:
    """Return whether the current config should submit real Polymarket orders.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        bool: True when live order submission is enabled.
    """
    return bool(config.get("trading", {}).get("live_trading_enabled", False))


def get_live_trader() -> Optional[Any]:
    """Return the process-wide live trading manager, if one has been started.
    
    Args:
        None.
    
    Returns:
        Optional[Any]: Active LiveTradingManager instance or None in paper mode.
    """
    return LIVE_TRADER


def _account_env(config: dict[str, Any], key: str) -> str:
    """Read one account secret or address from the environment using the configured variable name.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        key (str): Account config key that stores an environment variable name.
    
    Returns:
        str: Trimmed environment value, or an empty string when unset.
    """
    env_name = str(config.get("account", {}).get(key) or "").strip()
    return os.getenv(env_name, "").strip() if env_name else ""


def _result_value(result: Any, name: str, default: Any = None) -> Any:
    """Read a field from an executor OrderResult object or dictionary.
    
    Args:
        result (Any): Executor result object or dictionary.
        name (str): Field name to read.
        default (Any): Fallback value when the field is absent.
    
    Returns:
        Any: Extracted field value or default.
    """
    if isinstance(result, dict):
        return result.get(name, default)
    return getattr(result, name, default)


def _as_float(value: Any, default: float = 0.0) -> float:
    """Convert a loosely typed websocket or executor value into a float.
    
    Args:
        value (Any): Value to convert.
        default (float): Fallback value when conversion fails.
    
    Returns:
        float: Converted floating point value.
    """
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def get_telegram_notifier(config: dict[str, Any]) -> Optional[Any]:
    """Return the process-wide Telegram notifier when enabled and configured.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including notification settings.
    
    Returns:
        Optional[Any]: TelegramNotifier instance, or None when disabled/unavailable.
    """
    global TELEGRAM_NOTIFIER
    if not config.get("notifications", {}).get("telegram_enabled", True):
        return None
    if TELEGRAM_NOTIFIER is not None:
        return TELEGRAM_NOTIFIER
    try:
        from weather_telegram_notifier import TelegramNotifier
    except Exception:
        LOGGER.exception("telegram notifier import failed")
        return None
    TELEGRAM_NOTIFIER = TelegramNotifier()
    return TELEGRAM_NOTIFIER if getattr(TELEGRAM_NOTIFIER, "enabled", False) else None


def _telegram_escape(value: Any) -> str:
    """Escape dynamic text for Telegram legacy Markdown used by the notifier."""
    text = str(value or "")
    for char in ("\\", "`", "*", "_", "["):
        text = text.replace(char, "\\" + char)
    return text


def notify_trade(config: dict[str, Any], trade: PaperTrade, action: str, status: str, reason: str = "", source: str = "") -> None:
    """Send one Telegram notification for a trade lifecycle event.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including notification settings.
        trade (PaperTrade): Trade row that was submitted, filled, or closed.
        action (str): Trade action such as BUY or SELL.
        status (str): Lifecycle status such as SUBMITTED, FILLED, or CLOSED.
        reason (str): Strategy reason or order source to include in the message.
        source (str): Confirmation source, such as poll or user_websocket.
    
    Returns:
        None: This function is executed for its side effects.
    """
    notifications = config.get("notifications", {})
    normalized_status = status.upper()
    if normalized_status == "SUBMITTED" and not notifications.get("telegram_notify_order_submitted", True):
        return
    if normalized_status in {"FILLED", "CLOSED"} and not notifications.get("telegram_notify_order_filled", True):
        return
    notifier = get_telegram_notifier(config)
    if not notifier:
        LOGGER.warning("telegram trade notification skipped notifier unavailable trade=%s action=%s status=%s", trade.trade_id, action, status)
        return

    action = action.upper()
    side = (trade.position_side or "YES").upper()
    mode = trade.execution_mode or ("LIVE" if live_trading_enabled(config) else "PAPER")
    if action == "SELL":
        price = trade.exit_no_price if side == "NO" else trade.exit_yes_price
        amount = trade.exit_proceeds_usdc or (trade.shares * float(price or 0.0))
        order_id = trade.live_sell_order_id
    else:
        price = trade.yes_price
        amount = trade.notional_usdc
        order_id = trade.live_buy_order_id

    lines = [
        f"*{_telegram_escape(mode)} {action} {normalized_status}*",
        f"Side: *{_telegram_escape(side)}*",
        f"Price: ${float(price or 0.0):.4f}",
        f"Amount: ${float(amount or 0.0):.2f}",
        f"Shares: {float(trade.shares or 0.0):.4f}",
        f"Market: {_telegram_escape(trade.city)} {_telegram_escape(trade.kind)} {trade.event_date}",
        f"Question: {_telegram_escape(trade.market_question)}",
        f"Reason: {_telegram_escape(reason or trade.exit_reason or trade.forecast_source)}",
    ]
    if order_id:
        lines.append(f"Order: `{_telegram_escape(order_id)}`")
    if source:
        lines.append(f"Source: {_telegram_escape(source)}")
    if action == "SELL":
        lines.append(f"P&L: ${float(trade.pnl_usdc or 0.0):+.2f}")
    if trade.polymarket_url:
        lines.append(_telegram_escape(trade.polymarket_url))

    try:
        notifier.send("\n".join(lines))
    except Exception:
        LOGGER.exception("telegram trade notification failed trade=%s action=%s status=%s", trade.trade_id, action, status)


class LiveTradingManager:
    """Submit live Polymarket orders and confirm them through user websocket messages.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including account secrets, trading flags, and output paths.
    """

    def __init__(self, config: dict[str, Any]):
        """Create an unstarted live trading manager.
        
        Args:
            config (dict[str, Any]): Active bot configuration, including account secrets, trading flags, and output paths.
        
        Returns:
            None: Constructors return None.
        """
        self.config = config
        self.executor: Any = None
        self.user_feed: Any = None
        self.market_feed: Any = None
        self._pending: dict[str, LivePendingOrder] = {}
        self._hourly_batches: dict[str, ModelAwcHourlyBatch] = {}
        self._managed_order_ids: set[str] = set()
        self._market_wakeup = threading.Event()
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._batch_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Initialize executor.py, start the user websocket, and launch the pending-order poller.
        
        Args:
            None.
        
        Returns:
            None: This function is executed for its side effects.
        """
        if self._running:
            return
        private_key = _account_env(self.config, "private_key_env")
        if not private_key:
            raise RuntimeError("live_trading_enabled=true requires PRIVATE_KEY or configured account.private_key_env")
        safe_address = _account_env(self.config, "safe_address_env")
        funder_address = _account_env(self.config, "funder_address_env")
        signature_type_env = str(self.config.get("account", {}).get("signature_type_env") or "SIGNATURE_TYPE").strip()
        signature_type_raw = os.getenv(signature_type_env, str(self.config.get("account", {}).get("signature_type", 3))).strip() if signature_type_env else str(self.config.get("account", {}).get("signature_type", 3)).strip()
        signature_type = int(signature_type_raw or "3")
        dry_run = bool(self.config.get("trading", {}).get("live_trading_dry_run", False))
        from executor import Executor
        from polymarket_ws import PolymarketMarketFeed, PolymarketUserFeed

        self.executor = Executor(
            private_key=private_key,
            safe_address=safe_address,
            dry_run=dry_run,
            signature_type=signature_type,
            funder_address=funder_address,
        )
        LOGGER.info(
            "live trading executor starting dry_run=%s signature_type=%s safe_configured=%s funder_configured=%s",
            dry_run,
            signature_type,
            bool(safe_address),
            bool(funder_address),
        )
        if not self.executor.initialize():
            raise RuntimeError("executor.initialize() failed; live trading is not available")
        self.user_feed = PolymarketUserFeed(self.executor.get_api_creds(), on_message=self._on_user_order_message)
        self.user_feed.start()
        self.market_feed = PolymarketMarketFeed(
            on_raw_message=self._on_market_message
        )
        self.market_feed.start()
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, name="polymarket-live-orders", daemon=True)
        self._thread.start()
        self._batch_thread = threading.Thread(
            target=self._hourly_batch_loop,
            name="model-awc-hourly-orders",
            daemon=True,
        )
        self._batch_thread.start()
        self._recover_pending_from_disk()
        LOGGER.info("live trading manager started dry_run=%s", dry_run)

    def stop(self) -> None:
        """Stop the user websocket and pending-order poller.
        
        Args:
            None.
        
        Returns:
            None: This function is executed for its side effects.
        """
        self._running = False
        if self.user_feed:
            self.user_feed.stop()
        if self.market_feed:
            self.market_feed.stop()
        if self._thread:
            self._thread.join(timeout=2)
        if self._batch_thread:
            self._batch_thread.join(timeout=2)

    def submit_buy_trade(
        self,
        config: dict[str, Any],
        cycle_id: str,
        market: TemperatureMarket,
        wu_source: str,
        station: str,
        side: str,
        entry_price: float,
        observed_high: Optional[float],
        observed_low: Optional[float],
        reason: str,
        amount_usd: Optional[float] = None,
        shares: Optional[float] = None,
        notify_submitted: bool = True,
    ) -> Optional[PaperTrade]:
        """Post a live buy order and return a BUY_PENDING trade row when accepted by the CLOB.
        
        Args:
            config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
            cycle_id (str): Identifier for the strategy cycle that created the trade.
            market (TemperatureMarket): Market whose YES or NO token should be bought.
            wu_source (str): Weather Underground source URL associated with the event.
            station (str): Aviation station id used for the signal.
            side (str): Position side to buy, expected YES or NO.
            entry_price (float): Limit price selected from the CLOB order book.
            observed_high (Optional[float]): Observed high temperature for the event day, if available.
            observed_low (Optional[float]): Observed low temperature for the event day, if available.
            reason (str): Strategy reason attached to the local trade row.
        
        Returns:
            Optional[PaperTrade]: BUY_PENDING trade row, or None if order submission failed.
        """
        if not self.executor:
            raise RuntimeError("live trading manager is not started")
        token_id = asset_id_for_market_side(market, side)
        if not token_id:
            LOGGER.warning("live buy skipped missing token side=%s market=%s", side, market.market_id)
            return None
        if source_station_city_blocked(market.event_date, market.city):
            LOGGER.warning(
                "live buy skipped source station mismatch block city=%s event_date=%s market=%s side=%s",
                market.city,
                market.event_date,
                market.market_id,
                side,
            )
            return None
        max_price = configured_max_buy_price(config)
        if float(entry_price) > max_price:
            LOGGER.info(
                "live buy skipped price above configured max side=%s market=%s price=%.4f max_buy_price=%.4f",
                side,
                market.market_id,
                float(entry_price),
                max_price,
            )
            return None
        neg_risk = market_neg_risk(market)
        if shares is not None:
            amount = round(float(shares) * float(entry_price), 2)
            LOGGER.info(
                "live buy target fixed shares side=%s market=%s price=%.4f target_shares=%s target_amount_usd=%.2f neg_risk=%s",
                side,
                market.market_id,
                float(entry_price),
                shares,
                amount,
                neg_risk,
            )
            result = self.executor.place_buy_order_shares(token_id, float(shares), price=entry_price, neg_risk=neg_risk)
        else:
            amount = float(amount_usd if amount_usd is not None else config["trading"]["buy_notional_usdc"])
            LOGGER.info(
                "live buy target notional side=%s market=%s price=%.4f target_amount_usd=%.2f neg_risk=%s",
                side,
                market.market_id,
                float(entry_price),
                amount,
                neg_risk,
            )
            result = self.executor.place_buy_order(token_id, amount, price=entry_price, neg_risk=neg_risk)
        if not _result_value(result, "success", False):
            LOGGER.error("live buy rejected side=%s market=%s price=%s neg_risk=%s error=%s", side, market.market_id, entry_price, neg_risk, _result_value(result, "error", ""))
            return None

        trade = make_trade(
            config,
            cycle_id,
            market,
            wu_source,
            station,
            side,
            float(_result_value(result, "price", entry_price)),
            observed_high,
            observed_low,
            reason,
            notional_usdc=float(_result_value(result, "amount_usd", amount)),
            shares_override=float(_result_value(result, "shares", shares or 0.0)) if shares is not None else None,
        )
        self._apply_buy_result_to_trade(trade, result, pending=True)
        self._register_pending(
            LivePendingOrder(
                kind="BUY",
                trade_id=trade.trade_id,
                order_id=str(_result_value(result, "order_id", "")),
                token_id=token_id,
                condition_id=market.condition_id,
                price=float(_result_value(result, "price", entry_price)),
                shares=float(_result_value(result, "shares", trade.shares)),
                created_ts=time.time(),
                balance_before=float(_result_value(result, "balance_before", 0.0) or 0.0),
                token_balance_before=_result_value(result, "token_balance_before", None),
            )
        )
        LOGGER.info("live buy pending trade=%s order=%s side=%s market=%s price=%s shares=%s neg_risk=%s", trade.trade_id, trade.live_buy_order_id, side, market.market_id, trade.yes_price, trade.shares, neg_risk)
        if notify_submitted:
            notify_trade(config, trade, "BUY", "SUBMITTED", reason)
        return trade

    def start_model_awc_hourly_batch(
        self,
        city: str,
        station: str,
        event_date: str,
        local_hour: int,
        markets: tuple[TemperatureMarket, ...],
        sides: tuple[str, ...],
        target_shares: float,
        predicted_high_f: float,
        mode: str,
        target_notional_usd: float = 0.0,
        local_minute: int = 0,
    ) -> str:
        """Replace the prior city/hour target and begin websocket order management."""
        if not self.executor or not self.market_feed:
            raise RuntimeError("live trading manager is not started")
        batch_id = (
            f"{city}:{station}:{event_date}:hour_{local_hour:02d}:minute_{local_minute:02d}:{mode}"
        )
        group_prefix = f"{city}:{station}:{event_date}:"
        token_ids = tuple(
            asset_id_for_market_side(market, side)
            for market, side in zip(markets, sides)
        )
        if not all(token_ids):
            raise RuntimeError(f"missing token id for managed batch {batch_id}")
        with self._lock:
            old_batches = [
                batch
                for key, batch in self._hourly_batches.items()
                if key.startswith(group_prefix)
            ]
        for old in old_batches:
            self._close_hourly_batch(old, "next_hour_model_output")
        baseline = {
            token_id: float(
                self.executor._get_token_balance_optional(
                    token_id, refresh=True
                )
                or 0.0
            )
            for token_id in token_ids
        }
        batch = ModelAwcHourlyBatch(
            batch_id=batch_id,
            city=city,
            station=station,
            event_date=event_date,
            local_hour=local_hour,
            mode=mode,
            markets=markets,
            sides=sides,
            token_ids=token_ids,
            target_shares=float(target_shares),
            target_notional_usd=float(target_notional_usd or 0.0),
            predicted_high_f=float(predicted_high_f),
            cycle_id=(
                f"{datetime.now().strftime('%Y%m%dT%H%M%S')}:"
                f"model_awc_managed:{city}:{station}:{event_date}:"
                f"hour_{local_hour:02d}_minute_{local_minute:02d}"
            ),
            reason=f"model_awc_managed_{mode}_hour_{local_hour:02d}_minute_{local_minute:02d}",
            baseline_balances=baseline,
            acquired_shares={token_id: 0.0 for token_id in token_ids},
            acquired_cost_usd={token_id: 0.0 for token_id in token_ids},
            average_prices={token_id: 0.0 for token_id in token_ids},
            open_order_ids={},
            expires_ts=time.time()
            + max(
                1.0,
                float(
                    self.config["trading"].get(
                        "model_awc_order_management_window_minutes", 40
                    )
                ),
            )
            * 60.0,
        )
        with self._lock:
            self._hourly_batches[batch_id] = batch
        self._refresh_hourly_market_subscriptions()
        self._market_wakeup.set()
        LOGGER.info(
            "model awc hourly batch started batch=%s mode=%s tokens=%s "
            "target_shares=%s target_notional_usd=%s predicted_high_f=%r window_minutes=%s",
            batch_id,
            mode,
            [token[:16] for token in token_ids],
            target_shares,
            target_notional_usd,
            predicted_high_f,
            self.config["trading"].get(
                "model_awc_order_management_window_minutes", 40
            ),
        )
        return batch_id

    def _on_market_message(
        self, _raw: str, received_ts: Optional[float] = None
    ) -> None:
        """Wake the hourly manager after a market book or price update."""
        self._market_wakeup.set()

    def _refresh_hourly_market_subscriptions(self) -> None:
        if not self.market_feed:
            return
        with self._lock:
            batches = [
                batch for batch in self._hourly_batches.values()
                if not batch.closed
            ]
        asset_ids = sorted(
            {token for batch in batches for token in batch.token_ids}
        )
        labels = {
            token: f"{batch.city}:{batch.local_hour}:{idx}"
            for batch in batches
            for idx, token in enumerate(batch.token_ids)
        }
        self.market_feed.subscribe(asset_ids, labels)

    def _close_hourly_batch(
        self, batch: ModelAwcHourlyBatch, reason: str
    ) -> None:
        for token_id in list(batch.open_order_ids):
            self._cancel_batch_order(batch, token_id, reason=reason)
        batch.closed = True
        with self._lock:
            self._hourly_batches.pop(batch.batch_id, None)
        LOGGER.info(
            "model awc hourly batch closed batch=%s reason=%s acquired=%s",
            batch.batch_id,
            reason,
            batch.acquired_shares,
        )
        self._refresh_hourly_market_subscriptions()

    def _hourly_batch_loop(self) -> None:
        while self._running:
            self._market_wakeup.wait(timeout=1.0)
            self._market_wakeup.clear()
            with self._lock:
                batches = list(self._hourly_batches.values())
            for batch in batches:
                try:
                    self._manage_hourly_batch(batch)
                except Exception:
                    LOGGER.exception(
                        "model awc hourly batch management failed batch=%s",
                        batch.batch_id,
                    )

    def _batch_token_balance(self, batch: ModelAwcHourlyBatch, token: str) -> float:
        # Websocket order/trade messages are the primary fill signal. This
        # cached balance is only a reconciliation fallback; forcing a refresh
        # on every market tick caused balance-allowance request storms.
        current = self.executor._get_token_balance_optional(token, refresh=False)
        confirmed = batch.acquired_shares.get(token, 0.0)
        if current is None:
            return confirmed
        reconciled = max(
            0.0,
            float(current) - batch.baseline_balances.get(token, 0.0),
        )
        # Cached balances can lag websocket fill confirmations. Never let a
        # stale cache erase confirmed fills and cause the manager to rebuy.
        return max(confirmed, reconciled)

    def _cancel_batch_order(
        self,
        batch: ModelAwcHourlyBatch,
        token_id: str,
        reason: str = "managed_reprice",
    ) -> None:
        order_id = batch.open_order_ids.pop(token_id, "")
        if not order_id:
            return
        with self._lock:
            pending = self._pending.get(order_id)
        if pending is not None and pending.kind == "BUY":
            result = self.executor.check_pending_buy(
                pending.order_id,
                pending.price,
                pending.shares,
                pending.token_id,
                pending.balance_before,
                pending.token_balance_before,
            )
            if result and _result_value(result, "success", False):
                self._apply_order_result(
                    pending, result, source="managed_pre_cancel"
                )
                pending = None
        cancelled = self.executor.cancel_order(order_id)
        with self._lock:
            self._managed_order_ids.discard(order_id)
        if pending is not None:
            self._mark_order_cancelled(pending, reason, cancelled)

    def _record_managed_buy_fill(
        self, pending: LivePendingOrder, result: Any
    ) -> None:
        """Update hourly batch fill accounting for a managed BUY order."""
        if pending.order_id not in self._managed_order_ids:
            return
        shares = float(_result_value(result, "shares", 0.0) or 0.0)
        amount = float(
            _result_value(
                result,
                "amount_usd",
                shares * float(_result_value(result, "price", pending.price) or pending.price),
            )
            or 0.0
        )
        status = str(_result_value(result, "status", "") or "").upper()
        shares_remaining = float(_result_value(result, "shares_remaining", 0.0) or 0.0)
        with self._lock:
            batches = list(self._hourly_batches.values())
        for batch in batches:
            for token_id, order_id in list(batch.open_order_ids.items()):
                if order_id != pending.order_id:
                    continue
                batch.acquired_shares[token_id] = (
                    batch.acquired_shares.get(token_id, 0.0) + shares
                )
                batch.acquired_cost_usd[token_id] = (
                    batch.acquired_cost_usd.get(token_id, 0.0) + amount
                )
                if status == "FILLED" or shares_remaining < 1:
                    batch.open_order_ids.pop(token_id, None)
                    with self._lock:
                        self._managed_order_ids.discard(pending.order_id)
                LOGGER.info(
                    "model awc managed fill batch=%s token=%s shares=%s amount_usd=%.4f acquired_cost_usd=%.4f target_notional_usd=%.4f",
                    batch.batch_id,
                    token_id[:16],
                    shares,
                    amount,
                    batch.acquired_cost_usd.get(token_id, 0.0),
                    batch.target_notional_usd,
                )
                return

    def _submit_batch_order(
        self,
        batch: ModelAwcHourlyBatch,
        leg_index: int,
        shares: float,
        price: float,
        reason: str,
    ) -> Optional[PaperTrade]:
        order_shares = float(int(max(0.0, shares)))
        if order_shares < 1:
            return None
        if batch.token_ids[leg_index] in batch.open_order_ids:
            self._cancel_batch_order(batch, batch.token_ids[leg_index])
        market = batch.markets[leg_index]
        side = batch.sides[leg_index]
        trade = self.submit_buy_trade(
            self.config,
            batch.cycle_id,
            market,
            "",
            batch.station,
            side,
            float(price),
            batch.predicted_high_f,
            None,
            reason,
            amount_usd=round(order_shares * float(price), 2),
            shares=order_shares,
            notify_submitted=False,
        )
        if trade is None:
            return None
        trades = read_trades(self.config["outputs"]["trades_csv"])
        trades.append(trade)
        write_csv(self.config["outputs"]["trades_csv"], trades)
        write_csv(self.config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(self.config, trades)
        token_id = batch.token_ids[leg_index]
        batch.open_order_ids[token_id] = trade.live_buy_order_id
        batch.average_prices[token_id] = float(price)
        with self._lock:
            self._managed_order_ids.add(trade.live_buy_order_id)
        return trade

    def _live_offer(
        self, token_id: str
    ) -> Optional[tuple[float, float]]:
        if not self.market_feed:
            return None
        price = self.market_feed.get_price(token_id)
        if price is None or price.best_ask <= 0 or price.ask_size <= 0:
            return None
        return float(price.best_ask), float(price.ask_size)

    def _manage_hourly_batch(self, batch: ModelAwcHourlyBatch) -> None:
        if batch.closed:
            return
        if time.time() >= batch.expires_ts:
            self._close_hourly_batch(batch, "management_window_expired")
            return
        if time.time() < batch.next_action_ts:
            return
        for token in batch.token_ids:
            batch.acquired_shares[token] = self._batch_token_balance(
                batch, token
            )
        if batch.mode == "single":
            self._manage_single_hourly_batch(batch)
        else:
            self._manage_adjacent_hourly_batch(batch)

    def _manage_single_hourly_batch(
        self, batch: ModelAwcHourlyBatch
    ) -> None:
        token = batch.token_ids[0]
        if batch.target_notional_usd > 0:
            spent = batch.acquired_cost_usd.get(token, 0.0)
            remaining_notional = max(0.0, batch.target_notional_usd - spent)
            if remaining_notional < 0.01:
                self._close_hourly_batch(batch, "target_filled")
                return
            if token in batch.open_order_ids:
                return
            max_price = configured_max_buy_price(self.config)
            offer = self._live_offer(token)
            if offer is not None and offer[0] <= max_price:
                price = float(offer[0])
                shares = min(float(offer[1]), remaining_notional / price)
                reason = f"{batch.reason}_single_websocket_offer"
            else:
                price = max_price
                shares = remaining_notional / price
                reason = f"{batch.reason}_single_limit_085"
            self._submit_batch_order(batch, 0, shares, price, reason)
            batch.next_action_ts = time.time() + 2.0
            return

        acquired = batch.acquired_shares[token]
        remaining = max(0, int(batch.target_shares - acquired))
        if remaining < 1:
            self._close_hourly_batch(batch, "target_filled")
            return
        if token in batch.open_order_ids:
            return
        # A 0.85 GTC bid both rests when the YES ask is too high and
        # immediately matches any websocket-visible seller at 0.85 or below.
        self._submit_batch_order(
            batch,
            0,
            remaining,
            configured_max_buy_price(self.config),
            f"{batch.reason}_single_limit_085",
        )
        batch.next_action_ts = time.time() + 2.0

    def _manage_adjacent_hourly_batch(
        self, batch: ModelAwcHourlyBatch
    ) -> None:
        left, right = batch.token_ids
        left_qty = batch.acquired_shares[left]
        right_qty = batch.acquired_shares[right]
        target = max(0.0, batch.target_shares)
        if left_qty >= target and right_qty >= target:
            self._close_hourly_batch(batch, "target_filled")
            return
        epsilon = 0.5
        if abs(left_qty - right_qty) >= epsilon:
            richer = left if left_qty > right_qty else right
            poorer = right if richer == left else left
            for token in list(batch.open_order_ids):
                self._cancel_batch_order(
                    batch, token, reason="adjacent_imbalance_reprice"
                )
            richer_qty = batch.acquired_shares[richer]
            poorer_qty = batch.acquired_shares[poorer]
            # Repair only up to the configured per-leg target. If one leg
            # somehow overshoots, do not amplify it by chasing that excess.
            deficit = int(max(0.0, min(richer_qty, target) - poorer_qty))
            if deficit < 1 or poorer in batch.open_order_ids:
                return
            other_price = batch.average_prices.get(richer, 0.0)
            repair_price = round(
                max(
                    0.01,
                    min(
                        configured_max_buy_price(self.config),
                        0.85 - other_price,
                    ),
                ),
                2,
            )
            batch.repair_token_id = poorer
            self._submit_batch_order(
                batch,
                batch.token_ids.index(poorer),
                deficit,
                repair_price,
                f"{batch.reason}_balance_repair",
            )
            batch.next_action_ts = time.time() + 2.0
            LOGGER.info(
                "model awc adjacent repair batch=%s poorer=%s deficit=%s "
                "repair_price=%s other_cost=%s",
                batch.batch_id,
                poorer[:16],
                deficit,
                repair_price,
                other_price,
            )
            return

        if batch.repair_token_id:
            self._cancel_batch_order(batch, batch.repair_token_id)
            batch.repair_token_id = ""
        for token in list(batch.open_order_ids):
            self._cancel_batch_order(batch, token)
        equal_qty = min(left_qty, right_qty)
        remaining = max(0, int(batch.target_shares - equal_qty))
        if remaining < 1:
            self._close_hourly_batch(batch, "target_filled")
            return
        offers = [self._live_offer(token) for token in batch.token_ids]
        if any(offer is None for offer in offers):
            return
        left_offer, right_offer = offers
        assert left_offer is not None and right_offer is not None
        max_price = configured_max_buy_price(self.config)
        max_total = float(
            self.config["trading"].get(
                "model_awc_adjacent_yes_max_total_price", 0.9
            )
        )
        if (
            left_offer[0] > max_price
            or right_offer[0] > max_price
            or left_offer[0] + right_offer[0] > max_total
        ):
            return
        common_shares = min(
            remaining, int(left_offer[1]), int(right_offer[1])
        )
        if common_shares < 1:
            return
        submitted = []
        for idx, offer in enumerate((left_offer, right_offer)):
            submitted.append(
                self._submit_batch_order(
                    batch,
                    idx,
                    common_shares,
                    offer[0],
                    f"{batch.reason}_websocket_equal_pair",
                )
            )
        batch.next_action_ts = time.time() + 2.0
        LOGGER.info(
            "model awc adjacent websocket buy batch=%s shares_each=%s "
            "prices=%s total=%s submitted=%s",
            batch.batch_id,
            common_shares,
            [left_offer[0], right_offer[0]],
            left_offer[0] + right_offer[0],
            [trade is not None for trade in submitted],
        )

    def submit_sell_trade(self, config: dict[str, Any], trade: PaperTrade, market: TemperatureMarket, reason: str, exit_price: float) -> bool:
        """Post a live sell order for an existing open trade.
        
        Args:
            config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
            trade (PaperTrade): Existing open trade to sell.
            market (TemperatureMarket): Market associated with the trade.
            reason (str): Exit reason to persist on the trade row.
            exit_price (float): Limit sell price selected from the CLOB order book.
        
        Returns:
            bool: True when a sell order was accepted and is pending confirmation.
        """
        if not self.executor:
            raise RuntimeError("live trading manager is not started")
        token_id = trade.asset_id or asset_id_for_market_side(market, trade.position_side or "YES")
        if not token_id:
            trade.live_order_error = "missing token id for sell"
            LOGGER.warning("live sell skipped missing token trade=%s market=%s", trade.trade_id, market.market_id)
            return False
        neg_risk = market_neg_risk(market)
        result = self.executor.place_sell_order(token_id, trade.shares, price=exit_price, neg_risk=neg_risk)
        if not _result_value(result, "success", False):
            trade.live_order_status = str(_result_value(result, "status", "FAILED"))
            trade.live_order_error = str(_result_value(result, "error", ""))
            LOGGER.error("live sell rejected trade=%s order_error=%s", trade.trade_id, trade.live_order_error)
            return False
        side = (trade.position_side or "YES").upper()
        trade.status = "SELL_PENDING"
        trade.exit_at = datetime.now().isoformat(timespec="seconds")
        trade.exit_reason = reason
        trade.exit_action = "sell_no" if side == "NO" else "sell_yes"
        if side == "NO":
            trade.exit_no_price = float(_result_value(result, "price", exit_price))
        else:
            trade.exit_yes_price = float(_result_value(result, "price", exit_price))
        trade.execution_mode = "LIVE_DRY_RUN" if bool(_result_value(result, "dry_run", False)) else "LIVE"
        trade.live_sell_order_id = str(_result_value(result, "order_id", ""))
        trade.live_order_status = str(_result_value(result, "status", "PENDING"))
        trade.live_order_error = ""
        self._register_pending(
            LivePendingOrder(
                kind="SELL",
                trade_id=trade.trade_id,
                order_id=trade.live_sell_order_id,
                token_id=token_id,
                condition_id=market.condition_id,
                price=float(_result_value(result, "price", exit_price)),
                shares=float(_result_value(result, "shares", trade.shares)),
                created_ts=time.time(),
                balance_before=float(_result_value(result, "balance_before", 0.0) or 0.0),
                token_balance_before=_result_value(result, "token_balance_before", None),
            )
        )
        LOGGER.info("live sell pending trade=%s order=%s market=%s price=%s shares=%s", trade.trade_id, trade.live_sell_order_id, market.market_id, exit_price, trade.shares)
        notify_trade(config, trade, "SELL", "SUBMITTED", reason)
        return True

    def _register_pending(self, pending: LivePendingOrder) -> None:
        """Store a pending order and refresh the user websocket market subscriptions.
        
        Args:
            pending (LivePendingOrder): Pending order metadata to track.
        
        Returns:
            None: This function is executed for its side effects.
        """
        if not pending.order_id:
            return
        with self._lock:
            self._pending[pending.order_id] = pending
            condition_ids = sorted({p.condition_id for p in self._pending.values() if p.condition_id})
        if self.user_feed and condition_ids:
            self.user_feed.subscribe(condition_ids)

    def _recover_pending_from_disk(self) -> None:
        """Reload BUY_PENDING and SELL_PENDING rows from the trade CSV after startup.
        
        Args:
            None.
        
        Returns:
            None: This function is executed for its side effects.
        """
        try:
            trades = read_trades(self.config["outputs"]["trades_csv"])
        except Exception:
            LOGGER.exception("live pending recovery failed")
            return
        for trade in trades:
            if trade.status == "BUY_PENDING" and trade.live_buy_order_id:
                self._register_pending(LivePendingOrder("BUY", trade.trade_id, trade.live_buy_order_id, trade.asset_id, trade.condition_id, float(trade.yes_price or 0.0), float(trade.shares or 0.0), time.time()))
            elif trade.status == "SELL_PENDING" and trade.live_sell_order_id:
                price = float(trade.exit_no_price or trade.exit_yes_price or 0.0)
                self._register_pending(LivePendingOrder("SELL", trade.trade_id, trade.live_sell_order_id, trade.asset_id, trade.condition_id, price, float(trade.shares or 0.0), time.time()))

    def _on_user_order_message(self, raw: str, received_ts: Optional[float] = None) -> None:
        """Handle raw authenticated Polymarket websocket messages.
        
        Args:
            raw (str): Raw websocket payload.
            received_ts (Optional[float]): Local receipt timestamp supplied by PolymarketUserFeed.
        
        Returns:
            None: This function is executed for its side effects.
        """
        try:
            payload = json.loads(raw)
        except Exception:
            return
        messages = payload if isinstance(payload, list) else [payload]
        for msg in messages:
            if isinstance(msg, dict):
                self._handle_user_order_update(msg, received_ts or time.time())

    def _handle_user_order_update(self, msg: dict[str, Any], received_ts: float) -> None:
        """Match one user websocket order or trade update against tracked pending orders.
        
        Args:
            msg (dict[str, Any]): Decoded websocket message.
            received_ts (float): Local Unix timestamp when the message was received.
        
        Returns:
            None: This function is executed for its side effects.
        """
        event_type = str(msg.get("event_type") or msg.get("type") or "").lower()
        status = str(msg.get("status") or msg.get("type") or "").upper()
        if event_type not in {"order", "trade"} and status not in {"MATCHED", "UPDATE"}:
            return
        with self._lock:
            pending_orders = list(self._pending.values())
        for pending in pending_orders:
            match = self._matched_order_from_user_msg(msg, pending.order_id)
            if not match:
                continue
            shares, price = match
            if shares <= 0:
                continue
            result = {
                "success": True,
                "order_id": pending.order_id,
                "status": "FILLED" if max(0.0, pending.shares - shares) < 1 else "PARTIAL",
                "side": pending.kind,
                "price": price or pending.price,
                "amount_usd": shares * (price or pending.price),
                "shares": shares,
                "shares_remaining": max(0.0, pending.shares - shares),
                "token_id": pending.token_id[:16] + "...",
                "dry_run": False,
            }
            LOGGER.info("live order websocket confirmed order=%s kind=%s shares=%s price=%s latency_ms=%.0f", pending.order_id, pending.kind, shares, price, (time.time() - received_ts) * 1000)
            self._apply_order_result(pending, result, source="user_websocket")
            self._market_wakeup.set()
            return

    def _matched_order_from_user_msg(self, msg: dict[str, Any], order_id: str) -> Optional[tuple[float, float]]:
        """Extract matched shares and price for a pending order from a Polymarket user message.
        
        Args:
            msg (dict[str, Any]): Decoded websocket message.
            order_id (str): Pending order id to match.
        
        Returns:
            Optional[tuple[float, float]]: Matched shares and price, or None when the message is unrelated.
        """
        event_type = str(msg.get("event_type") or msg.get("type") or "").lower()
        msg_type = str(msg.get("type") or "").upper()
        if event_type == "order" or msg_type in {"UPDATE", "PLACEMENT", "CANCELLATION"}:
            if str(msg.get("id") or "") != order_id:
                return None
            return _as_float(msg.get("size_matched")), _as_float(msg.get("price"))
        if event_type == "trade" or msg_type == "MATCHED":
            if str(msg.get("taker_order_id") or "") == order_id:
                return _as_float(msg.get("size")), _as_float(msg.get("price"))
            for maker in msg.get("maker_orders") or []:
                if str(maker.get("order_id") or "") == order_id:
                    return _as_float(maker.get("matched_amount")), _as_float(maker.get("price") or msg.get("price"))
        return None

    def _poll_loop(self) -> None:
        """Poll pending live orders as a fallback when websocket confirmation is delayed.
        
        Args:
            None.
        
        Returns:
            None: This function is executed for its side effects.
        """
        check_seconds = max(1.0, float(self.config.get("trading", {}).get("live_order_check_seconds", 5)))
        while self._running:
            time.sleep(check_seconds)
            try:
                self.poll_pending_orders()
            except Exception:
                LOGGER.exception("live pending poll failed")

    def poll_pending_orders(self) -> None:
        """Run one pending-order verification and timeout cancellation pass.
        
        Args:
            None.
        
        Returns:
            None: This function is executed for its side effects.
        """
        if not self.executor:
            return
        timeout = max(1.0, float(self.config.get("trading", {}).get("live_order_timeout_seconds", 12)))
        with self._lock:
            pending_orders = list(self._pending.values())
        for pending in pending_orders:
            with self._lock:
                managed_order = pending.order_id in self._managed_order_ids
            if managed_order:
                # Managed model-AWC orders are driven by the authenticated
                # user websocket and the hourly batch's cached balance
                # reconciliation. Do not poll them every second.
                continue
            if pending.kind == "BUY":
                result = self.executor.check_pending_buy(pending.order_id, pending.price, pending.shares, pending.token_id, pending.balance_before, pending.token_balance_before)
            else:
                result = self.executor.check_pending_sell(pending.order_id, pending.price, pending.shares, pending.token_id, pending.balance_before, pending.token_balance_before)
            if result and _result_value(result, "success", False):
                self._apply_order_result(pending, result, source="poll")
                continue
            if time.time() - pending.created_ts >= timeout:
                cancelled = self.executor.cancel_order(pending.order_id)
                self._mark_order_cancelled(pending, "timeout", cancelled)

    def _apply_order_result(self, pending: LivePendingOrder, result: Any, source: str) -> None:
        """Apply a confirmed buy or sell fill to the persisted trade CSV.
        
        Args:
            pending (LivePendingOrder): Pending order that matched.
            result (Any): Executor OrderResult or websocket-derived result dictionary.
            source (str): Confirmation source, such as user_websocket or poll.
        
        Returns:
            None: This function is executed for its side effects.
        """
        if pending.kind == "BUY":
            self._record_managed_buy_fill(pending, result)
        trades = read_trades(self.config["outputs"]["trades_csv"])
        changed = False
        for trade in trades:
            if trade.trade_id != pending.trade_id:
                continue
            if pending.kind == "BUY":
                self._apply_buy_result_to_trade(trade, result, pending=False)
            else:
                self._apply_sell_result_to_trade(trade, result, source)
            trade.live_order_status = str(_result_value(result, "status", "FILLED"))
            trade.live_order_error = ""
            changed = True
            break
        if not changed:
            LOGGER.info("live order confirmed before trade row persisted order=%s trade=%s source=%s", pending.order_id, pending.trade_id, source)
            return
        write_csv(self.config["outputs"]["trades_csv"], trades)
        write_csv(self.config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(self.config, trades)
        with self._lock:
            self._pending.pop(pending.order_id, None)
        notify_trade(self.config, trade, pending.kind, "FILLED", source=source)

    def _apply_buy_result_to_trade(self, trade: PaperTrade, result: Any, pending: bool) -> None:
        """Copy live buy order result fields onto a trade row.
        
        Args:
            trade (PaperTrade): Trade row to mutate.
            result (Any): Executor OrderResult or websocket-derived result dictionary.
            pending (bool): Whether the order is still awaiting confirmation.
        
        Returns:
            None: This function is executed for its side effects.
        """
        price = float(_result_value(result, "price", trade.yes_price or 0.0) or 0.0)
        shares = float(_result_value(result, "shares", trade.shares or 0.0) or 0.0)
        amount = float(_result_value(result, "amount_usd", shares * price) or 0.0)
        fee = taker_fee_usdc(shares, price, float(trade.taker_fee_rate), bool(self.config["trading"].get("fee_enabled", True)))
        trade.yes_price = price
        trade.notional_usdc = round(amount, 8)
        trade.shares = shares
        trade.buy_fee_usdc = fee
        trade.total_cost_usdc = round(amount + fee, 8)
        trade.status = "BUY_PENDING" if pending else "OPEN"
        trade.execution_mode = "LIVE_DRY_RUN" if bool(_result_value(result, "dry_run", False)) else "LIVE"
        trade.live_buy_order_id = str(_result_value(result, "order_id", trade.live_buy_order_id))
        trade.live_order_status = str(_result_value(result, "status", "PENDING" if pending else "FILLED"))
        trade.live_order_error = str(_result_value(result, "error", "") or "")

    def _apply_sell_result_to_trade(self, trade: PaperTrade, result: Any, source: str) -> None:
        """Copy live sell fill fields onto a trade row and compute realized PnL.
        
        Args:
            trade (PaperTrade): Trade row to mutate.
            result (Any): Executor OrderResult or websocket-derived result dictionary.
            source (str): Confirmation source, such as user_websocket or poll.
        
        Returns:
            None: This function is executed for its side effects.
        """
        price = float(_result_value(result, "price", trade.exit_no_price or trade.exit_yes_price or 0.0) or 0.0)
        sold_shares = float(_result_value(result, "shares", trade.shares or 0.0) or 0.0)
        shares_remaining = float(_result_value(result, "shares_remaining", max(0.0, trade.shares - sold_shares)) or 0.0)
        fee = taker_fee_usdc(sold_shares, price, float(trade.taker_fee_rate), bool(self.config["trading"].get("fee_enabled", True)))
        proceeds = sold_shares * price - fee
        side = (trade.position_side or "YES").upper()
        if side == "NO":
            trade.exit_no_price = price
        else:
            trade.exit_yes_price = price
        trade.exit_fee_usdc = round(fee, 8)
        trade.exit_proceeds_usdc = round(proceeds, 8)
        trade.payout_usdc = trade.exit_proceeds_usdc
        trade.settlement_source = source
        trade.live_sell_order_id = str(_result_value(result, "order_id", trade.live_sell_order_id))
        if shares_remaining < 1:
            trade.status = "SOLD"
            trade.pnl_usdc = round(proceeds - trade.total_cost_usdc, 8)
        else:
            original_shares = max(0.0, sold_shares + shares_remaining)
            sold_cost = trade.total_cost_usdc * (sold_shares / original_shares) if original_shares > 0 else 0.0
            trade.shares = shares_remaining
            trade.total_cost_usdc = round(max(0.0, trade.total_cost_usdc - sold_cost), 8)
            trade.status = "OPEN"
            trade.error = f"partial live sell via {source}; sold {sold_shares:.4f}, remaining {shares_remaining:.4f}"

    def _mark_order_cancelled(self, pending: LivePendingOrder, reason: str, cancelled: bool) -> None:
        """Persist a cancelled or expired pending order state.
        
        Args:
            pending (LivePendingOrder): Pending order that timed out.
            reason (str): Human-readable cancellation reason.
            cancelled (bool): Whether executor.py reported successful cancellation.
        
        Returns:
            None: This function is executed for its side effects.
        """
        trades = read_trades(self.config["outputs"]["trades_csv"])
        for trade in trades:
            if trade.trade_id != pending.trade_id:
                continue
            trade.live_order_status = "CANCELLED" if cancelled else "CANCEL_FAILED"
            trade.live_order_error = reason
            if pending.kind == "BUY":
                trade.status = "BUY_CANCELLED" if cancelled else "BUY_CANCEL_FAILED"
            else:
                trade.status = "OPEN"
            break
        write_csv(self.config["outputs"]["trades_csv"], trades)
        write_csv(self.config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(self.config, trades)
        with self._lock:
            self._pending.pop(pending.order_id, None)
        LOGGER.info("live order cancelled order=%s kind=%s reason=%s cancelled=%s", pending.order_id, pending.kind, reason, cancelled)


def make_trade(
    config: dict[str, Any],
    cycle_id: str,
    market: TemperatureMarket,
    wu_source: str,
    station: str,
    side: str,
    entry_price: float,
    observed_high: Optional[float],
    observed_low: Optional[float],
    reason: str,
    notional_usdc: Optional[float] = None,
    shares_override: Optional[float] = None,
) -> PaperTrade:
    """Create a PaperTrade record for a deterministic strategy entry.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        cycle_id (str): Identifier for the strategy cycle that created the trade.
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        wu_source (str): Weather Underground source URL associated with the event.
        station (str): Weather station identifier, usually a four-character ICAO code.
        side (str): Position side, expected to be YES or NO.
        entry_price (float): CLOB-verified buy price used for the simulated entry.
        observed_high (Optional[float]): Observed high temperature for the event day, if available.
        observed_low (Optional[float]): Observed low temperature for the event day, if available.
        reason (str): Machine-readable reason for creating, closing, or syncing a record.
    
    Returns:
        PaperTrade: New PaperTrade record.
    """
    now = datetime.now().isoformat(timespec="seconds")
    side = side.upper()
    price = float(entry_price)
    if shares_override is not None:
        shares = float(shares_override)
        notional = shares * price
    else:
        notional = float(notional_usdc if notional_usdc is not None else config["trading"]["buy_notional_usdc"])
        shares = notional / price if price > 0 else 0.0
    fee = taker_fee_usdc(shares, price, float(config["trading"]["fee_rate"]), bool(config["trading"].get("fee_enabled", True)))
    comparable_min, comparable_max, comparable_unit = comparable_rule_bounds(market, market.unit)
    return PaperTrade(
        trade_id=f"{cycle_id}:{side}:{market.market_id}:{time.time_ns()}",
        created_at=now,
        cycle_id=cycle_id,
        strategy=str(config["trading"]["strategy_name"]),
        event_id=market.event_id,
        market_id=market.market_id,
        condition_id=market.condition_id,
        city=market.city,
        kind=market.kind,
        event_date=market.event_date,
        event_title=market.event_title,
        market_question=market.market_question,
        polymarket_url=market.polymarket_url,
        wunderground_source_url=wu_source,
        forecast_source=f"deterministic_harvest_{reason}",
        forecast_observed_at=now,
        forecast_station=station,
        forecast_temp=observed_high if market.kind == "Highest" else observed_low,
        forecast_high=observed_high,
        forecast_low=observed_low,
        forecast_first_valid_time_local="",
        forecast_last_valid_time_local="",
        forecast_unit=market.unit,
        rule_min=market.rule_min,
        rule_max=market.rule_max,
        market_unit=market.unit,
        comparable_rule_min=comparable_min,
        comparable_rule_max=comparable_max,
        comparable_unit=comparable_unit,
        yes_price=price,
        mispricing_price_threshold=1.0,
        pricing_edge=0.0,
        notional_usdc=notional,
        shares=shares,
        taker_fee_rate=float(config["trading"]["fee_rate"]),
        buy_fee_usdc=fee,
        total_cost_usdc=round(notional + fee, 8),
        position_side=side,
        monitor_last_yes_price=market.yes_price,
        monitor_last_checked_at=now,
        monitor_price_trigger=float(config["trading"].get("monitor_price_change_pct", 0.03)),
        asset_id=asset_id_for_market_side(market, side),
    )


def close_trade(config: dict[str, Any], trade: PaperTrade, market: TemperatureMarket, reason: str) -> bool:
    """Mark an open PaperTrade as sold and compute exit proceeds and PnL.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        trade (PaperTrade): PaperTrade record being evaluated or mutated.
        market (TemperatureMarket): Normalized TemperatureMarket object or raw market payload being evaluated.
        reason (str): Machine-readable reason for creating, closing, or syncing a record.
    
    Returns:
        bool: True when the trade was closed in paper mode or a live sell order was accepted.
    """
    side = (trade.position_side or "YES").upper()
    exit_price = float(best_sell_price(config, market, side) or 0.0)
    hedge_yes_price = float(best_buy_price(config, market, "YES") or 0.0) if side == "NO" else 0.0
    hedge_effective_exit = (1.0 - hedge_yes_price) if hedge_yes_price > 0 else 0.0
    if side == "NO" and hedge_effective_exit > exit_price:
        return hedge_no_trade_with_yes(config, trade, market, reason, hedge_yes_price)
    if exit_price <= 0:
        trade.error = f"no CLOB sell price for {side}"
        LOGGER.info("skip sell no_clob_sell_price trade=%s side=%s market=%s", trade.trade_id, side, market.market_id)
        return False
    live_trader = get_live_trader() if live_trading_enabled(config) else None
    if live_trader:
        return bool(live_trader.submit_sell_trade(config, trade, market, reason, exit_price))
    if side == "NO":
        trade.exit_action = "sell_no"
        trade.exit_no_price = exit_price
    else:
        trade.exit_action = "sell_yes"
        trade.exit_yes_price = exit_price
    fee = taker_fee_usdc(trade.shares, exit_price, float(trade.taker_fee_rate), bool(config["trading"].get("fee_enabled", True)))
    proceeds = trade.shares * exit_price - fee
    trade.status = "SOLD"
    trade.exit_at = datetime.now().isoformat(timespec="seconds")
    trade.exit_reason = reason
    trade.exit_fee_usdc = round(fee, 8)
    trade.exit_proceeds_usdc = round(proceeds, 8)
    trade.payout_usdc = trade.exit_proceeds_usdc
    trade.pnl_usdc = round(proceeds - trade.total_cost_usdc, 8)
    notify_trade(config, trade, "SELL", "CLOSED", reason)
    return True


def hedge_no_trade_with_yes(config: dict[str, Any], trade: PaperTrade, market: TemperatureMarket, reason: str, hedge_price: float) -> bool:
    """Close risk on a NO position by buying matching YES exposure when cheaper than selling NO."""
    if hedge_price <= 0 or (trade.position_side or "YES").upper() != "NO":
        return False
    token_id = asset_id_for_market_side(market, "YES")
    if not token_id:
        trade.error = "no YES token id for hedge"
        return False
    fee_rate = float(trade.taker_fee_rate)
    fee_enabled = bool(config["trading"].get("fee_enabled", True))
    live_trader = get_live_trader() if live_trading_enabled(config) else None
    hedge_shares = float(trade.shares)
    hedge_notional = hedge_shares * hedge_price
    if live_trader and live_trader.executor:
        result = live_trader.executor.buy(token_id, hedge_notional, price=hedge_price)
        if not _result_value(result, "success", False):
            trade.error = f"YES hedge buy failed: {_result_value(result, 'error', '')}"
            LOGGER.info("skip hedge buy_yes_failed trade=%s market=%s error=%s", trade.trade_id, market.market_id, trade.error)
            return False
        hedge_price = float(_result_value(result, "price", hedge_price) or hedge_price)
        hedge_shares = float(_result_value(result, "shares", hedge_shares) or hedge_shares)
        hedge_notional = float(_result_value(result, "amount_usd", hedge_shares * hedge_price) or hedge_shares * hedge_price)
        trade.live_sell_order_id = str(_result_value(result, "order_id", trade.live_sell_order_id))
        trade.live_order_status = str(_result_value(result, "status", "FILLED"))
        trade.live_order_error = str(_result_value(result, "error", "") or "")
    fee = taker_fee_usdc(hedge_shares, hedge_price, fee_rate, fee_enabled)
    hedge_cost = hedge_notional + fee
    locked_payout = min(float(trade.shares), hedge_shares)
    trade.status = "HEDGED"
    trade.exit_action = "buy_yes_hedge"
    trade.exit_at = datetime.now().isoformat(timespec="seconds")
    trade.exit_reason = reason
    trade.exit_yes_price = hedge_price
    trade.exit_fee_usdc = round(fee, 8)
    trade.exit_hedge_cost_usdc = round(hedge_cost, 8)
    trade.payout_usdc = round(locked_payout, 8)
    trade.pnl_usdc = round(locked_payout - trade.total_cost_usdc - hedge_cost, 8)
    notify_trade(config, trade, "BUY", "HEDGED", reason)
    LOGGER.info(
        "hedge buy_yes trade=%s market=%s yes_price=%s hedge_shares=%s locked_payout=%s hedge_cost=%s reason=%s",
        trade.trade_id,
        market.market_id,
        hedge_price,
        hedge_shares,
        locked_payout,
        hedge_cost,
        reason,
    )
    return True


def unwind_yes_hedge_if_no_impossible(
    config: dict[str, Any],
    trade: PaperTrade,
    market: TemperatureMarket,
    observed_high: Optional[float],
    observed_low: Optional[float],
    event_unit: str,
    reason: str,
) -> bool:
    """Sell a previously bought YES hedge when the original NO is valid again."""
    if trade.status != "HEDGED" or (trade.position_side or "YES").upper() != "NO":
        return False
    if not deterministic_market_impossible(market, trade.kind, observed_high, observed_low, event_unit):
        return False
    token_id = asset_id_for_market_side(market, "YES")
    if not token_id:
        trade.error = "no YES token id to unwind hedge"
        return False
    sell_price = float(best_sell_price(config, market, "YES") or 0.0)
    if sell_price <= 0:
        trade.error = "no CLOB sell price for YES hedge"
        LOGGER.info("skip unwind hedge no_yes_sell_price trade=%s market=%s", trade.trade_id, market.market_id)
        return False
    hedge_shares = float(trade.shares)
    live_trader = get_live_trader() if live_trading_enabled(config) else None
    if live_trader and live_trader.executor:
        result = live_trader.executor.sell(token_id, hedge_shares, price=sell_price)
        if not _result_value(result, "success", False):
            trade.error = f"YES hedge sell failed: {_result_value(result, 'error', '')}"
            LOGGER.info("skip unwind hedge sell_failed trade=%s market=%s error=%s", trade.trade_id, market.market_id, trade.error)
            return False
        sell_price = float(_result_value(result, "price", sell_price) or sell_price)
        hedge_shares = float(_result_value(result, "shares", hedge_shares) or hedge_shares)
        trade.live_sell_order_id = str(_result_value(result, "order_id", trade.live_sell_order_id))
        trade.live_order_status = str(_result_value(result, "status", "FILLED"))
        trade.live_order_error = str(_result_value(result, "error", "") or "")
    fee = taker_fee_usdc(hedge_shares, sell_price, float(trade.taker_fee_rate), bool(config["trading"].get("fee_enabled", True)))
    proceeds = hedge_shares * sell_price - fee
    trade.status = "OPEN"
    trade.exit_action = "sell_yes_hedge"
    trade.exit_at = datetime.now().isoformat(timespec="seconds")
    trade.exit_reason = reason
    trade.exit_yes_price = sell_price
    trade.exit_fee_usdc = round(fee, 8)
    trade.exit_proceeds_usdc = round(proceeds, 8)
    trade.pnl_usdc = round(proceeds - float(trade.exit_hedge_cost_usdc or 0.0), 8)
    trade.payout_usdc = 0.0
    trade.settlement_source = "yes_hedge_unwound_no_impossible"
    trade.error = ""
    notify_trade(config, trade, "SELL", "HEDGE_UNWOUND", reason)
    LOGGER.info(
        "unwind yes hedge trade=%s market=%s yes_sell_price=%s hedge_shares=%s proceeds=%s reason=%s",
        trade.trade_id,
        market.market_id,
        sell_price,
        hedge_shares,
        proceeds,
        reason,
    )
    return True


def read_csv_dicts(path: str) -> list[dict[str, str]]:
    """Read a CSV file into dictionaries, returning an empty list if the file is absent.
    
    Args:
        path (str): Filesystem path to read from or write to.
    
    Returns:
        list[dict[str, str]]: CSV rows as dictionaries.
    """
    if not os.path.exists(path):
        return []
    with IO_LOCK, open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: str, rows: Iterable[Any]) -> None:
    """Write PaperTrade rows or dictionaries to a CSV file using the canonical trade headers.
    
    Args:
        path (str): Filesystem path to read from or write to.
        rows (Iterable[Any]): Rows to aggregate, convert, or write.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Creates parent directories if needed and overwrites the target CSV file.
    """
    data = [asdict(r) if hasattr(r, "__dataclass_fields__") else dict(r) for r in rows]
    if not data:
        return
    with IO_LOCK, open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


def read_trades(path: str) -> list[PaperTrade]:
    """Read persisted trade rows and convert them into PaperTrade objects.
    
    Args:
        path (str): Filesystem path to read from or write to.
    
    Returns:
        list[PaperTrade]: Persisted PaperTrade records.
    """
    trades: list[PaperTrade] = []
    numeric_fields = {
        "forecast_temp", "forecast_high", "forecast_low", "rule_min", "rule_max", "comparable_rule_min", "comparable_rule_max",
        "yes_price", "mispricing_price_threshold", "pricing_edge", "notional_usdc", "shares", "taker_fee_rate", "buy_fee_usdc",
        "total_cost_usdc", "exit_yes_price", "exit_no_price", "exit_fee_usdc", "exit_hedge_cost_usdc", "exit_proceeds_usdc",
        "monitor_last_yes_price", "monitor_price_trigger", "monitor_peak_pnl_usdc", "payout_usdc", "pnl_usdc",
    }
    optional_numeric = {"forecast_temp", "forecast_high", "forecast_low", "rule_min", "rule_max", "comparable_rule_min", "comparable_rule_max", "yes_price", "exit_yes_price", "exit_no_price", "monitor_last_yes_price"}
    defaults = {f.name: (f.default if f.default is not MISSING else "") for f in fields(PaperTrade)}
    for row in read_csv_dicts(path):
        cleaned: dict[str, Any] = {}
        for f in fields(PaperTrade):
            value = row.get(f.name, defaults.get(f.name, ""))
            if f.name in numeric_fields:
                cleaned[f.name] = None if value in (None, "") and f.name in optional_numeric else float(value or 0.0)
            else:
                cleaned[f.name] = value if value not in (None, "") else defaults.get(f.name, "")
        cleaned.setdefault("position_side", "YES")
        trades.append(PaperTrade(**cleaned))
    return trades


def pct(numerator: float, denominator: float) -> float:
    """Compute a rounded percentage while avoiding division by zero.
    
    Args:
        numerator (float): Value to divide by the denominator.
        denominator (float): Base value used for percentage calculation.
    
    Returns:
        float: Rounded percentage value.
    """
    return round((numerator / denominator * 100.0), 4) if denominator else 0.0


def performance_row(group_name: str, group_value: str, rows: list[PaperTrade]) -> dict[str, Any]:
    """Aggregate a group of trades into one performance report row.
    
    Args:
        group_name (str): Name of the report grouping dimension.
        group_value (str): Concrete value for the report grouping dimension.
        rows (list[PaperTrade]): Rows to aggregate, convert, or write.
    
    Returns:
        dict[str, Any]: Performance summary row.
    """
    closed = [r for r in rows if r.status in {"SETTLED", "SOLD"}]
    pnl = sum(r.pnl_usdc for r in closed)
    closed_cost = sum(r.total_cost_usdc for r in closed)
    wins = sum(1 for r in closed if r.pnl_usdc > 0)
    return {
        "group_name": group_name,
        "group_value": group_value,
        "trades": len(rows),
        "closed_trades": len(closed),
        "wins": wins,
        "win_rate_pct": pct(wins, len(closed)),
        "total_cost_usdc": round(sum(r.total_cost_usdc for r in rows), 8),
        "total_payout_usdc": round(sum(r.payout_usdc for r in closed), 8),
        "total_pnl_usdc": round(pnl, 8),
        "roi_pct_on_closed_cost": pct(pnl, closed_cost),
    }


def write_performance_reports(config: dict[str, Any], trades: list[PaperTrade]) -> None:
    """Write cycle-level and event-level performance CSV reports.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        trades (list[PaperTrade]): Collection of PaperTrade records.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Overwrites performance report CSV files when trade groups exist.
    """
    by_cycle: dict[str, list[PaperTrade]] = {}
    by_event: dict[str, list[PaperTrade]] = {}
    for trade in trades:
        by_cycle.setdefault(trade.cycle_id, []).append(trade)
        by_event.setdefault(f"{trade.event_date}|{trade.city}|{trade.kind}", []).append(trade)
    if by_cycle:
        write_csv(config["outputs"]["performance_by_cycle_csv"], [performance_row("cycle", key, rows) for key, rows in by_cycle.items()])
    if by_event:
        write_csv(config["outputs"]["performance_by_event_csv"], [performance_row("event", key, rows) for key, rows in by_event.items()])


def resolved_outcome_from_market(market: dict[str, Any]) -> str:
    """Extract a resolved YES/NO outcome from a closed market payload when available.
    
    Args:
        market (dict[str, Any]): Normalized TemperatureMarket object or raw market payload being evaluated.
    
    Returns:
        str: Resolved outcome label Yes/No, or empty string.
    """
    outcome = market.get("outcome") or market.get("winningOutcome") or market.get("resolution") or ""
    return outcome.title() if isinstance(outcome, str) and outcome.lower() in {"yes", "no"} else ""


def settle_open_trades(config: dict[str, Any]) -> list[PaperTrade]:
    """Check open trades against closed Polymarket markets and mark resolved trades as settled.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        list[PaperTrade]: Updated trade list after settlement checks.
    
    Side effects:
        Reads and writes trade CSVs and performance reports when settlements change.
    """
    trades = read_trades(config["outputs"]["trades_csv"])
    changed = False
    for trade in trades:
        if trade.status != "OPEN":
            continue
        try:
            event = fetch_event_by_url(config, trade.polymarket_url, trade.city, trade.kind, trade.event_date)
            if not event:
                continue
            for market in markets_for_event(config, event):
                if market.market_id != trade.market_id:
                    continue
                raw = parse_jsonish(market.raw_market_json, {})
                if not parse_bool(raw.get("closed")):
                    continue
                outcome = resolved_outcome_from_market(raw)
                if not outcome:
                    continue
                side = (trade.position_side or "YES").strip().lower()
                trade.status = "SETTLED"
                trade.settlement_source = "polymarket_closed_market"
                trade.winning_outcome = outcome
                trade.payout_usdc = round(trade.shares if outcome.lower() == side else 0.0, 8)
                trade.pnl_usdc = round(trade.payout_usdc - trade.total_cost_usdc, 8)
                changed = True
        except Exception:
            LOGGER.exception("settle failed trade=%s", trade.trade_id)
    if changed:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
    return trades


def open_trade_exists(trades: list[PaperTrade], strategy_name: str, market_id: str, side: str) -> bool:
    """Check whether an open trade already exists for a strategy, market, and side.
    
    Args:
        trades (list[PaperTrade]): Collection of PaperTrade records.
        strategy_name (str): Strategy identifier used to filter trades.
        market_id (str): Polymarket market identifier.
        side (str): Position side, expected to be YES or NO.
    
    Returns:
        bool: True when a matching open trade exists.
    """
    active_statuses = {"OPEN", "BUY_PENDING", "SELL_PENDING"}
    return any(t.status in active_statuses and t.strategy == strategy_name and t.market_id == market_id and (t.position_side or "YES").upper() == side.upper() for t in trades)


def open_no_trade_exists_for_event(trades: list[PaperTrade], strategy_name: str, city: str, kind: str, event_date: str) -> bool:
    """Check whether an event already has an active NO trade."""
    active_statuses = {"OPEN", "BUY_PENDING", "SELL_PENDING"}
    return any(
        t.status in active_statuses
        and t.strategy == strategy_name
        and t.city == city
        and t.kind == kind
        and t.event_date == event_date
        and (t.position_side or "YES").upper() == "NO"
        for t in trades
    )


def safe_float(value: Any, default: float = 0.0) -> float:
    """Parse a numeric value with a default fallback on invalid input.
    
    Args:
        value (Any): Raw value to parse or normalize.
        default (float): Fallback value returned when parsing fails.
    
    Returns:
        float: Parsed float or default fallback.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def polymarket_account_positions(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch all open Polymarket account positions for the configured user address.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        list[dict[str, Any]]: Position rows for the configured account.
    
    Side effects:
        Calls the Polymarket Data API and paginates through position rows.
    """
    user = configured_polymarket_user(config)
    if not user:
        LOGGER.info("polymarket position sync skipped: no account.polymarket_user_address configured")
        return []
    rows: list[dict[str, Any]] = []
    limit = 500
    for offset in range(0, 5000, limit):
        params = {"user": user, "limit": limit, "offset": offset}
        payload = polymarket_data_get(config, "/positions", params)
        batch = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(batch, list) or not batch:
            break
        rows.extend([row for row in batch if isinstance(row, dict)])
        if len(batch) < limit:
            break
    return rows


def weather_market_lookup(config: dict[str, Any]) -> tuple[dict[str, TemperatureMarket], dict[str, TemperatureMarket], dict[str, TemperatureMarket]]:
    """Build lookup maps from CLOB asset, condition id, and market id to weather markets.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        tuple[dict[str, TemperatureMarket], dict[str, TemperatureMarket], dict[str, TemperatureMarket]]: Lookup maps by asset id, condition id, and market id.
    """
    by_asset: dict[str, TemperatureMarket] = {}
    by_condition: dict[str, TemperatureMarket] = {}
    by_market: dict[str, TemperatureMarket] = {}
    for target in resolve_event_target_dates(config):
        for event in discover_temperature_events(config, target):
            for market in markets_for_event(config, event):
                by_market[market.market_id] = market
                if market.condition_id:
                    by_condition[market.condition_id.lower()] = market
                raw = parse_jsonish(market.raw_market_json, {})
                if isinstance(raw, dict):
                    for asset_id, _ in market_clob_tokens(raw):
                        by_asset[asset_id] = market
    return by_asset, by_condition, by_market


def position_asset_id(row: dict[str, Any]) -> str:
    """Extract an asset/token id from a Polymarket position row.
    
    Args:
        row (dict[str, Any]): Raw CSV, position, or observation row.
    
    Returns:
        str: Asset id string or empty string.
    """
    for key in ("asset", "asset_id", "assetId", "token_id", "tokenId", "clobTokenId", "clobTokenID"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def position_side(row: dict[str, Any]) -> str:
    """Extract and normalize the YES/NO side from a Polymarket position row.
    
    Args:
        row (dict[str, Any]): Raw CSV, position, or observation row.
    
    Returns:
        str: Normalized side YES or NO.
    """
    outcome = str(row.get("outcome") or row.get("side") or row.get("title") or "").strip().lower()
    if outcome in {"yes", "no"}:
        return outcome.upper()
    return "YES"


def position_size(row: dict[str, Any]) -> float:
    """Extract a positive position size from a Polymarket position row.
    
    Args:
        row (dict[str, Any]): Raw CSV, position, or observation row.
    
    Returns:
        float: Positive position size or 0.0.
    """
    for key in ("size", "balance", "shares", "quantity"):
        value = safe_float(row.get(key), 0.0)
        if value:
            return value
    return 0.0


def position_average_price(row: dict[str, Any]) -> float:
    """Extract or infer the average entry price from a Polymarket position row.
    
    Args:
        row (dict[str, Any]): Raw CSV, position, or observation row.
    
    Returns:
        float: Average price or inferred value/size price.
    """
    for key in ("avgPrice", "averagePrice", "avg_price", "price"):
        value = safe_float(row.get(key), 0.0)
        if value:
            return value
    size = position_size(row)
    for key in ("initialValue", "currentValue", "value"):
        value = safe_float(row.get(key), 0.0)
        if value and size:
            return value / size
    return 0.0


def raw_positions_snapshot_path(config: dict[str, Any]) -> str:
    """Resolve the JSON snapshot path for synced Polymarket positions.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        str: Path for the position snapshot JSON.
    """
    return str(config["outputs"].get("positions_json") or "polymarket_weather_positions.json")


def sync_polymarket_positions_to_disk(config: dict[str, Any], *, reason: str) -> list[PaperTrade]:
    """Sync configured Polymarket account positions into local paper-trade storage.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        reason (str): Machine-readable reason for creating, closing, or syncing a record.
    
    Returns:
        list[PaperTrade]: Current trades after optional account sync.
    
    Side effects:
        Writes position snapshots, trade CSVs, and performance reports when new positions are added.
    """
    if not config.get("account", {}).get("sync_positions_on_start", True) and reason == "start":
        return read_trades(config["outputs"]["trades_csv"])
    positions = polymarket_account_positions(config)
    if not positions:
        LOGGER.info("polymarket position sync %s: no remote positions, using local trades", reason)
        return read_trades(config["outputs"]["trades_csv"])

    with IO_LOCK, open(raw_positions_snapshot_path(config), "w", encoding="utf-8") as f:
        json.dump({"synced_at": datetime.now().isoformat(timespec="seconds"), "reason": reason, "positions": positions}, f, indent=2, ensure_ascii=False)

    by_asset, by_condition, by_market = weather_market_lookup(config)
    trades = read_trades(config["outputs"]["trades_csv"])
    existing = {(t.asset_id or "", t.market_id, (t.position_side or "YES").upper()) for t in trades if t.status == "OPEN"}
    strategy_name = str(config["trading"]["strategy_name"])
    added = 0
    for row in positions:
        size = position_size(row)
        if size <= 0:
            continue
        asset_id = position_asset_id(row)
        side = position_side(row)
        market_id = str(row.get("market") or row.get("marketId") or row.get("market_id") or "")
        condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
        market = by_asset.get(asset_id) or by_market.get(market_id) or by_condition.get(condition_id.lower())
        if not market:
            continue
        dedupe_key = (asset_id, market.market_id, side)
        if dedupe_key in existing or ("", market.market_id, side) in existing:
            continue
        price = position_average_price(row) or (market.yes_price if side == "YES" else market_no_price(market)) or 0.0
        now = datetime.now().isoformat(timespec="seconds")
        comparable_min, comparable_max, comparable_unit = comparable_rule_bounds(market, market.unit)
        total_cost = round(size * price, 8)
        trade = PaperTrade(
            trade_id=f"{now}:polymarket_account_sync:{side}:{market.market_id}:{asset_id or time.time_ns()}",
            created_at=now,
            cycle_id=f"{now}:polymarket_account_sync",
            strategy=strategy_name,
            event_id=market.event_id,
            market_id=market.market_id,
            condition_id=market.condition_id or condition_id,
            city=market.city,
            kind=market.kind,
            event_date=market.event_date,
            event_title=market.event_title,
            market_question=market.market_question,
            polymarket_url=market.polymarket_url,
            wunderground_source_url="",
            forecast_source="polymarket_account_position_sync_metar_unknown",
            forecast_observed_at=now,
            forecast_station="",
            forecast_temp=None,
            forecast_high=None,
            forecast_low=None,
            forecast_first_valid_time_local="",
            forecast_last_valid_time_local="",
            forecast_unit=market.unit,
            rule_min=market.rule_min,
            rule_max=market.rule_max,
            market_unit=market.unit,
            comparable_rule_min=comparable_min,
            comparable_rule_max=comparable_max,
            comparable_unit=comparable_unit,
            yes_price=price,
            mispricing_price_threshold=1.0,
            pricing_edge=0.0,
            notional_usdc=total_cost,
            shares=size,
            taker_fee_rate=float(config["trading"]["fee_rate"]),
            buy_fee_usdc=0.0,
            total_cost_usdc=total_cost,
            position_side=side,
            monitor_last_yes_price=market.yes_price,
            monitor_last_checked_at=now,
            monitor_price_trigger=float(config["trading"].get("monitor_price_change_pct", 0.03)),
            settlement_source="polymarket_account_sync",
            asset_id=asset_id or asset_id_for_market_side(market, side),
        )
        trades.append(trade)
        existing.add(dedupe_key)
        added += 1

    if added:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
    LOGGER.info("polymarket position sync %s complete remote_positions=%s added_weather_positions=%s", reason, len(positions), added)
    return trades


def persist_positions_before_stop(config: dict[str, Any], *, reason: str = "stop") -> None:
    """Best-effort position sync during shutdown when configured.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        reason (str): Machine-readable reason for creating, closing, or syncing a record.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        May call account sync during interpreter shutdown.
    """
    try:
        if config.get("account", {}).get("sync_positions_on_stop", True):
            sync_polymarket_positions_to_disk(config, reason=reason)
        else:
            trades = read_trades(config["outputs"]["trades_csv"])
            if trades:
                write_csv(config["outputs"]["trades_csv"], trades)
                write_csv(config["outputs"]["settled_trades_csv"], trades)
                write_performance_reports(config, trades)
        LOGGER.info("position persistence complete reason=%s", reason)
    except Exception:
        LOGGER.exception("position persistence failed reason=%s", reason)


def trade_age_seconds(trade: PaperTrade) -> float:
    """Compute the age of a trade from its created_at timestamp.
    
    Args:
        trade (PaperTrade): PaperTrade record being evaluated or mutated.
    
    Returns:
        float: Trade age in seconds.
    """
    try:
        created = datetime.fromisoformat(trade.created_at)
    except ValueError:
        return 0.0
    if created.tzinfo is None:
        created = created.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return max(0.0, (datetime.now(created.tzinfo) - created).total_seconds())


def verify_open_positions_with_twc(config: dict[str, Any], trigger_context: Optional[dict[str, Any]] = None) -> list[PaperTrade]:
    """Verify METAR-sourced open positions against TWC observations and sell inconsistent positions.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        trigger_context (Optional[dict[str, Any]]): Context from scheduler, WebSocket, or METAR supervisor that triggered strategy processing.
    
    Returns:
        list[PaperTrade]: Updated trade list after verification.
    
    Side effects:
        May close trades and rewrite trade/performance CSV files.
    """
    trades = read_trades(config["outputs"]["trades_csv"])
    strategy_name = str(config["trading"]["strategy_name"])
    verify_seconds = int(config["trading"].get("twc_post_entry_verify_seconds", 7200))
    trigger_context = trigger_context or {}
    trigger_market_id = str(trigger_context.get("market_id") or "")
    trigger_trade_id = str(trigger_context.get("trade_id") or "")
    trigger_station = str(trigger_context.get("station") or "").upper()
    trigger_event_date = str(trigger_context.get("event_date") or "")
    trigger_city = str(trigger_context.get("city") or "")
    twc_cache: dict[tuple[str, str, str, str], tuple[Optional[float], Optional[float], list[tuple[datetime, float]], dict[str, Any]]] = {}
    changed = False

    for trade in trades:
        if trade.status not in {"OPEN", "HEDGED"} or trade.strategy != strategy_name:
            continue
        if trigger_trade_id and trade.trade_id != trigger_trade_id:
            continue
        if trigger_event_date and trade.event_date != trigger_event_date:
            continue
        if trigger_city and trade.city != trigger_city:
            continue
        source_text = (trade.forecast_source or "").lower()
        if not any(token in source_text for token in ("metar", "weatherrecord", "tgftp")):
            continue
        try:
            event = fetch_event_by_url(config, trade.polymarket_url, trade.city, trade.kind, trade.event_date)
            if not event:
                continue
            markets = markets_for_event(config, event)
            market = next((m for m in markets if m.market_id == trade.market_id), None)
            if not market:
                continue
            event_unit = event_market_unit(markets)
            station = trade.forecast_station or station_for_event(config, trade.city, trade.polymarket_url)
            if not station:
                continue
            station = station.upper()
            if trigger_station and station != trigger_station:
                continue
            city_local_dt, _, _ = city_local_now(config, trade.city)
            cache_key = (station, trade.event_date, event_unit.upper(), city_local_dt.date().isoformat())
            if cache_key not in twc_cache:
                twc_cache[cache_key] = deterministic_observed_extremes_from_twc(config, station, trade.event_date, city_local_dt, event_unit)
            twc_high, twc_low, _, _ = twc_cache[cache_key]
            if (trade.kind == "Highest" and twc_high is None) or (trade.kind == "Lowest" and twc_low is None):
                LOGGER.info(
                    "twc verification skip no local observations trade=%s city=%s kind=%s event_date=%s city_local=%s",
                    trade.trade_id,
                    trade.city,
                    trade.kind,
                    trade.event_date,
                    city_local_dt.isoformat(timespec="seconds"),
                )
                continue
            if trade.status == "HEDGED":
                if unwind_yes_hedge_if_no_impossible(
                    config,
                    trade,
                    market,
                    twc_high,
                    twc_low,
                    event_unit,
                    "twc_no_impossible_unwind_yes_hedge",
                ):
                    changed = True
                    LOGGER.info(
                        "twc verification unwind hedge trade=%s city=%s kind=%s market=%s twc_high=%s twc_low=%s",
                        trade.trade_id,
                        trade.city,
                        trade.kind,
                        trade.market_id,
                        twc_high,
                        twc_low,
                    )
                continue
            if trade_observation_verified(trade, market, twc_high, twc_low, event_unit):
                if trade.settlement_source != "twc_verified_hold":
                    trade.settlement_source = "twc_verified_hold"
                    trade.error = ""
                    changed = True
                continue

            triggered_by_position_price = bool(trigger_market_id and trigger_market_id == trade.market_id)
            price_momentum_trade = "price_momentum" in (trade.forecast_source or "").lower()
            post_buy_validation_trade = any(token in source_text for token in ("weatherrecord", "tgftp", "metar"))
            staggered_validation = str(trigger_context.get("source") or "") == "staggered_twc_position_verification"
            expired = trade_age_seconds(trade) >= verify_seconds
            if triggered_by_position_price or expired or price_momentum_trade or (staggered_validation and post_buy_validation_trade):
                reason = (
                    "twc_invalidated_price_momentum" if price_momentum_trade
                    else "twc_inconsistent_after_position_price_move" if triggered_by_position_price
                    else "twc_invalidated_post_buy_validation" if staggered_validation
                    else "twc_not_verified_within_2h"
                )
                if close_trade(config, trade, market, reason):
                    changed = True
                    LOGGER.info(
                        "twc verification sell trade=%s city=%s kind=%s market=%s reason=%s twc_high=%s twc_low=%s age_seconds=%.0f",
                        trade.trade_id,
                        trade.city,
                        trade.kind,
                        trade.market_id,
                        reason,
                        twc_high,
                        twc_low,
                        trade_age_seconds(trade),
                    )
        except Exception:
            LOGGER.exception("twc position verification failed trade=%s", trade.trade_id)

    if changed:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
    return trades


def process_deterministic_harvest(config: dict[str, Any], trigger_context: dict[str, Any]) -> list[PaperTrade]:
    """Run the deterministic harvest strategy for one event trigger using aviation observations.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        trigger_context (dict[str, Any]): Context from scheduler, WebSocket, or METAR supervisor that triggered strategy processing.
    
    Returns:
        list[PaperTrade]: Updated trade list after deterministic strategy processing.
    
    Side effects:
        May append new trades, close NO positions, update EXTREMES_BY_EVENT, and rewrite reports.
    """
    city = str(trigger_context.get("city") or "")
    kind = str(trigger_context.get("kind") or "")
    event_date = str(trigger_context.get("event_date") or "")
    event_url = str(trigger_context.get("polymarket_url") or "")
    if not city or not kind or not event_date or not event_url:
        return []
    event = fetch_event_by_url(config, event_url, city, kind, event_date)
    if not event:
        return []
    markets = markets_for_event(config, event)
    event_unit = event_market_unit(markets)
    station = station_for_event(config, city, event_url)
    if not station:
        LOGGER.warning("deterministic skip missing station city=%s kind=%s url=%s", city, kind, event_url)
        return []
    wu_source = ""
    if not configured_station_for_city(config, city):
        try:
            wu_source = extract_wunderground_source(config, event_url)
        except requests.RequestException as exc:
            LOGGER.warning("deterministic source url unavailable city=%s kind=%s url=%s error=%s", city, kind, event_url, exc)
    signal_source = str(trigger_context.get("source") or "aviation_metar")
    observed_high = trigger_context.get("aviation_high")
    observed_low = trigger_context.get("aviation_low")
    validation_min_obs_utc = str(trigger_context.get("observed_at_utc") or "")
    if observed_high is None and observed_low is None:
        observed_high, observed_low, _, _ = aviation_observed_extremes(config, station, city, event_date, event_unit)
    observed_high = float(observed_high) if observed_high is not None else None
    observed_low = float(observed_low) if observed_low is not None else None
    if (kind == "Highest" and observed_high is None) or (kind == "Lowest" and observed_low is None):
        LOGGER.info(
            "deterministic skip no local aviation observations city=%s kind=%s event_date=%s station=%s",
            city,
            kind,
            event_date,
            station,
        )
        return []
    EXTREMES_BY_EVENT[(event_date, city, kind)] = {
        "source": signal_source,
        "observed_high": observed_high,
        "observed_low": observed_low,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }

    trades = read_trades(config["outputs"]["trades_csv"])
    strategy_name = str(config["trading"]["strategy_name"])
    markets_by_id = {m.market_id: m for m in markets}
    changed = False
    new_trades: list[PaperTrade] = []
    post_buy_validation_requests: list[tuple[str, str, str]] = []

    for trade in trades:
        if trade.status != "OPEN" or trade.strategy != strategy_name or trade.city != city or trade.kind != kind or trade.event_date != event_date:
            continue
        if (trade.position_side or "YES").upper() != "NO":
            continue
        market = markets_by_id.get(trade.market_id)
        if market and deterministic_no_possible_again(market, kind, observed_high, observed_low, event_unit):
            if close_trade(config, trade, market, "metar_correction_no_possible_again"):
                changed = True
                LOGGER.info("metar correction sell_no trade=%s city=%s kind=%s market=%s", trade.trade_id, city, kind, market.market_id)

    ordered = deterministic_ordered_markets(markets, event_unit, kind)
    extreme_market = ordered[-1] if ordered else None
    extreme_reached = bool(extreme_market and deterministic_extreme_market_reached(extreme_market, kind, observed_high, observed_low, event_unit))
    no_max = float(config["trading"].get("deterministic_no_max_price", 0.99))
    yes_max = float(config["trading"].get("deterministic_yes_max_price", 0.99))
    cycle_id = datetime.now().strftime("%Y%m%dT%H%M%S") + ":deterministic_harvest"
    live_trader = get_live_trader() if live_trading_enabled(config) else None

    if extreme_market and extreme_reached and not open_trade_exists(trades + new_trades, strategy_name, extreme_market.market_id, "YES"):
        price = best_buy_price(config, extreme_market, "YES")
        if price is None or price <= 0:
            LOGGER.info(
                "metar skip_buy_yes_no_clob_buy_price city=%s kind=%s market=%s observed_high=%s observed_low=%s question=%r",
                city,
                kind,
                extreme_market.market_id,
                observed_high,
                observed_low,
                extreme_market.market_question,
            )
        elif price <= yes_max:
            trade = (
                live_trader.submit_buy_trade(config, cycle_id, extreme_market, wu_source, station, "YES", price, observed_high, observed_low, f"{signal_source}_extreme_yes_reached")
                if live_trader
                else make_trade(config, cycle_id, extreme_market, wu_source, station, "YES", price, observed_high, observed_low, f"{signal_source}_extreme_yes_reached")
            )
            if trade:
                new_trades.append(trade)
                changed = True
                if not live_trader:
                    notify_trade(config, trade, "BUY", "FILLED", f"{signal_source}_extreme_yes_reached")
                    post_buy_validation_requests.append((trade.trade_id, station, validation_min_obs_utc))
                LOGGER.info("metar buy_yes city=%s kind=%s market=%s price=%s observed_high=%s observed_low=%s status=%s", city, kind, extreme_market.market_id, price, observed_high, observed_low, trade.status)
        else:
            LOGGER.info("metar skip_buy_yes_price_too_high city=%s kind=%s market=%s yes_clob_buy_price=%s max=%s observed_high=%s observed_low=%s", city, kind, extreme_market.market_id, price, yes_max, observed_high, observed_low)

    if not extreme_reached:
        impossible_markets = deterministic_impossible_markets_by_proximity(markets, kind, observed_high, observed_low, event_unit)
        market = next((candidate for candidate in impossible_markets if not open_trade_exists(trades + new_trades, strategy_name, candidate.market_id, "NO")), None)
        if market:
            no_price = best_buy_price(config, market, "NO")
            if no_price is None or no_price <= 0:
                LOGGER.info(
                    "metar skip_buy_no_no_clob_buy_price city=%s kind=%s market=%s observed_high=%s observed_low=%s question=%r",
                    city,
                    kind,
                    market.market_id,
                    observed_high,
                    observed_low,
                    market.market_question,
                )
            elif no_price > no_max:
                LOGGER.info(
                    "metar skip_buy_no_price_too_high city=%s kind=%s market=%s no_clob_buy_price=%s max=%s observed_high=%s observed_low=%s question=%r",
                    city,
                    kind,
                    market.market_id,
                    no_price,
                    no_max,
                    observed_high,
                    observed_low,
                    market.market_question,
                )
            else:
                trade = (
                    live_trader.submit_buy_trade(config, cycle_id, market, wu_source, station, "NO", no_price, observed_high, observed_low, f"{signal_source}_impossible_no")
                    if live_trader
                    else make_trade(config, cycle_id, market, wu_source, station, "NO", no_price, observed_high, observed_low, f"{signal_source}_impossible_no")
                )
                if trade:
                    new_trades.append(trade)
                    changed = True
                    if not live_trader:
                        notify_trade(config, trade, "BUY", "FILLED", f"{signal_source}_impossible_no")
                        post_buy_validation_requests.append((trade.trade_id, station, validation_min_obs_utc))
                    LOGGER.info("metar buy_no city=%s kind=%s market=%s no_price=%s observed_high=%s observed_low=%s status=%s", city, kind, market.market_id, no_price, observed_high, observed_low, trade.status)
        else:
            LOGGER.info("metar skip_buy_no_no_impossible_candidate city=%s kind=%s observed_high=%s observed_low=%s", city, kind, observed_high, observed_low)

    if new_trades:
        trades.extend(new_trades)
    if changed:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
    if signal_source != "tgftp_metar":
        for trade_id, validation_station, min_obs_utc in post_buy_validation_requests:
            start_tgftp_validation_thread(config, trade_id, validation_station, min_obs_utc)
    return trades


def process_price_momentum_buy(config: dict[str, Any], context: dict[str, Any]) -> Optional[PaperTrade]:
    """Buy a fast-rising token inside the expected METAR report window.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including price momentum settings.
        context (dict[str, Any]): WebSocket trigger context for a temperature market.
    
    Returns:
        Optional[PaperTrade]: Created trade row, or None when skipped.
    """
    settings = config.get("price_momentum", {})
    if not settings.get("enabled", True):
        return None
    side = str(context.get("position_side") or "").upper()
    if str(context.get("role") or "") != "temperature_market" or side not in {"YES", "NO"}:
        return None
    price = float(context.get("price") or 0.0)
    previous_price = float(context.get("previous_price") or 0.0)
    price_field = str(context.get("price_field") or "price")
    if price <= previous_price or previous_price <= 0:
        return None
    max_price = float(settings.get("yes_max_price" if side == "YES" else "no_max_price", 1.0))
    if price <= 0 or price > max_price:
        LOGGER.info("price momentum skip price city=%s market=%s side=%s price=%s max=%s", context.get("city"), context.get("market_id"), side, price, max_price)
        return None

    city = str(context.get("city") or "")
    kind = str(context.get("kind") or "")
    event_date = str(context.get("event_date") or "")
    event_url = str(context.get("polymarket_url") or "")
    station = str(context.get("station") or "")
    market_id = str(context.get("market_id") or "")
    if not city or not kind or not event_date or not event_url or not station or not market_id:
        return None
    in_window, timing = in_station_report_window(config, city, station)
    asset_id = str(context.get("asset_id") or market_id)
    momentum_key = str(context.get("momentum_key") or price_momentum_window_key(asset_id, price_field))
    expected_next_obs_utc = str(timing.get("expected_next_obs_utc") or "")
    if not in_window:
        if momentum_key:
            PRICE_MOMENTUM_WINDOWS.pop(momentum_key, None)
        if context.get("log_skip"):
            LOGGER.info(
                "price momentum skip outside_report_window city=%s station=%s market=%s price_field=%s expected_next_obs_utc=%s",
                city,
                station,
                market_id,
                price_field,
                expected_next_obs_utc,
            )
        return None
    window = PRICE_MOMENTUM_WINDOWS.get(momentum_key)
    if not window or window.get("expected_next_obs_utc") != expected_next_obs_utc:
        window = {
            "expected_next_obs_utc": expected_next_obs_utc,
            "base_price": previous_price,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "price_field": price_field,
        }
        PRICE_MOMENTUM_WINDOWS[momentum_key] = window
    base_price = float(window.get("base_price") or previous_price)
    move_fraction = float(context.get("move_fraction", settings.get("move_to_one_fraction", settings.get("yes_change_pct" if side == "YES" else "no_change_pct", 0.30))))
    target_price = momentum_target_price(base_price, move_fraction)
    if price < target_price:
        LOGGER.info(
            "price momentum skip below_target city=%s station=%s market=%s side=%s price_field=%s base_price=%s price=%s target_price=%s move_to_one_fraction=%s expected_next_obs_utc=%s",
            city,
            station,
            market_id,
            side,
            price_field,
            base_price,
            price,
            target_price,
            move_fraction,
            expected_next_obs_utc,
        )
        return None

    event = fetch_event_by_url(config, event_url, city, kind, event_date)
    if not event:
        return None
    markets = markets_for_event(config, event)
    market = next((m for m in markets if m.market_id == market_id), None)
    if not market:
        return None
    if side == "YES":
        boundary = outer_boundary_market(markets, event_market_unit(markets), kind)
        if not boundary or boundary.market_id != market.market_id:
            LOGGER.info(
                "price momentum skip yes_not_outer_boundary city=%s kind=%s market=%s boundary_market=%s move_to_one_fraction=%s question=%r",
                city,
                kind,
                market.market_id,
                boundary.market_id if boundary else "",
                move_fraction,
                market.market_question,
            )
            return None
    trades = read_trades(config["outputs"]["trades_csv"])
    strategy_name = str(config["trading"]["strategy_name"])
    if open_trade_exists(trades, strategy_name, market.market_id, side):
        return None

    reason = "metar_price_momentum_boundary_yes" if side == "YES" else "metar_price_momentum_no"
    cycle_id = datetime.now().strftime("%Y%m%dT%H%M%S") + f":price_momentum_{side.lower()}"
    live_trader = get_live_trader() if live_trading_enabled(config) else None
    buy_price = price
    if live_trader:
        executable_price = best_buy_price(config, market, side)
        if executable_price is None or executable_price <= 0:
            LOGGER.info(
                "price momentum skip no_executable_price city=%s station=%s market=%s side=%s signal_price=%s price_field=%s",
                city,
                station,
                market.market_id,
                side,
                price,
                price_field,
            )
            return None
        if executable_price > max_price:
            LOGGER.info(
                "price momentum skip executable_price_too_high city=%s station=%s market=%s side=%s signal_price=%s executable_price=%s max=%s price_field=%s",
                city,
                station,
                market.market_id,
                side,
                price,
                executable_price,
                max_price,
                price_field,
            )
            return None
        buy_price = executable_price
    trade = (
        live_trader.submit_buy_trade(config, cycle_id, market, "", station, side, buy_price, None, None, reason)
        if live_trader
        else make_trade(config, cycle_id, market, "", station, side, buy_price, None, None, reason)
    )
    if not trade:
        return None
    if not live_trader:
        notify_trade(config, trade, "BUY", "FILLED", reason)
    trades.append(trade)
    write_csv(config["outputs"]["trades_csv"], trades)
    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)
    LOGGER.info(
        "price momentum buy city=%s station=%s market=%s side=%s price_field=%s base_price=%s signal_price=%s buy_price=%s target_price=%s move_to_one_fraction=%s expected_next_obs_utc=%s status=%s",
        city,
        station,
        market.market_id,
        side,
        price_field,
        base_price,
        price,
        buy_price,
        target_price,
        move_fraction,
        expected_next_obs_utc,
        trade.status,
    )
    start_tgftp_validation_thread(config, trade.trade_id, station, str(timing.get("expected_next_obs_utc") or ""))
    return trade


def process_price_momentum_no_buy(config: dict[str, Any], context: dict[str, Any]) -> Optional[PaperTrade]:
    """Backward-compatible wrapper for the generalized price momentum strategy."""
    return process_price_momentum_buy(config, context)


def start_tgftp_validation_thread(config: dict[str, Any], trade_id: str, station: str, min_obs_utc: str) -> None:
    """Start one TGFTP validation worker for a just-created price momentum trade."""
    if not trade_id or trade_id in TGFTP_VALIDATION_THREADS:
        return
    TGFTP_VALIDATION_THREADS.add(trade_id)
    thread = threading.Thread(target=tgftp_validation_worker, args=(config, trade_id, station, min_obs_utc), name=f"tgftp-verify-{trade_id[:24]}", daemon=True)
    thread.start()


def parse_optional_utc_datetime(value: Any) -> Optional[datetime]:
    """Parse an optional datetime string/value into UTC."""
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def tgftp_validation_worker(config: dict[str, Any], trade_id: str, station: str, min_obs_utc: str) -> None:
    """Poll TGFTP until a new station report validates or rejects a price momentum trade."""
    settings = config.get("price_momentum", {})
    interval = max(1, int(settings.get("tgftp_verify_interval_seconds", 10)))
    timeout = max(interval, int(settings.get("tgftp_verify_timeout_seconds", 180)))
    station_key = station.upper()
    min_obs_dt = parse_optional_utc_datetime(min_obs_utc)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() <= deadline:
            try:
                obs = tgftp_metar_observation(station)
            except Exception:
                LOGGER.exception("tgftp validation fetch failed trade=%s station=%s", trade_id, station)
                time.sleep(interval)
                continue
            if not obs:
                time.sleep(interval)
                continue
            previous = cached_tgftp_observation(station_key)
            if previous is None:
                obs_dt = obs["obs_dt"].astimezone(timezone.utc)
                if min_obs_dt is None or obs_dt >= min_obs_dt:
                    update_cached_tgftp_observation(station_key, obs)
                    LOGGER.info(
                        "tgftp validation first usable observation trade=%s station=%s obs_utc=%s temp_c=%s min_obs_utc=%s",
                        trade_id,
                        station_key,
                        obs_dt.isoformat(),
                        obs["temp_c"],
                        min_obs_dt.isoformat() if min_obs_dt else "",
                    )
                    validate_trade_with_tgftp_observation(config, trade_id, station, obs)
                    return
                update_cached_tgftp_observation(station_key, obs)
                LOGGER.info(
                    "tgftp validation baseline initialized trade=%s station=%s obs_utc=%s temp_c=%s",
                    trade_id,
                    station_key,
                    obs["obs_dt"].astimezone(timezone.utc).isoformat(),
                    obs["temp_c"],
                )
                time.sleep(interval)
                continue
            if not tgftp_observation_changed(previous, obs):
                update_cached_tgftp_observation(station_key, obs)
                LOGGER.info(
                    "tgftp validation wait unchanged trade=%s station=%s obs_utc=%s temp_c=%s",
                    trade_id,
                    station_key,
                    obs["obs_dt"].astimezone(timezone.utc).isoformat(),
                    obs["temp_c"],
                )
                time.sleep(interval)
                continue
            update_cached_tgftp_observation(station_key, obs)
            LOGGER.info(
                "tgftp validation new observation trade=%s station=%s obs_utc=%s temp_c=%s previous_obs_utc=%s previous_temp_c=%s",
                trade_id,
                station_key,
                obs["obs_dt"].astimezone(timezone.utc).isoformat(),
                obs["temp_c"],
                previous["obs_dt"].astimezone(timezone.utc).isoformat() if previous and isinstance(previous.get("obs_dt"), datetime) else "",
                previous.get("temp_c") if previous else "",
            )
            validate_trade_with_tgftp_observation(config, trade_id, station, obs)
            return
        LOGGER.warning("tgftp validation timeout trade=%s station=%s", trade_id, station_key)
    finally:
        TGFTP_VALIDATION_THREADS.discard(trade_id)


def validate_trade_with_tgftp_observation(config: dict[str, Any], trade_id: str, station: str, obs: dict[str, Any]) -> None:
    """Apply one latest TGFTP METAR observation to an open price momentum trade."""
    trades = read_trades(config["outputs"]["trades_csv"])
    trade = next((t for t in trades if t.trade_id == trade_id), None)
    if not trade:
        return
    event = fetch_event_by_url(config, trade.polymarket_url, trade.city, trade.kind, trade.event_date)
    if not event:
        return
    markets = markets_for_event(config, event)
    market = next((m for m in markets if m.market_id == trade.market_id), None)
    if not market:
        return
    event_unit = event_market_unit(markets)
    temp_f = convert_temperature(float(obs["temp_c"]), "C", "F")
    temp = convert_temperature(temp_f, "F", event_unit) if event_unit.upper() != "F" else temp_f
    if temp is None:
        return
    rounded_temp = round(float(temp))
    key = (trade.event_date, trade.city, trade.kind)
    previous = EXTREMES_BY_EVENT.get(key, {})
    observed_high = previous.get("observed_high")
    observed_low = previous.get("observed_low")
    if trade.kind == "Highest":
        observed_high = max(float(observed_high), rounded_temp) if observed_high is not None else rounded_temp
    else:
        observed_low = min(float(observed_low), rounded_temp) if observed_low is not None else rounded_temp
    EXTREMES_BY_EVENT[key] = {
        "source": "tgftp_metar",
        "observed_high": observed_high,
        "observed_low": observed_low,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "obs_utc": obs["obs_dt"].astimezone(timezone.utc).isoformat(),
        "raw_ob": obs["raw_ob"],
    }
    trade.forecast_high = float(observed_high) if observed_high is not None else trade.forecast_high
    trade.forecast_low = float(observed_low) if observed_low is not None else trade.forecast_low
    trade.forecast_temp = float(observed_high) if trade.kind == "Highest" and observed_high is not None else float(observed_low) if trade.kind == "Lowest" and observed_low is not None else trade.forecast_temp
    trade.forecast_observed_at = obs["obs_dt"].astimezone(timezone.utc).isoformat()
    if trade.status == "HEDGED":
        if unwind_yes_hedge_if_no_impossible(
            config,
            trade,
            market,
            observed_high,
            observed_low,
            event_unit,
            "tgftp_no_impossible_unwind_yes_hedge",
        ):
            LOGGER.info("tgftp validation unwind hedge trade=%s station=%s obs_utc=%s temp=%s high=%s low=%s raw=%r", trade_id, station, obs["obs_dt"].isoformat(), rounded_temp, observed_high, observed_low, obs["raw_ob"])
        else:
            trade.error = ""
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
        return
    if trade.status != "OPEN":
        trade.error = f"tgftp validated while trade status={trade.status}; raw={obs['raw_ob']}"
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        return
    if trade_observation_verified(trade, market, observed_high, observed_low, event_unit):
        trade.settlement_source = "tgftp_verified_hold"
        trade.error = ""
        LOGGER.info("tgftp validation hold trade=%s station=%s obs_utc=%s temp=%s high=%s low=%s raw=%r", trade_id, station, obs["obs_dt"].isoformat(), rounded_temp, observed_high, observed_low, obs["raw_ob"])
    else:
        if close_trade(config, trade, market, "tgftp_invalidated_post_buy_validation"):
            LOGGER.info("tgftp validation sell trade=%s station=%s obs_utc=%s temp=%s high=%s low=%s raw=%r", trade_id, station, obs["obs_dt"].isoformat(), rounded_temp, observed_high, observed_low, obs["raw_ob"])
    write_csv(config["outputs"]["trades_csv"], trades)
    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)


def tgftp_extremes_for_event(
    config: dict[str, Any],
    city: str,
    station: str,
    kind: str,
    event_date: str,
    event_unit: str,
    obs: dict[str, Any],
) -> tuple[Optional[float], Optional[float], Optional[datetime]]:
    """Merge one TGFTP observation into current event high/low state."""
    obs_dt = obs.get("obs_dt")
    if not isinstance(obs_dt, datetime):
        return None, None, None
    local_dt = obs_dt.astimezone(city_timezone(config, city))
    if local_dt.date().isoformat() != event_date:
        return None, None, obs_dt
    temp_f = convert_temperature(float(obs["temp_c"]), "C", "F")
    temp = convert_temperature(temp_f, "F", event_unit) if event_unit.upper() != "F" else temp_f
    if temp is None:
        return None, None, obs_dt
    rounded_temp = round(float(temp))
    key = (event_date, city, kind)
    previous = EXTREMES_BY_EVENT.get(key, {})
    observed_high = previous.get("observed_high")
    observed_low = previous.get("observed_low")
    if kind == "Highest":
        observed_high = max(float(observed_high), rounded_temp) if observed_high is not None else rounded_temp
    else:
        observed_low = min(float(observed_low), rounded_temp) if observed_low is not None else rounded_temp
    EXTREMES_BY_EVENT[key] = {
        "source": "tgftp_metar",
        "observed_high": observed_high,
        "observed_low": observed_low,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "obs_utc": obs_dt.astimezone(timezone.utc).isoformat(),
        "raw_ob": obs.get("raw_ob", ""),
    }
    return (
        float(observed_high) if observed_high is not None else None,
        float(observed_low) if observed_low is not None else None,
        obs_dt,
    )


def process_tgftp_window_observation(config: dict[str, Any], group: dict[str, Any], timing: dict[str, Any], obs: dict[str, Any]) -> None:
    """Use a new TGFTP observation to validate existing buys and attempt deterministic buys."""
    city = str(group["city"])
    station = str(group["station"])
    obs_dt = obs["obs_dt"].astimezone(timezone.utc)
    update_cached_tgftp_observation(station, obs)
    LOGGER.info(
        "tgftp window observation city=%s station=%s obs_utc=%s temp_c=%s raw=%r",
        city,
        station,
        obs_dt.isoformat(),
        obs.get("temp_c"),
        obs.get("raw_ob", ""),
    )
    trades = read_trades(config["outputs"]["trades_csv"])
    active_event_dates = {str(event.get("_parsed_event_date") or "") for event in group.get("events", [])}
    for trade in trades:
        if (
            trade.status in {"OPEN", "HEDGED"}
            and trade.city == city
            and trade.event_date in active_event_dates
            and (trade.forecast_station or "").upper() == station.upper()
        ):
            validate_trade_with_tgftp_observation(config, trade.trade_id, station, obs)

    for event in list(group["events"]):
        kind = event["_parsed_kind"]
        event_date = event["_parsed_event_date"]
        event_url = poly_url_from_event(event)
        try:
            markets = markets_for_event(config, event)
            event_unit = event_market_unit(markets)
            observed_high, observed_low, latest_dt = tgftp_extremes_for_event(config, city, station, kind, event_date, event_unit, obs)
            if latest_dt is None or (kind == "Highest" and observed_high is None) or (kind == "Lowest" and observed_low is None):
                continue
            process_deterministic_harvest(
                config,
                {
                    "source": "tgftp_metar",
                    "city": city,
                    "kind": kind,
                    "event_date": event_date,
                    "polymarket_url": event_url,
                    "aviation_high": observed_high,
                    "aviation_low": observed_low,
                    "observed_at_utc": latest_dt.astimezone(timezone.utc).isoformat(),
                },
            )
        except Exception:
            LOGGER.exception("tgftp window event failed city=%s kind=%s station=%s", city, kind, station)


def market_clob_tokens(market_json: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract CLOB token ids and outcomes from a market payload.
    
    Args:
        market_json (dict[str, Any]): Raw market payload containing outcome and CLOB token metadata.
    
    Returns:
        list[tuple[str, str]]: List of token id and outcome pairs.
    """
    outcomes = parse_jsonish(market_json.get("outcomes"), [])
    token_ids = parse_jsonish(market_json.get("clobTokenIds") or market_json.get("clobTokenIDs"), [])
    rows: list[tuple[str, str]] = []
    for idx, token in enumerate(token_ids):
        outcome = str(outcomes[idx]) if idx < len(outcomes) else ""
        if token:
            rows.append((str(token), outcome))
    return rows


def websocket_message_prices(message: Any) -> list[tuple[str, float, str]]:
    """Extract asset price updates from Polymarket WebSocket message shapes.
    
    Args:
        message (Any): Raw or decoded WebSocket message payload.
    
    Returns:
        list[tuple[str, float, str]]: Extracted asset id, price, and source field tuples.
    """
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            return []
    rows: list[tuple[str, float, str]] = []

    def add_price(asset: str, value: Any, field_name: str) -> None:
        if not asset or value in (None, ""):
            return
        try:
            price = float(value)
        except (TypeError, ValueError):
            return
        rows.append((asset, price, field_name))

    def best_book_price(levels: Any, bid: bool) -> Optional[float]:
        prices: list[float] = []
        if not isinstance(levels, list):
            return None
        for level in levels:
            if not isinstance(level, dict):
                continue
            raw_price = level.get("price")
            if raw_price in (None, ""):
                continue
            try:
                prices.append(float(raw_price))
            except (TypeError, ValueError):
                continue
        if not prices:
            return None
        return max(prices) if bid else min(prices)

    def walk(node: Any) -> None:
        if isinstance(node, str):
            try:
                node = json.loads(node)
            except json.JSONDecodeError:
                return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        asset = str(node.get("asset_id") or node.get("assetId") or node.get("token_id") or node.get("tokenId") or node.get("asset") or "")
        event_type = str(node.get("event_type") or node.get("eventType") or "").strip()
        if event_type == "last_trade_price":
            add_price(asset, node.get("price"), "last_price")
        elif event_type == "book":
            add_price(asset, best_book_price(node.get("bids"), bid=True), "best_bid")
            add_price(asset, best_book_price(node.get("asks"), bid=False), "best_ask")
        else:
            add_price(asset, node.get("best_bid"), "best_bid")
            add_price(asset, node.get("best_ask"), "best_ask")
            if "last_price" in node:
                add_price(asset, node.get("last_price"), "last_price")
        for value in node.values():
            if isinstance(value, (dict, list)):
                walk(value)

    walk(message)
    return rows


def websocket_assets(
    config: dict[str, Any],
    only_city: str = "",
    only_kind: str = "",
    only_event_date: str = "",
) -> dict[str, dict[str, Any]]:
    """Discover all WebSocket asset ids to subscribe for current target weather events and open positions.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        dict[str, dict[str, Any]]: Asset metadata keyed by CLOB token id.
    """
    trades = read_trades(config["outputs"]["trades_csv"])
    assets: dict[str, dict[str, Any]] = {}
    observed_cache: dict[tuple[str, str, str, str], tuple[Optional[float], Optional[float]]] = {}
    strategy_name = str(config["trading"]["strategy_name"])
    open_trades = [t for t in trades if t.status == "OPEN" and t.strategy == strategy_name]
    for target in resolve_event_target_dates(config):
        for event in discover_temperature_events(config, target):
            city = event["_parsed_city"]
            kind = event["_parsed_kind"]
            event_date = event["_parsed_event_date"]
            if only_city and city != only_city:
                continue
            if only_kind and kind != only_kind:
                continue
            if only_event_date and event_date != only_event_date:
                continue
            event_url = poly_url_from_event(event)
            try:
                station = station_for_event(config, city, event_url)
                markets = markets_for_event(config, event)
                event_unit = event_market_unit(markets)
                observed_high: Optional[float] = None
                observed_low: Optional[float] = None
                if station:
                    cache_key = (station, city, event_date, event_unit)
                    if cache_key not in observed_cache:
                        try:
                            observed_high, observed_low, _, _ = aviation_observed_extremes(config, station, city, event_date, event_unit)
                        except Exception:
                            LOGGER.exception("websocket asset filter aviation fallback all city=%s kind=%s station=%s event_date=%s", city, kind, station, event_date)
                            observed_high, observed_low = None, None
                        observed_cache[cache_key] = (observed_high, observed_low)
                        LOGGER.info(
                            "websocket asset filter observed extremes city=%s kind=%s station=%s event_date=%s high=%s low=%s unit=%s markets_before=%s",
                            city,
                            kind,
                            station,
                            event_date,
                            observed_high,
                            observed_low,
                            event_unit,
                            len(markets),
                        )
                    observed_high, observed_low = observed_cache[cache_key]
                relevant_markets = websocket_relevant_markets_for_observed_extreme(markets, kind, event_unit, observed_high, observed_low)
                if len(relevant_markets) != len(markets):
                    LOGGER.info(
                        "websocket asset filter applied city=%s kind=%s station=%s event_date=%s high=%s low=%s markets_before=%s markets_after=%s",
                        city,
                        kind,
                        station,
                        event_date,
                        observed_high,
                        observed_low,
                        len(markets),
                        len(relevant_markets),
                    )
                markets_by_id = {m.market_id: m for m in markets}
                for trade in open_trades:
                    if trade.city != city or trade.kind != kind or trade.event_date != event_date:
                        continue
                    held_market = markets_by_id.get(trade.market_id)
                    if not held_market:
                        continue
                    if trade.asset_id:
                        current_price = held_market.yes_price if (trade.position_side or "YES").upper() == "YES" else market_no_price(held_market)
                        assets[trade.asset_id] = {
                            "asset_id": trade.asset_id,
                            "role": "held_position",
                            "trade_id": trade.trade_id,
                            "position_side": trade.position_side,
                            "market_id": held_market.market_id,
                            "market_question": held_market.market_question,
                            "rule_min": held_market.rule_min,
                            "rule_max": held_market.rule_max,
                            "market_unit": held_market.unit,
                            "city": city,
                            "kind": kind,
                            "event_date": event_date,
                            "polymarket_url": event_url,
                            "station": station,
                            "last_price": current_price,
                        }
                        continue
                    raw = parse_jsonish(held_market.raw_market_json, {})
                    wanted_side = (trade.position_side or "YES").lower()
                    for asset_id, outcome in market_clob_tokens(raw):
                        if outcome.lower() == wanted_side:
                            current_price = held_market.yes_price if wanted_side == "yes" else market_no_price(held_market)
                            assets[asset_id] = {
                                "asset_id": asset_id,
                                "role": "held_position",
                                "trade_id": trade.trade_id,
                                "position_side": trade.position_side,
                                "market_id": held_market.market_id,
                                "market_question": held_market.market_question,
                                "rule_min": held_market.rule_min,
                                "rule_max": held_market.rule_max,
                                "market_unit": held_market.unit,
                                "city": city,
                                "kind": kind,
                                "event_date": event_date,
                                "polymarket_url": event_url,
                                "station": station,
                                "last_price": current_price,
                            }
                for market in relevant_markets:
                    raw = parse_jsonish(market.raw_market_json, {})
                    for asset_id, outcome in market_clob_tokens(raw):
                        side = outcome.strip().upper()
                        if side not in {"YES", "NO"} or asset_id in assets:
                            continue
                        assets[asset_id] = {
                            "asset_id": asset_id,
                            "role": "temperature_market",
                            "position_side": side,
                            "market_id": market.market_id,
                            "market_question": market.market_question,
                            "rule_min": market.rule_min,
                            "rule_max": market.rule_max,
                            "market_unit": market.unit,
                            "city": city,
                            "kind": kind,
                            "event_date": event_date,
                            "polymarket_url": event_url,
                            "station": station,
                            "last_price": None,
                        }
            except Exception:
                LOGGER.exception("websocket asset discovery failed city=%s kind=%s", city, kind)
    return assets


def request_websocket_asset_refresh(city: str, kind: str = "", event_date: str = "", station: str = "", reason: str = "") -> None:
    """Queue a non-blocking, targeted websocket asset refresh request."""
    city = str(city or "")
    kind = str(kind or "")
    event_date = str(event_date or "")
    if not city:
        return
    key = (city, kind, event_date)
    with WEBSOCKET_ASSET_REFRESH_LOCK:
        if key in WEBSOCKET_ASSET_REFRESH_DEDUP:
            return
        WEBSOCKET_ASSET_REFRESH_DEDUP.add(key)
    WEBSOCKET_ASSET_REFRESH_REQUESTS.put(
        {
            "city": city,
            "kind": kind,
            "event_date": event_date,
            "station": str(station or ""),
            "reason": str(reason or ""),
            "requested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    )
    LOGGER.info("websocket asset refresh queued city=%s kind=%s event_date=%s station=%s reason=%s", city, kind, event_date, station, reason)


def websocket_asset_refresh_worker(config: dict[str, Any]) -> None:
    """Build targeted websocket asset updates away from the hot receive loop."""
    LOGGER.info("websocket asset refresh worker started")
    while True:
        request = WEBSOCKET_ASSET_REFRESH_REQUESTS.get()
        key = (str(request.get("city") or ""), str(request.get("kind") or ""), str(request.get("event_date") or ""))
        try:
            assets = websocket_assets(
                config,
                only_city=key[0],
                only_kind=key[1],
                only_event_date=key[2],
            )
            WEBSOCKET_ASSET_UPDATES.put({"request": request, "assets": assets})
            LOGGER.info(
                "websocket asset refresh built city=%s kind=%s event_date=%s reason=%s assets=%s",
                key[0],
                key[1],
                key[2],
                request.get("reason", ""),
                len(assets),
            )
        except Exception:
            LOGGER.exception("websocket asset refresh failed city=%s kind=%s event_date=%s reason=%s", key[0], key[1], key[2], request.get("reason", ""))
        finally:
            with WEBSOCKET_ASSET_REFRESH_LOCK:
                WEBSOCKET_ASSET_REFRESH_DEDUP.discard(key)
            WEBSOCKET_ASSET_REFRESH_REQUESTS.task_done()


def start_websocket_asset_refresh_thread(config: dict[str, Any]) -> list[threading.Thread]:
    """Start the background asset refresh worker pool once per process."""
    with WEBSOCKET_ASSET_REFRESH_LOCK:
        alive = [thread for thread in WEBSOCKET_ASSET_REFRESH_THREADS if thread.is_alive()]
        WEBSOCKET_ASSET_REFRESH_THREADS[:] = alive
        worker_count = max(1, int(config.get("trading", {}).get("websocket_asset_refresh_workers", 3)))
        while len(WEBSOCKET_ASSET_REFRESH_THREADS) < worker_count:
            thread = threading.Thread(
                target=websocket_asset_refresh_worker,
                args=(config,),
                name=f"websocket-asset-refresh-{len(WEBSOCKET_ASSET_REFRESH_THREADS) + 1}",
                daemon=True,
            )
            thread.start()
            WEBSOCKET_ASSET_REFRESH_THREADS.append(thread)
        return list(WEBSOCKET_ASSET_REFRESH_THREADS)


def drain_websocket_asset_updates(max_updates: int = 20) -> list[dict[str, Any]]:
    """Return completed background asset updates without blocking."""
    updates: list[dict[str, Any]] = []
    for _ in range(max_updates):
        try:
            updates.append(WEBSOCKET_ASSET_UPDATES.get_nowait())
        except queue.Empty:
            break
    return updates


def asset_matches_refresh_request(asset: dict[str, Any], request: dict[str, Any]) -> bool:
    """Check whether an existing asset belongs to a targeted refresh scope."""
    for field in ("city", "kind", "event_date"):
        wanted = str(request.get(field) or "")
        if wanted and str(asset.get(field) or "") != wanted:
            return False
    return True


def merge_websocket_asset_updates(assets: dict[str, dict[str, Any]], updates: list[dict[str, Any]]) -> bool:
    """Merge targeted asset refresh results while preserving live price state."""
    changed = False
    for update in updates:
        request = update.get("request") or {}
        new_assets = update.get("assets") or {}
        remove_ids = [
            asset_id
            for asset_id, asset in assets.items()
            if asset.get("role") == "temperature_market" and asset_matches_refresh_request(asset, request)
        ]
        for asset_id in remove_ids:
            assets.pop(asset_id, None)
            changed = True
        for asset_id, new_asset in new_assets.items():
            old_asset = assets.get(asset_id)
            if old_asset:
                if "last_prices_by_field" in old_asset:
                    new_asset.setdefault("last_prices_by_field", dict(old_asset.get("last_prices_by_field") or {}))
                if new_asset.get("last_price") is None and old_asset.get("last_price") is not None:
                    new_asset["last_price"] = old_asset.get("last_price")
            if assets.get(asset_id) != new_asset:
                assets[asset_id] = new_asset
                changed = True
        LOGGER.info(
            "websocket asset refresh applied city=%s kind=%s event_date=%s reason=%s assets=%s removed=%s",
            request.get("city", ""),
            request.get("kind", ""),
            request.get("event_date", ""),
            request.get("reason", ""),
            len(new_assets),
            len(remove_ids),
        )
    return changed


def process_harvest_if_new_websocket_metar(config: dict[str, Any], context: dict[str, Any]) -> None:
    """Refresh METAR after a WebSocket price trigger and run harvest only when observations changed.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        context (dict[str, Any]): WebSocket trigger context for one asset price movement.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Updates WEBSOCKET_AVIATION_OBS_BY_EVENT and may call process_deterministic_harvest.
    """
    city = str(context.get("city") or "")
    kind = str(context.get("kind") or "")
    event_date = str(context.get("event_date") or "")
    event_url = str(context.get("polymarket_url") or "")
    if not city or not kind or not event_date or not event_url:
        return
    try:
        event = fetch_event_by_url(config, event_url, city, kind, event_date)
        if not event:
            return
        markets = markets_for_event(config, event)
        event_unit = event_market_unit(markets)
        station = station_for_event(config, city, event_url)
        if not station:
            LOGGER.warning("websocket metar refresh skip missing station city=%s kind=%s url=%s", city, kind, event_url)
            return

        aviation_high, aviation_low, latest_dt, _ = aviation_observed_extremes(config, station, city, event_date, event_unit)
        if latest_dt is None or (kind == "Highest" and aviation_high is None) or (kind == "Lowest" and aviation_low is None):
            LOGGER.info(
                "websocket metar refresh skip no usable observations city=%s kind=%s event_date=%s station=%s",
                city,
                kind,
                event_date,
                station,
            )
            return

        key = (event_date, city, kind)
        latest_utc = latest_dt.astimezone(timezone.utc).isoformat()
        previous = WEBSOCKET_AVIATION_OBS_BY_EVENT.get(key, {})
        changed = (
            previous.get("latest_utc") != latest_utc
            or previous.get("aviation_high") != aviation_high
            or previous.get("aviation_low") != aviation_low
        )
        WEBSOCKET_AVIATION_OBS_BY_EVENT[key] = {
            "latest_utc": latest_utc,
            "aviation_high": aviation_high,
            "aviation_low": aviation_low,
        }
        if not changed:
            LOGGER.info(
                "websocket metar refresh unchanged city=%s kind=%s event_date=%s latest_utc=%s high=%s low=%s",
                city,
                kind,
                event_date,
                latest_utc,
                aviation_high,
                aviation_low,
            )
            return

        harvest_context = dict(context)
        harvest_context.update(
            {
                "source": "polymarket_websocket_metar_refresh",
                "aviation_high": aviation_high,
                "aviation_low": aviation_low,
                "aviation_latest_utc": latest_utc,
            }
        )
        LOGGER.info(
            "websocket metar refresh changed city=%s kind=%s event_date=%s latest_utc=%s high=%s low=%s",
            city,
            kind,
            event_date,
            latest_utc,
            aviation_high,
            aviation_low,
        )
        process_deterministic_harvest(config, harvest_context)
    except Exception:
        LOGGER.exception("websocket metar refresh failed context=%s", context)


def price_record_payload(event_type: str, asset: dict[str, Any], asset_id: str, price: Optional[float], previous_price: Optional[float], price_field: str, timing: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-serializable price capture record with millisecond timestamps."""
    now_utc = datetime.now(timezone.utc)
    return {
        "event_type": event_type,
        "captured_at_utc": now_utc.isoformat(timespec="milliseconds"),
        "captured_at_epoch_ms": int(now_utc.timestamp() * 1000),
        "expected_next_obs_utc": timing.get("expected_next_obs_utc", ""),
        "latest_obs_utc": timing.get("latest_obs_utc", ""),
        "scheduled_minutes_utc": timing.get("scheduled_minutes_utc", []),
        "asset_id": asset_id,
        "role": asset.get("role", ""),
        "position_side": asset.get("position_side", ""),
        "market_id": asset.get("market_id", ""),
        "market_question": asset.get("market_question", ""),
        "rule_min": asset.get("rule_min"),
        "rule_max": asset.get("rule_max"),
        "market_unit": asset.get("market_unit", ""),
        "city": asset.get("city", ""),
        "kind": asset.get("kind", ""),
        "event_date": asset.get("event_date", ""),
        "polymarket_url": asset.get("polymarket_url", ""),
        "station": asset.get("station", ""),
        "price": price,
        "previous_price": previous_price,
        "price_field": price_field,
    }


def raw_ws_record_payload(message: Any, matched_assets: list[dict[str, Any]], timing: dict[str, Any]) -> dict[str, Any]:
    """Build a raw websocket capture record for replay/debugging parser gaps."""
    now_utc = datetime.now(timezone.utc)
    return {
        "event_type": "websocket_raw",
        "captured_at_utc": now_utc.isoformat(timespec="milliseconds"),
        "captured_at_epoch_ms": int(now_utc.timestamp() * 1000),
        "expected_next_obs_utc": timing.get("expected_next_obs_utc", ""),
        "latest_obs_utc": timing.get("latest_obs_utc", ""),
        "scheduled_minutes_utc": timing.get("scheduled_minutes_utc", []),
        "matched_assets": matched_assets,
        "raw_message": message,
    }


def record_raw_price_window_message(config: dict[str, Any], assets: dict[str, dict[str, Any]], message: Any, rows: list[tuple[str, float, str]], received_ts: float) -> None:
    """Record every raw websocket message while any city observation recording window is active."""
    now_mono = time.monotonic()
    if not any(now_mono < float(recording.get("record_until_monotonic", 0.0)) for recording in PRICE_RECORDING_WINDOWS.values()):
        return
    append_price_window_raw_record(config, message)


def record_price_window_tick(config: dict[str, Any], assets: dict[str, dict[str, Any]], asset: dict[str, Any], asset_id: str, price: float, previous_price: Optional[float], price_field: str) -> None:
    """Record city-wide websocket prices while a station is inside its observation window."""
    city = str(asset.get("city") or "")
    station = str(asset.get("station") or "")
    if not city or not station:
        return
    in_window, timing = in_station_report_window(config, city, station)
    expected_next = str(timing.get("expected_next_obs_utc") or "")
    window_key = f"{city}:{station}:{expected_next}"
    now_mono = time.monotonic()
    record_seconds = max(1, int(config.get("price_momentum", {}).get("price_window_record_seconds", 300)))
    recording = PRICE_RECORDING_WINDOWS.get(window_key)
    if not recording:
        prefix = f"{city}:{station}:"
        for existing_key, existing_recording in PRICE_RECORDING_WINDOWS.items():
            if existing_key.startswith(prefix) and now_mono < float(existing_recording.get("record_until_monotonic", 0.0)):
                window_key = existing_key
                recording = existing_recording
                timing = dict(timing)
                timing["expected_next_obs_utc"] = existing_key[len(prefix):]
                break
    if not in_window:
        if not recording:
            return
        if now_mono < float(recording.get("record_until_monotonic", 0.0)):
            append_price_window_record(config, price_record_payload("websocket_tick", asset, asset_id, price, previous_price, price_field, timing))
            return
        if not recording.get("ended_logged"):
            recording["ended_logged"] = True
            LOGGER.info("price window recording ended city=%s station=%s expected_next_obs_utc=%s record_seconds=%s", city, station, expected_next, record_seconds)
        return
    if recording and now_mono >= float(recording.get("record_until_monotonic", 0.0)):
        if not recording.get("ended_logged"):
            recording["ended_logged"] = True
            LOGGER.info("price window recording ended city=%s station=%s expected_next_obs_utc=%s record_seconds=%s", city, station, expected_next, record_seconds)
        return
    if not recording:
        recording = {
            "record_until_monotonic": now_mono + record_seconds,
            "started_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "record_seconds": record_seconds,
            "ended_logged": False,
        }
        PRICE_RECORDING_WINDOWS[window_key] = recording
        signal_fields = websocket_momentum_signal_fields(config)
        for snapshot_asset_id, snapshot_asset in assets.items():
            if snapshot_asset.get("city") != city or snapshot_asset.get("station") != station:
                continue
            snapshot_fields = dict(snapshot_asset.get("last_prices_by_field") or {})
            if snapshot_asset.get("last_price") not in (None, ""):
                snapshot_fields.setdefault("last_price", snapshot_asset.get("last_price"))
            if snapshot_asset_id == asset_id and previous_price is not None:
                snapshot_fields[price_field] = previous_price
            for snapshot_field, snapshot_price in snapshot_fields.items():
                if snapshot_field not in signal_fields or snapshot_price in (None, ""):
                    continue
                PRICE_MOMENTUM_WINDOWS[price_momentum_window_key(snapshot_asset_id, snapshot_field)] = {
                    "expected_next_obs_utc": expected_next,
                    "base_price": float(snapshot_price),
                    "created_at": recording["started_at_utc"],
                    "source": "window_snapshot",
                    "price_field": snapshot_field,
                }
                append_price_window_record(config, price_record_payload("window_snapshot", snapshot_asset, snapshot_asset_id, float(snapshot_price), None, snapshot_field, timing))
        LOGGER.info("price window recording started city=%s station=%s expected_next_obs_utc=%s record_seconds=%s assets=%s", city, station, expected_next, record_seconds, sum(1 for a in assets.values() if a.get("city") == city and a.get("station") == station))
    momentum_key = price_momentum_window_key(asset_id, price_field)
    momentum_window = PRICE_MOMENTUM_WINDOWS.get(momentum_key)
    if price_field in websocket_momentum_signal_fields(config) and (
        not momentum_window or momentum_window.get("expected_next_obs_utc") != expected_next
    ):
        base_price = previous_price if previous_price is not None else price
        PRICE_MOMENTUM_WINDOWS[momentum_key] = {
            "expected_next_obs_utc": expected_next,
            "base_price": float(base_price),
            "created_at": recording.get("started_at_utc", datetime.now(timezone.utc).isoformat(timespec="milliseconds")),
            "source": "first_window_tick",
            "price_field": price_field,
        }
    append_price_window_record(config, price_record_payload("websocket_tick", asset, asset_id, price, previous_price, price_field, timing))


def ensure_price_recording_windows(config: dict[str, Any], assets: dict[str, dict[str, Any]]) -> None:
    """Start city-wide price recording as soon as a station enters its observation window."""
    seen: set[tuple[str, str]] = set()
    for asset_id, asset in assets.items():
        city = str(asset.get("city") or "")
        station = str(asset.get("station") or "")
        if not city or not station or (city, station) in seen:
            continue
        seen.add((city, station))
        in_window, timing = in_station_report_window(config, city, station)
        if not in_window:
            continue
        expected_next = str(timing.get("expected_next_obs_utc") or "")
        window_key = f"{city}:{station}:{expected_next}"
        if window_key in PRICE_RECORDING_WINDOWS:
            continue
        record_seconds = max(1, int(config.get("price_momentum", {}).get("price_window_record_seconds", 300)))
        recording = {
            "record_until_monotonic": time.monotonic() + record_seconds,
            "started_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "record_seconds": record_seconds,
            "ended_logged": False,
        }
        PRICE_RECORDING_WINDOWS[window_key] = recording
        count = 0
        signal_fields = websocket_momentum_signal_fields(config)
        for snapshot_asset_id, snapshot_asset in assets.items():
            if snapshot_asset.get("city") != city or snapshot_asset.get("station") != station:
                continue
            snapshot_fields = dict(snapshot_asset.get("last_prices_by_field") or {})
            if snapshot_asset.get("last_price") not in (None, ""):
                snapshot_fields.setdefault("last_price", snapshot_asset.get("last_price"))
            for snapshot_field, snapshot_price in snapshot_fields.items():
                if snapshot_field not in signal_fields or snapshot_price in (None, ""):
                    continue
                PRICE_MOMENTUM_WINDOWS[price_momentum_window_key(snapshot_asset_id, snapshot_field)] = {
                    "expected_next_obs_utc": expected_next,
                    "base_price": float(snapshot_price),
                    "created_at": recording["started_at_utc"],
                    "source": "window_snapshot",
                    "price_field": snapshot_field,
                }
                append_price_window_record(config, price_record_payload("window_snapshot", snapshot_asset, snapshot_asset_id, float(snapshot_price), None, snapshot_field, timing))
            count += 1
        LOGGER.info("price window recording started city=%s station=%s expected_next_obs_utc=%s record_seconds=%s assets=%s", city, station, expected_next, record_seconds, count)


def monitor_websocket(config: dict[str, Any], duration_seconds: int = 0) -> bool:
    """Connect to Polymarket WebSocket, subscribe to assets, and dispatch significant price triggers.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        duration_seconds (int): Optional maximum monitor runtime; zero or less means run until disconnected.
    
    Returns:
        bool: True when monitoring was attempted, False when disabled or no assets exist.
    
    Side effects:
        Opens a WebSocket connection, updates in-memory asset prices, and may trigger trade processing.
    """
    if not config["trading"].get("websocket_enabled", True):
        return False
    try:
        import websocket  # type: ignore
    except ImportError:
        LOGGER.warning("websocket disabled: install websocket-client from requirements.txt")
        return False
    ws_url = str(config["trading"].get("websocket_url"))
    timeout_seconds = int(config["trading"].get("websocket_timeout_seconds", 10))
    ping_seconds = max(1, int(config["trading"].get("websocket_ping_seconds", 10)))
    trigger_pct = float(config["trading"].get("monitor_price_change_pct", 0.03))
    signal_fields = websocket_momentum_signal_fields(config)
    start_websocket_asset_refresh_thread(config)
    assets = websocket_assets(config)
    if not assets:
        LOGGER.info("websocket no temperature market assets")
        return False
    asset_ids = sorted(assets)
    ws = websocket.create_connection(ws_url, timeout=timeout_seconds)
    try:
        ws.settimeout(min(1, timeout_seconds))
        ws.send(json.dumps({"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}))
        started = time.monotonic()
        last_ping = started
        while duration_seconds <= 0 or time.monotonic() - started < duration_seconds:
            updates = drain_websocket_asset_updates()
            if updates and merge_websocket_asset_updates(assets, updates):
                new_ids = sorted(assets)
                if new_ids and new_ids != asset_ids:
                    asset_ids = new_ids
                    ws.send(json.dumps({"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}))
                    LOGGER.info("websocket subscription updated assets=%s", len(asset_ids))
            ensure_price_recording_windows(config, assets)
            if time.monotonic() - last_ping >= ping_seconds:
                ws.send("PING")
                last_ping = time.monotonic()
            try:
                message = ws.recv()
                received_ts = time.time()
                if message == "PONG":
                    continue
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                break
            price_rows = websocket_message_prices(message)
            record_raw_price_window_message(config, assets, message, price_rows, received_ts)
            for asset_id, price, price_field in price_rows:
                asset = assets.get(asset_id)
                if not asset:
                    continue
                field_prices = asset.setdefault("last_prices_by_field", {})
                previous_field = field_prices.get(price_field)
                field_prices[price_field] = price
                record_price_window_tick(config, assets, asset, asset_id, price, float(previous_field) if previous_field is not None else None, price_field)
                if price_field not in signal_fields:
                    continue
                if price_field in {"price", "last_price"}:
                    previous = asset.get("last_price")
                    asset["last_price"] = price
                else:
                    previous = previous_field
                if previous is None:
                    continue
                previous_price = float(previous)
                if previous_price <= 0:
                    continue
                price_change_pct = directional_price_change_fraction(previous_price, float(price))
                context = {
                    "source": "polymarket_websocket_temperature_market",
                    "asset_id": asset_id,
                    "price": price,
                    "previous_price": previous_price,
                    "price_change_pct": price_change_pct,
                    "log_skip": price_change_pct >= trigger_pct,
                    "price_field": price_field,
                    "momentum_key": price_momentum_window_key(asset_id, price_field),
                    **asset,
                }
                if price_change_pct >= trigger_pct:
                    LOGGER.info("websocket trigger %s", json.dumps(context, ensure_ascii=False, sort_keys=True))
                if asset.get("role") == "held_position" and price_change_pct >= trigger_pct:
                    verify_open_positions_with_twc(config, context)
                trade = process_price_momentum_buy(config, context)
                if trade:
                    request_websocket_asset_refresh(
                        str(asset.get("city") or ""),
                        str(asset.get("kind") or ""),
                        str(asset.get("event_date") or ""),
                        str(asset.get("station") or ""),
                        "trade_opened",
                    )
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return True


def websocket_supervisor(config: dict[str, Any]) -> None:
    """Continuously run the WebSocket monitor and reconnect after failures.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Runs forever until the process exits.
    """
    reconnect = int(config["trading"].get("websocket_reconnect_seconds", 5))
    LOGGER.info("websocket supervisor started")
    while True:
        try:
            monitor_websocket(config, 0)
        except Exception:
            LOGGER.exception("websocket monitor failed")
        time.sleep(reconnect)


def start_websocket_thread(config: dict[str, Any]) -> threading.Thread:
    """Start the WebSocket supervisor in a daemon thread.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        threading.Thread: Started daemon thread.
    
    Side effects:
        Starts a daemon thread.
    """
    thread = threading.Thread(target=websocket_supervisor, args=(config,), name="deterministic-websocket", daemon=True)
    thread.start()
    return thread


def aviation_supervisor(config: dict[str, Any]) -> None:
    """Dynamically poll AviationWeather METAR and trigger harvest when observed extremes change.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Runs forever until the process exits and may trigger trade processing.
    """
    fallback_poll_seconds = max(10, int(config["trading"].get("aviation_poll_interval_seconds", 60)))
    probe_seconds = max(2, int(config["trading"].get("aviation_refresh_probe_seconds", 5)))
    pre_probe_seconds = 60
    event_cache: list[dict[str, Any]] = []
    station_groups: dict[tuple[str, str], dict[str, Any]] = {}
    event_refresh_at = 0.0
    event_state: dict[tuple[str, str, str], dict[str, Any]] = {}
    station_state: dict[tuple[str, str], dict[str, Any]] = {}
    LOGGER.info(
        "aviation supervisor started fallback_poll_seconds=%s probe_seconds=%s pre_probe_seconds=%s",
        fallback_poll_seconds,
        probe_seconds,
        pre_probe_seconds,
    )
    while True:
        try:
            now_mono = time.monotonic()
            if now_mono >= event_refresh_at:
                event_cache = []
                for target in resolve_event_target_dates(config):
                    event_cache.extend(discover_temperature_events(config, target))
                station_groups = {}
                for event in event_cache:
                    city = event["_parsed_city"]
                    event_url = poly_url_from_event(event)
                    try:
                        station = station_for_event(config, city, event_url)
                    except Exception:
                        LOGGER.exception("aviation station discovery failed city=%s url=%s", city, event_url)
                        continue
                    if not station:
                        continue
                    group_key = (city, station)
                    group = station_groups.setdefault(group_key, {"city": city, "station": station, "events": []})
                    group["events"].append(event)
                    station_state.setdefault(group_key, {"next_probe_at": now_mono, "latest_obs_utc": "", "min_interval_seconds": None, "probing": False})
                for stale_key in set(station_state) - set(station_groups):
                    station_state.pop(stale_key, None)
                event_refresh_at = now_mono + 300
                LOGGER.info("aviation event cache refreshed events=%s station_groups=%s", len(event_cache), len(station_groups))

            if not station_groups:
                time.sleep(min(fallback_poll_seconds, 30))
                continue

            now_mono = time.monotonic()
            for group_key, group in station_groups.items():
                schedule = station_state.setdefault(group_key, {"next_probe_at": now_mono, "latest_obs_utc": "", "min_interval_seconds": None, "probing": False})
                if now_mono < float(schedule.get("next_probe_at", 0.0)):
                    continue

                city = str(group["city"])
                station = str(group["station"])
                try:
                    latest_dt, min_interval_seconds, report_count, scheduled_minutes, expected_next_dt = aviation_report_timing(station, 24, config)
                    if latest_dt is None:
                        schedule.update({"next_probe_at": now_mono + fallback_poll_seconds, "probing": False})
                        LOGGER.info("aviation timing unavailable city=%s station=%s next_probe_seconds=%s", city, station, fallback_poll_seconds)
                        continue

                    latest_utc = latest_dt.astimezone(timezone.utc).isoformat()
                    previous_latest = str(schedule.get("latest_obs_utc") or "")
                    has_new_report = latest_utc != previous_latest
                    effective_interval = max(60, int(min_interval_seconds or fallback_poll_seconds))
                    next_probe_anchor = expected_next_dt or (latest_dt.astimezone(timezone.utc) + timedelta(seconds=effective_interval))
                    next_probe_wall = next_probe_anchor.astimezone(timezone.utc) - timedelta(seconds=pre_probe_seconds)
                    next_probe_delay = max(0.0, (next_probe_wall - datetime.now(timezone.utc)).total_seconds())
                    timing = update_station_report_timing(city, station, latest_dt, effective_interval, report_count, scheduled_minutes, expected_next_dt)

                    if not has_new_report:
                        schedule.update({
                            "next_probe_at": now_mono + probe_seconds,
                            "min_interval_seconds": effective_interval,
                            "probing": True,
                        })
                        LOGGER.info(
                            "aviation no new report city=%s station=%s latest_obs_utc=%s scheduled_minutes_utc=%s expected_next_obs_utc=%s retry_seconds=%s",
                            city,
                            station,
                            latest_utc,
                            timing.get("scheduled_minutes_utc"),
                            timing.get("expected_next_obs_utc"),
                            probe_seconds,
                        )
                        continue

                    schedule.update({
                        "latest_obs_utc": latest_utc,
                        "min_interval_seconds": effective_interval,
                        "next_probe_at": now_mono + next_probe_delay,
                        "probing": False,
                    })
                    LOGGER.info(
                        "aviation new report city=%s station=%s latest_obs_utc=%s interval_seconds=%s report_count=%s scheduled_minutes_utc=%s expected_next_obs_utc=%s next_probe_at_utc=%s next_probe_seconds=%.1f",
                        city,
                        station,
                        latest_utc,
                        effective_interval,
                        report_count,
                        timing.get("scheduled_minutes_utc"),
                        timing.get("expected_next_obs_utc"),
                        next_probe_wall.isoformat(),
                        next_probe_delay,
                    )

                    for event in list(group["events"]):
                        kind = event["_parsed_kind"]
                        event_date = event["_parsed_event_date"]
                        event_url = poly_url_from_event(event)
                        try:
                            markets = markets_for_event(config, event)
                            event_unit = event_market_unit(markets)
                            aviation_high, aviation_low, event_latest_dt, _ = aviation_observed_extremes(config, station, city, event_date, event_unit)
                            if event_latest_dt is None:
                                continue
                            key = (event_date, city, kind)
                            previous = event_state.get(key, {})
                            changed = (
                                (kind == "Highest" and aviation_high is not None and aviation_high != previous.get("aviation_high")) or
                                (kind == "Lowest" and aviation_low is not None and aviation_low != previous.get("aviation_low"))
                            )
                            event_state[key] = {
                                "aviation_high": aviation_high,
                                "aviation_low": aviation_low,
                                "latest_dt": event_latest_dt.isoformat(),
                            }
                            if not changed:
                                continue
                            LOGGER.info("aviation extreme changed city=%s kind=%s high=%s low=%s latest=%s", city, kind, aviation_high, aviation_low, event_latest_dt.isoformat())
                            request_websocket_asset_refresh(city, kind, event_date, station, "aviation_extreme_changed")
                            if config.get("price_momentum", {}).get("awc_extreme_harvest_enabled", False):
                                process_deterministic_harvest(
                                    config,
                                    {
                                        "source": "aviation_metar",
                                        "city": city,
                                        "kind": kind,
                                        "event_date": event_date,
                                        "polymarket_url": event_url,
                                        "aviation_high": aviation_high,
                                        "aviation_low": aviation_low,
                                    },
                                )
                        except Exception:
                            LOGGER.exception("aviation event failed city=%s kind=%s", city, kind)
                except Exception:
                    LOGGER.exception("aviation station poll failed city=%s station=%s", group.get("city"), group.get("station"))
        except Exception:
            LOGGER.exception("aviation supervisor loop failed")

        now_mono = time.monotonic()
        next_probe_at = min((float(s.get("next_probe_at", now_mono + fallback_poll_seconds)) for s in station_state.values()), default=now_mono + fallback_poll_seconds)
        next_wakeup = min(next_probe_at, event_refresh_at)
        time.sleep(max(1.0, min(30.0, next_wakeup - now_mono)))


def start_aviation_thread(config: dict[str, Any]) -> threading.Thread:
    """Start the AviationWeather supervisor in a daemon thread.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        threading.Thread: Started daemon thread.
    
    Side effects:
        Starts a daemon thread.
    """
    thread = threading.Thread(target=aviation_supervisor, args=(config,), name="deterministic-aviation", daemon=True)
    thread.start()
    return thread


def model_awc_run_prediction_rows(
    config: dict[str, Any],
    city: str,
    station: str,
    event_date: str,
    local_hour: int,
    expected_utc: datetime,
    events_for_date: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    source: str,
) -> bool:
    """Build and trade the current hourly prediction from a supplied METAR history."""
    got_latest_metar = False
    for event in events_for_date:
        built = model_awc_feature_row(config, city, station, rows, event_date)
        if built is None:
            continue
        features, latest_row, latest_local = built
        if latest_local.hour != local_hour or abs(latest_row.valid_utc - expected_utc) > timedelta(minutes=10):
            LOGGER.info(
                "model awc latest metar not current window source=%s city=%s station=%s latest=%s latest_local=%s expected=%s",
                source,
                city,
                station,
                latest_row.valid_utc.isoformat(),
                latest_local.isoformat(),
                expected_utc.isoformat(),
            )
            continue
        got_latest_metar = True
        predicted_high_f = model_awc_predict_high(config, features)
        process_model_awc_prediction(config, event, city, station, predicted_high_f, latest_row, latest_local)
    return got_latest_metar


def model_awc_tgftp_window_worker(
    config: dict[str, Any],
    city: str,
    station: str,
    event_date: str,
    local_hour: int,
    expected_utc: datetime,
    tgftp_start_utc: datetime,
    events_for_date: list[dict[str, Any]],
    lookback_hours: int,
) -> None:
    """Poll TGFTP every few seconds and run the model as soon as the hourly METAR appears."""
    trading = config["trading"]
    interval = max(0.5, float(trading.get("model_awc_tgftp_poll_interval_seconds", 2)))
    request_timeout = max(0.5, float(trading.get("model_awc_tgftp_request_timeout_seconds", 2)))
    timeout = max(interval, float(trading.get("model_awc_tgftp_poll_timeout_seconds", 300)))
    required_history_hours = model_awc_required_history_hours(config)
    history_hours = max(required_history_hours, int(trading.get("model_awc_tgftp_awc_history_hours", required_history_hours)))
    history_rows = model_awc_fetch_history_with_retry(
        config,
        station,
        history_hours,
        "model_awc_tgftp_prefetch",
    )
    wait_seconds = (tgftp_start_utc - datetime.now(timezone.utc)).total_seconds()
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    deadline = time.monotonic() + timeout
    attempts = 0
    LOGGER.info(
        "model awc tgftp polling started city=%s station=%s event_date=%s local_hour=%s expected=%s tgftp_start=%s awc_history_hours=%s awc_rows=%s interval_seconds=%s timeout_seconds=%s",
        city,
        station,
        event_date,
        local_hour,
        expected_utc.isoformat(),
        tgftp_start_utc.isoformat(),
        history_hours,
        len(history_rows),
        interval,
        timeout,
    )
    while time.monotonic() <= deadline:
        attempts += 1
        started = time.monotonic()
        try:
            obs = tgftp_metar_observation(station, request_timeout)
            obs_dt = obs.get("obs_dt") if obs else None
            if isinstance(obs_dt, datetime) and obs_dt >= expected_utc - timedelta(minutes=10):
                rows = merge_tgftp_into_aviation_rows(history_rows, obs)
                if model_awc_run_prediction_rows(
                    config,
                    city,
                    station,
                    event_date,
                    local_hour,
                    expected_utc,
                    events_for_date,
                    rows,
                    "tgftp",
                ):
                    update_cached_tgftp_observation(station, obs)
                    LOGGER.info(
                        "model awc tgftp observation accepted city=%s station=%s obs_utc=%s attempts=%s raw=%r",
                        city,
                        station,
                        obs_dt.isoformat(),
                        attempts,
                        obs.get("raw_ob"),
                    )
                    return
        except Exception as exc:
            if attempts == 1 or attempts % 15 == 0:
                LOGGER.warning(
                    "model awc tgftp poll failed city=%s station=%s event_date=%s local_hour=%s attempt=%s error=%s",
                    city,
                    station,
                    event_date,
                    local_hour,
                    attempts,
                    exc,
                )
        remaining = interval - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)

    LOGGER.warning(
        "model awc tgftp window timeout city=%s station=%s event_date=%s local_hour=%s expected=%s attempts=%s",
        city,
        station,
        event_date,
        local_hour,
        expected_utc.isoformat(),
        attempts,
    )
    rows = model_awc_fetch_history_with_retry(
        config,
        station,
        lookback_hours,
        "model_awc_timeout_fallback",
    )
    try:
        model_awc_run_prediction_rows(
            config,
            city,
            station,
            event_date,
            local_hour,
            expected_utc,
            events_for_date,
            rows,
            "awc_timeout_fallback",
        )
    except Exception:
        LOGGER.exception("model awc timeout fallback failed city=%s station=%s local_hour=%s", city, station, local_hour)


def model_awc_supervisor(config: dict[str, Any]) -> None:
    """Poll AWC METAR after expected observation windows and trade model predictions."""
    try:
        if not model_awc_enabled(config):
            LOGGER.info("model awc supervisor disabled")
            return
        model_awc_load_model(config)
    except Exception:
        LOGGER.exception("model awc supervisor failed during startup")
        return

    fallback_poll_seconds = max(10, int(config["trading"].get("aviation_poll_interval_seconds", 60)))
    tgftp_enabled = bool(config["trading"].get("model_awc_tgftp_enabled", True))
    poll_delay_seconds = max(
        0,
        int(
            config["trading"].get(
                "model_awc_tgftp_start_delay_seconds" if tgftp_enabled else "model_awc_poll_delay_seconds",
                60 if tgftp_enabled else 180,
            )
        ),
    )
    poll_interval_seconds = max(10, int(config["trading"].get("model_awc_poll_interval_seconds", 60)))
    poll_attempts = max(1, int(config["trading"].get("model_awc_poll_attempts", 5)))
    required_history_hours = model_awc_required_history_hours(config)
    lookback_hours = max(
        required_history_hours,
        int(config["trading"].get("model_awc_metar_lookback_hours", required_history_hours)),
    )
    start_hour = int(config["trading"].get("model_awc_buy_start_hour", 12))
    end_hour = int(config["trading"].get("model_awc_buy_end_hour", 16))
    event_cache: list[dict[str, Any]] = []
    station_groups: dict[tuple[str, str], dict[str, Any]] = {}
    window_state: dict[str, dict[str, Any]] = {}
    event_refresh_at = 0.0
    LOGGER.info(
        "model awc supervisor started live_station=%s default_local_hours=%s-%s station_local_hours=%s source=%s poll_delay_seconds=%s poll_interval_seconds=%s poll_attempts_per_window=%s lookback_hours=%s station_observation_minutes=%s",
        model_awc_live_station(config),
        start_hour,
        end_hour,
        config["trading"].get("model_awc_station_buy_hours", {}),
        "tgftp" if tgftp_enabled else "awc",
        poll_delay_seconds,
        poll_interval_seconds,
        poll_attempts,
        lookback_hours,
        config["trading"].get("model_awc_station_observation_minutes", {}),
    )
    while True:
        try:
            now_mono = time.monotonic()
            now_utc = datetime.now(timezone.utc)
            if now_mono >= event_refresh_at:
                event_cache = []
                for target in resolve_event_target_dates(config):
                    event_cache.extend(
                        event for event in discover_temperature_events(config, target)
                        if str(event.get("_parsed_kind") or "") == "Highest"
                    )
                station_groups = {}
                for event in event_cache:
                    city = str(event["_parsed_city"])
                    event_url = poly_url_from_event(event)
                    try:
                        station = station_for_event(config, city, event_url).upper()
                    except Exception:
                        LOGGER.exception("model awc station discovery failed city=%s url=%s", city, event_url)
                        continue
                    if not station:
                        continue
                    group_key = (city, station)
                    group = station_groups.setdefault(group_key, {"city": city, "station": station, "events": []})
                    group["events"].append(event)
                event_refresh_at = now_mono + 300
                LOGGER.info("model awc event cache refreshed highest_events=%s station_groups=%s", len(event_cache), len(station_groups))

            for group_key, group in station_groups.items():
                city = str(group["city"])
                station = str(group["station"])
                tz = city_timezone(config, city)
                now_local = now_utc.astimezone(tz)
                observation_minutes = model_awc_station_observation_minutes(config, station)
                events_by_date: dict[str, list[dict[str, Any]]] = {}
                for event in list(group["events"]):
                    event_date = str(event.get("_parsed_event_date") or "")
                    if event_date:
                        events_by_date.setdefault(event_date, []).append(event)

                start_hour, end_hour = model_awc_station_buy_hours(config, station)
                for event_date, events_for_date in events_by_date.items():
                    try:
                        target_date = date.fromisoformat(event_date)
                    except ValueError:
                        continue
                    if now_local.date() != target_date:
                        continue
                    windows = (
                        (hour, minute)
                        for hour in range(start_hour, end_hour + 1)
                        for minute in observation_minutes
                    )
                    for local_hour, observation_minute in windows:
                        expected_local = datetime(
                            target_date.year, target_date.month, target_date.day,
                            local_hour, observation_minute, tzinfo=tz,
                        )
                        expected_utc = expected_local.astimezone(timezone.utc)
                        tgftp_start_utc = expected_utc + timedelta(seconds=poll_delay_seconds)
                        poll_start_utc = expected_utc if tgftp_enabled else tgftp_start_utc
                        poll_end_utc = tgftp_start_utc + timedelta(
                            seconds=(
                                float(config["trading"].get("model_awc_tgftp_poll_timeout_seconds", 300))
                                if tgftp_enabled
                                else poll_interval_seconds * poll_attempts
                            )
                        )
                        if now_utc < poll_start_utc:
                            continue
                        window_key = (
                            f"{city}:{station}:{event_date}:hour_{local_hour:02d}:"
                            f"minute_{observation_minute:02d}:model_awc"
                        )
                        state = window_state.setdefault(
                            window_key,
                            {
                                "attempts": 0,
                                "next_poll_at": poll_start_utc.timestamp(),
                                "done": False,
                                "expected_obs_utc": expected_utc.isoformat(),
                            },
                        )
                        if state.get("done"):
                            continue
                        if now_utc > poll_end_utc:
                            state["done"] = True
                            continue
                        if tgftp_enabled:
                            state["done"] = True
                            state["attempts"] = 1
                            thread = threading.Thread(
                                target=model_awc_tgftp_window_worker,
                                args=(
                                    config,
                                    city,
                                    station,
                                    event_date,
                                    local_hour,
                                    expected_utc,
                                    tgftp_start_utc,
                                    list(events_for_date),
                                    lookback_hours,
                                ),
                                name=f"model-tgftp-{station}-{event_date}-{local_hour:02d}-{observation_minute:02d}",
                                daemon=True,
                            )
                            thread.start()
                            continue
                        if int(state.get("attempts", 0)) >= poll_attempts:
                            state["done"] = True
                            continue
                        next_poll_dt = datetime.fromtimestamp(float(state.get("next_poll_at", 0.0)), timezone.utc)
                        if now_utc < next_poll_dt:
                            continue

                        state["attempts"] = int(state.get("attempts", 0)) + 1
                        state["next_poll_at"] = (now_utc + timedelta(seconds=poll_interval_seconds)).timestamp()
                        try:
                            rows = aviation_metar_observations(station, lookback_hours)
                            append_aviation_metar_history(config, station, rows, "model_awc_prediction")
                            got_latest_metar = False
                            for event in events_for_date:
                                built = model_awc_feature_row(config, city, station, rows, event_date)
                                if built is None:
                                    continue
                                features, latest_row, latest_local = built
                                if latest_local.hour != local_hour or abs(latest_row.valid_utc - expected_utc) > timedelta(minutes=10):
                                    LOGGER.info(
                                        "model awc latest metar not current window city=%s station=%s latest=%s latest_local=%s expected=%s attempt=%s",
                                        city,
                                        station,
                                        latest_row.valid_utc.isoformat(),
                                        latest_local.isoformat(),
                                        expected_utc.isoformat(),
                                        state["attempts"],
                                    )
                                    continue
                                got_latest_metar = True
                                predicted_high_f = model_awc_predict_high(config, features)
                                process_model_awc_prediction(config, event, city, station, predicted_high_f, latest_row, latest_local)
                            if got_latest_metar:
                                state["done"] = True
                            LOGGER.info(
                                "model awc poll city=%s station=%s event_date=%s local_hour=%s expected=%s attempts=%s done=%s got_latest_metar=%s rows=%s",
                                city,
                                station,
                                event_date,
                                local_hour,
                                expected_utc.isoformat(),
                                state["attempts"],
                                state.get("done"),
                                got_latest_metar,
                                len(rows),
                            )
                        except Exception:
                            LOGGER.exception("model awc poll failed city=%s station=%s event_date=%s local_hour=%s", city, station, event_date, local_hour)

            active_prefixes = {f"{group['city']}:{group['station']}:" for group in station_groups.values()}
            for key in list(window_state):
                if not any(key.startswith(prefix) for prefix in active_prefixes):
                    window_state.pop(key, None)
            time.sleep(1.0 if tgftp_enabled else min(30.0, fallback_poll_seconds))
        except Exception:
            LOGGER.exception("model awc supervisor loop failed")
            time.sleep(min(30.0, fallback_poll_seconds))


def start_model_awc_thread(config: dict[str, Any]) -> threading.Thread:
    """Start the AWC METAR model strategy in a daemon thread."""
    LOGGER.info("model awc thread starting")
    thread = threading.Thread(target=model_awc_supervisor, args=(config,), name="model-awc-high", daemon=True)
    thread.start()
    return thread


def weather_record_supervisor(config: dict[str, Any]) -> None:
    """Process pushed weatherrecord observations inside airport report windows."""
    fallback_poll_seconds = max(10, int(config["trading"].get("aviation_poll_interval_seconds", 60)))
    pre_window_seconds = max(0, int(config["trading"].get("weather_record_pre_window_seconds", 120)))
    receive_window_seconds = max(300, int(config["trading"].get("weather_record_receive_window_seconds", 300)))
    timing_refresh_seconds = max(10, int(config["trading"].get("weather_record_timing_refresh_seconds", 60)))
    timing_stagger_seconds = max(1, int(config["trading"].get("weather_record_timing_stagger_seconds", timing_refresh_seconds)))
    tgftp_start_delay_seconds = max(0, int(config["trading"].get("tgftp_window_start_delay_seconds", 90)))
    tgftp_min_interval_seconds = max(0.01, float(config["trading"].get("tgftp_window_poll_min_interval_seconds", 0.05)))
    tgftp_max_inflight = max(1, int(config["trading"].get("tgftp_window_max_inflight_per_station", 10)))
    price_momentum_settings = config.get("price_momentum", {})
    clob_poll_enabled = bool(price_momentum_settings.get("clob_poll_enabled", True))
    clob_poll_interval_seconds = max(0.01, float(price_momentum_settings.get("clob_poll_interval_seconds", 0.025)))
    clob_no_change_fraction = max(0.0, float(price_momentum_settings.get("clob_no_change_pct", 0.10)))
    clob_max_inflight = max(1, int(price_momentum_settings.get("clob_poll_max_inflight_per_market", 32)))
    event_cache: list[dict[str, Any]] = []
    station_groups: dict[tuple[str, str], dict[str, Any]] = {}
    station_state: dict[tuple[str, str], dict[str, Any]] = {}
    awc_timing_pending: dict[tuple[str, str], dict[str, Any]] = {}
    tgftp_window_state: dict[str, dict[str, Any]] = {}
    tgftp_pending: dict[str, dict[str, Any]] = {}
    clob_momentum_state: dict[str, dict[str, Any]] = {}
    clob_momentum_pending: dict[str, dict[str, Any]] = {}
    event_state: dict[tuple[str, str, str], dict[str, Any]] = {}
    event_refresh_at = 0.0
    next_tgftp_poll_at = 0.0
    next_clob_poll_at = 0.0
    LOGGER.info(
        "weatherrecord supervisor started source=websocket pre_window_seconds=%s receive_window_seconds=%s timing_refresh_seconds=%s timing_stagger_seconds=%s tgftp_start_delay_seconds=%s tgftp_min_interval_seconds=%s tgftp_max_inflight=%s clob_poll_enabled=%s clob_poll_interval_seconds=%s clob_no_change_fraction=%s clob_max_inflight=%s fallback_poll_seconds=%s",
        pre_window_seconds,
        receive_window_seconds,
        timing_refresh_seconds,
        timing_stagger_seconds,
        tgftp_start_delay_seconds,
        tgftp_min_interval_seconds,
        tgftp_max_inflight,
        clob_poll_enabled,
        clob_poll_interval_seconds,
        clob_no_change_fraction,
        clob_max_inflight,
        fallback_poll_seconds,
    )
    while True:
        try:
            now_mono = time.monotonic()
            for done_key, pending in list(awc_timing_pending.items()):
                future = pending["future"]
                if not future.done():
                    continue
                city = str(pending["city"])
                station = str(pending["station"])
                state = station_state.setdefault(done_key, {"next_timing_refresh_at": now_mono})
                try:
                    latest_dt, interval_seconds, report_count, scheduled_minutes, expected_next_dt = future.result()
                    if latest_dt is None:
                        LOGGER.info("weatherrecord timing unavailable city=%s station=%s", city, station)
                    else:
                        update_station_report_timing(city, station, latest_dt, interval_seconds, report_count, scheduled_minutes, expected_next_dt)
                except Exception:
                    LOGGER.exception("weatherrecord timing refresh task failed city=%s station=%s", city, station)
                finally:
                    awc_timing_pending.pop(done_key, None)
            for done_key, pending in list(tgftp_pending.items()):
                future = pending["future"]
                if not future.done():
                    continue
                window_key = str(pending["window_key"])
                group = pending["group"]
                station = str(group["station"])
                state = tgftp_window_state.setdefault(window_key, {"done": False, "attempts": 0, "next_poll_at": now_mono})
                try:
                    obs = future.result()
                    expected_dt = pending["expected_dt"]
                    if state.get("done"):
                        pass
                    elif obs and isinstance(obs.get("obs_dt"), datetime):
                        obs_dt = obs["obs_dt"].astimezone(timezone.utc)
                        LOGGER.info(
                            "tgftp window poll completed city=%s station=%s expected_obs_utc=%s obs_utc=%s attempts=%s",
                            group.get("city"),
                            station,
                            expected_dt.isoformat(),
                            obs_dt.isoformat(),
                            state.get("attempts"),
                        )
                        if obs_dt >= expected_dt:
                            process_tgftp_window_observation(config, group, pending["timing"], obs)
                            state["done"] = True
                    else:
                        LOGGER.info("tgftp window poll completed no observation city=%s station=%s attempts=%s", group.get("city"), station, state.get("attempts"))
                except Exception:
                    LOGGER.exception("tgftp window poll task failed city=%s station=%s attempts=%s", group.get("city"), station, state.get("attempts"))
                tgftp_pending.pop(done_key, None)
            for done_key, pending in list(clob_momentum_pending.items()):
                future = pending["future"]
                if not future.done():
                    continue
                try:
                    price_by_asset = future.result()
                    if not isinstance(price_by_asset, dict):
                        price_by_asset = {}
                    trades = read_trades(config["outputs"]["trades_csv"])
                    for item in pending.get("items", []):
                        window_key = str(item["window_key"])
                        state = clob_momentum_state.setdefault(window_key, {})
                        city = str(item.get("city") or "")
                        kind = str(item.get("kind") or "")
                        event_date = str(item.get("event_date") or "")
                        market = item.get("market")
                        asset_id = str(item.get("asset_id") or "")
                        price = price_by_asset.get(asset_id)
                        if open_no_trade_exists_for_event(trades, str(config["trading"]["strategy_name"]), city, kind, event_date):
                            state["done"] = True
                        elif state.get("done"):
                            pass
                        elif price is None or float(price) <= 0:
                            LOGGER.info("clob momentum poll no_price city=%s kind=%s station=%s market=%s attempts=%s", city, kind, item.get("station"), item.get("market_id"), state.get("attempts"))
                        elif not state.get("base_price"):
                            base_price = float(price)
                            state["base_price"] = base_price
                            state["target_price"] = momentum_target_price(base_price, clob_no_change_fraction)
                            state["base_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                            LOGGER.info(
                                "clob momentum baseline city=%s kind=%s station=%s market=%s base_price=%s target_price=%s delta=%s expected_obs_utc=%s question=%r",
                                city,
                                kind,
                                item.get("station"),
                                item.get("market_id"),
                                base_price,
                                state["target_price"],
                                clob_no_change_fraction,
                                item.get("expected_obs_utc"),
                                market.market_question if isinstance(market, TemperatureMarket) else "",
                            )
                        else:
                            base_price = float(state["base_price"])
                            target_price = float(state.get("target_price") or momentum_target_price(base_price, clob_no_change_fraction))
                            current_price = float(price)
                            if current_price >= target_price:
                                LOGGER.info(
                                    "clob momentum trigger city=%s kind=%s station=%s market=%s base_price=%s price=%s target_price=%s delta=%s expected_obs_utc=%s",
                                    city,
                                    kind,
                                    item.get("station"),
                                    item.get("market_id"),
                                    base_price,
                                    current_price,
                                    target_price,
                                    clob_no_change_fraction,
                                    item.get("expected_obs_utc"),
                                )
                                context = {
                                    "role": "temperature_market",
                                    "position_side": "NO",
                                    "price": current_price,
                                    "previous_price": base_price,
                                    "price_field": "clob_no_buy_price",
                                    "city": city,
                                    "kind": kind,
                                    "event_date": event_date,
                                    "polymarket_url": item.get("polymarket_url"),
                                    "station": item.get("station"),
                                    "market_id": item.get("market_id"),
                                    "asset_id": asset_id,
                                    "momentum_key": f"clob:{window_key}",
                                    "move_fraction": clob_no_change_fraction,
                                }
                                PRICE_MOMENTUM_WINDOWS[str(context["momentum_key"])] = {
                                    "expected_next_obs_utc": item.get("expected_obs_utc"),
                                    "base_price": base_price,
                                    "created_at": state.get("base_at_utc") or datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                                    "source": "active_clob_poll",
                                    "price_field": "clob_no_buy_price",
                                }
                                trade = process_price_momentum_buy(config, context)
                                if trade:
                                    state["done"] = True
                            else:
                                LOGGER.info(
                                    "clob momentum below_target city=%s kind=%s station=%s market=%s base_price=%s price=%s target_price=%s expected_obs_utc=%s",
                                    city,
                                    kind,
                                    item.get("station"),
                                    item.get("market_id"),
                                    base_price,
                                    current_price,
                                    target_price,
                                    item.get("expected_obs_utc"),
                                )
                except Exception:
                    LOGGER.exception("clob momentum batch poll task failed items=%s", len(pending.get("items", [])))
                clob_momentum_pending.pop(done_key, None)
            if now_mono >= event_refresh_at:
                event_cache = []
                for target in resolve_event_target_dates(config):
                    event_cache.extend(discover_temperature_events(config, target))
                station_groups = {}
                for event in event_cache:
                    city = event["_parsed_city"]
                    event_url = poly_url_from_event(event)
                    try:
                        station = station_for_event(config, city, event_url)
                    except Exception:
                        LOGGER.exception("weatherrecord station discovery failed city=%s url=%s", city, event_url)
                        continue
                    if not station:
                        continue
                    group_key = (city, station)
                    group = station_groups.setdefault(group_key, {"city": city, "station": station, "events": []})
                    group["events"].append(event)
                    if group_key not in station_state:
                        spread = min(timing_stagger_seconds, timing_refresh_seconds)
                        station_state[group_key] = {
                            "next_timing_refresh_at": now_mono + stable_stagger_seconds(f"{city}:{station}:awc_timing", spread)
                        }
                for stale_key in set(station_state) - set(station_groups):
                    station_state.pop(stale_key, None)
                    awc_timing_pending.pop(stale_key, None)
                event_refresh_at = now_mono + 300
                LOGGER.info("weatherrecord event cache refreshed events=%s station_groups=%s", len(event_cache), len(station_groups))

            for group_key, group in station_groups.items():
                state = station_state.setdefault(group_key, {"next_timing_refresh_at": now_mono})
                if now_mono < float(state.get("next_timing_refresh_at", 0.0)):
                    continue
                if group_key in awc_timing_pending:
                    continue
                city = str(group["city"])
                station = str(group["station"])
                state["next_timing_refresh_at"] = now_mono + timing_refresh_seconds
                awc_timing_pending[group_key] = {
                    "future": HTTP_EXECUTOR.submit(aviation_report_timing, station, 24, config),
                    "city": city,
                    "station": station,
                }
                LOGGER.info("weatherrecord timing refresh queued city=%s station=%s", city, station)

            window_groups = []
            weather_record_groups = []
            for group_key, group in station_groups.items():
                in_weather_record_window, weather_timing = in_station_weather_record_window(config, str(group["city"]), str(group["station"]))
                if in_weather_record_window:
                    weather_record_groups.append((group_key, group, weather_timing))
                in_window, timing = in_station_report_window(config, str(group["city"]), str(group["station"]))
                if in_window:
                    window_groups.append((group_key, group, timing))
            active_tgftp_keys: set[str] = set()
            eligible_tgftp_groups = []
            now_utc = datetime.now(timezone.utc)
            for group_key, group, timing in window_groups:
                expected_dt = parse_optional_utc_datetime(timing.get("expected_next_obs_utc"))
                if expected_dt is None:
                    continue
                window_key = f"{group['city']}:{group['station']}:{expected_dt.isoformat()}"
                active_tgftp_keys.add(window_key)
                state = tgftp_window_state.setdefault(window_key, {"done": False, "attempts": 0})
                if state.get("done"):
                    continue
                if now_utc < expected_dt + timedelta(seconds=tgftp_start_delay_seconds):
                    continue
                eligible_tgftp_groups.append((window_key, group, timing, expected_dt))
            for stale_key in set(tgftp_window_state) - active_tgftp_keys:
                tgftp_window_state.pop(stale_key, None)
                for request_key, pending in list(tgftp_pending.items()):
                    if pending.get("window_key") == stale_key:
                        tgftp_pending.pop(request_key, None)

            active_clob_keys: set[str] = set()
            eligible_clob_groups: list[tuple[str, dict[str, Any]]] = []
            if clob_poll_enabled and window_groups:
                trades_snapshot = read_trades(config["outputs"]["trades_csv"])
                strategy_name = str(config["trading"]["strategy_name"])
                for _, group, timing in window_groups:
                    city = str(group["city"])
                    station = str(group["station"])
                    expected_obs_utc = str(timing.get("expected_next_obs_utc") or "")
                    if not expected_obs_utc:
                        continue
                    for event in list(group["events"]):
                        kind = str(event["_parsed_kind"])
                        event_date = str(event["_parsed_event_date"])
                        event_url = poly_url_from_event(event)
                        window_key = f"{city}:{station}:{kind}:{event_date}:{expected_obs_utc}:clob_no"
                        active_clob_keys.add(window_key)
                        state = clob_momentum_state.setdefault(window_key, {"done": False, "attempts": 0, "next_poll_at": now_mono})
                        if state.get("done"):
                            continue
                        if open_no_trade_exists_for_event(trades_snapshot, strategy_name, city, kind, event_date):
                            state["done"] = True
                            continue
                        observed_state = event_state.get((event_date, city, kind), EXTREMES_BY_EVENT.get((event_date, city, kind), {}))
                        observed_high = observed_state.get("observed_high", observed_state.get("aviation_high"))
                        observed_low = observed_state.get("observed_low", observed_state.get("aviation_low"))
                        observed_high = float(observed_high) if observed_high is not None else None
                        observed_low = float(observed_low) if observed_low is not None else None
                        observed_signature = f"{observed_high}:{observed_low}"
                        if state.get("observed_signature") != observed_signature or not isinstance(state.get("market"), TemperatureMarket):
                            try:
                                markets = markets_for_event(config, event)
                                event_unit = event_market_unit(markets)
                                market = adjacent_no_momentum_market(markets, kind, observed_high, observed_low, event_unit)
                            except Exception:
                                LOGGER.exception("clob momentum candidate failed city=%s kind=%s station=%s event_date=%s", city, kind, station, event_date)
                                continue
                            if not market:
                                state["done"] = True
                                LOGGER.info("clob momentum skip no_candidate city=%s kind=%s station=%s event_date=%s", city, kind, station, event_date)
                                continue
                            asset_id = asset_id_for_market_side(market, "NO")
                            if not asset_id:
                                state["done"] = True
                                LOGGER.info("clob momentum skip no_asset city=%s kind=%s station=%s market=%s", city, kind, station, market.market_id)
                                continue
                            state.update(
                                {
                                    "city": city,
                                    "kind": kind,
                                    "event_date": event_date,
                                    "station": station,
                                    "expected_obs_utc": expected_obs_utc,
                                    "observed_signature": observed_signature,
                                    "observed_high": observed_high,
                                    "observed_low": observed_low,
                                    "market": market,
                                    "market_id": market.market_id,
                                    "asset_id": asset_id,
                                    "event_url": event_url,
                                    "event_unit": event_unit,
                                    "base_price": None,
                                    "target_price": None,
                                    "next_poll_at": now_mono,
                                }
                            )
                            LOGGER.info(
                                "clob momentum candidate city=%s kind=%s station=%s event_date=%s market=%s observed_high=%s observed_low=%s expected_obs_utc=%s question=%r",
                                city,
                                kind,
                                station,
                                event_date,
                                market.market_id,
                                observed_high,
                                observed_low,
                                expected_obs_utc,
                                market.market_question,
                            )
                        eligible_clob_groups.append((window_key, state))
            for stale_key in set(clob_momentum_state) - active_clob_keys:
                clob_momentum_state.pop(stale_key, None)
                for request_key, pending in list(clob_momentum_pending.items()):
                    if stale_key in pending.get("window_keys", set()):
                        clob_momentum_pending.pop(request_key, None)

            while weather_record_groups:
                try:
                    rows = WEATHER_RECORD_UPDATES.get_nowait()
                except queue.Empty:
                    break
                append_weather_record_history(config, rows, "weatherrecord_websocket")
                LOGGER.info("weatherrecord websocket processing rows=%s active_windows=%s", len(rows), len(weather_record_groups))
                for _, group, _ in weather_record_groups:
                    city = str(group["city"])
                    station = str(group["station"])
                    for event in list(group["events"]):
                        kind = event["_parsed_kind"]
                        event_date = event["_parsed_event_date"]
                        event_url = poly_url_from_event(event)
                        try:
                            markets = markets_for_event(config, event)
                            event_unit = event_market_unit(markets)
                            observed_high, observed_low, latest_dt, _ = weather_record_observed_extremes(
                                config,
                                station,
                                city,
                                event_date,
                                event_unit,
                                rows,
                            )
                            if latest_dt is None:
                                continue
                            key = (event_date, city, kind)
                            previous = event_state.get(key, {})
                            changed = (
                                (kind == "Highest" and observed_high is not None and observed_high != previous.get("observed_high")) or
                                (kind == "Lowest" and observed_low is not None and observed_low != previous.get("observed_low"))
                            )
                            event_state[key] = {
                                "observed_high": observed_high,
                                "observed_low": observed_low,
                                "latest_dt": latest_dt.isoformat(),
                            }
                            if not changed:
                                continue
                            LOGGER.info(
                                "weatherrecord extreme changed city=%s kind=%s station=%s event_date=%s high=%s low=%s latest=%s",
                                city,
                                kind,
                                station,
                                event_date,
                                observed_high,
                                observed_low,
                                latest_dt.isoformat(),
                            )
                            process_deterministic_harvest(
                                config,
                                {
                                    "source": "weatherrecord_websocket",
                                    "city": city,
                                    "kind": kind,
                                    "event_date": event_date,
                                    "polymarket_url": event_url,
                                    "aviation_high": observed_high,
                                    "aviation_low": observed_low,
                                    "observed_at_utc": latest_dt.astimezone(timezone.utc).isoformat(),
                                },
                            )
                        except Exception:
                            LOGGER.exception("weatherrecord event failed city=%s kind=%s station=%s", city, kind, station)
            next_tgftp_due = now_mono + fallback_poll_seconds
            if eligible_tgftp_groups:
                for window_key, group, timing, expected_dt in eligible_tgftp_groups:
                    state = tgftp_window_state.setdefault(window_key, {"done": False, "attempts": 0, "next_poll_at": now_mono})
                    if state.get("done"):
                        continue
                    window_next_poll_at = float(state.get("next_poll_at", now_mono))
                    if now_mono < window_next_poll_at:
                        next_tgftp_due = min(next_tgftp_due, window_next_poll_at)
                        continue
                    inflight = sum(1 for pending in tgftp_pending.values() if pending.get("window_key") == window_key)
                    if inflight >= tgftp_max_inflight:
                        state["next_poll_at"] = now_mono + tgftp_min_interval_seconds
                        next_tgftp_due = min(next_tgftp_due, float(state["next_poll_at"]))
                        continue
                    station = str(group["station"])
                    state["attempts"] = int(state.get("attempts", 0)) + 1
                    request_key = f"{window_key}:{state['attempts']}:{time.time_ns()}"
                    tgftp_pending[request_key] = {
                        "window_key": window_key,
                        "future": TGFTP_EXECUTOR.submit(tgftp_metar_observation, station),
                        "group": group,
                        "timing": timing,
                        "expected_dt": expected_dt,
                    }
                    state["next_poll_at"] = now_mono + tgftp_min_interval_seconds
                    next_tgftp_due = min(next_tgftp_due, float(state["next_poll_at"]))
                    LOGGER.info(
                        "tgftp window poll queued city=%s station=%s expected_obs_utc=%s attempts=%s inflight=%s interval_seconds=%s",
                        group.get("city"),
                        station,
                        expected_dt.isoformat(),
                        state["attempts"],
                        inflight + 1,
                        tgftp_min_interval_seconds,
                    )
                next_tgftp_poll_at = next_tgftp_due

            next_clob_due = now_mono + fallback_poll_seconds
            if eligible_clob_groups:
                batch_items: list[dict[str, Any]] = []
                batch_assets: list[str] = []
                for window_key, state in eligible_clob_groups:
                    if state.get("done"):
                        continue
                    window_next_poll_at = float(state.get("next_poll_at", now_mono))
                    if now_mono < window_next_poll_at:
                        next_clob_due = min(next_clob_due, window_next_poll_at)
                        continue
                    inflight = sum(1 for pending in clob_momentum_pending.values() if window_key in pending.get("window_keys", set()))
                    if inflight >= clob_max_inflight:
                        state["next_poll_at"] = now_mono + clob_poll_interval_seconds
                        next_clob_due = min(next_clob_due, float(state["next_poll_at"]))
                        continue
                    market = state.get("market")
                    if not isinstance(market, TemperatureMarket):
                        state["done"] = True
                        continue
                    state["attempts"] = int(state.get("attempts", 0)) + 1
                    asset_id = str(state.get("asset_id") or "")
                    if not asset_id:
                        state["done"] = True
                        continue
                    batch_items.append({
                        "window_key": window_key,
                        "city": state.get("city") or market.city,
                        "kind": state.get("kind") or market.kind,
                        "event_date": state.get("event_date") or market.event_date,
                        "station": state.get("station"),
                        "polymarket_url": state.get("event_url") or market.polymarket_url,
                        "market": market,
                        "market_id": market.market_id,
                        "asset_id": asset_id,
                        "expected_obs_utc": state.get("expected_obs_utc"),
                    })
                    batch_assets.append(asset_id)
                    state["next_poll_at"] = now_mono + clob_poll_interval_seconds
                    next_clob_due = min(next_clob_due, float(state["next_poll_at"]))
                    LOGGER.info(
                        "clob momentum poll staged city=%s kind=%s station=%s market=%s attempts=%s inflight=%s interval_seconds=%s base_price=%s target_price=%s",
                        state.get("city") or market.city,
                        state.get("kind") or market.kind,
                        state.get("station"),
                        market.market_id,
                        state["attempts"],
                        inflight + 1,
                        clob_poll_interval_seconds,
                        state.get("base_price"),
                        state.get("target_price"),
                    )
                if batch_items:
                    request_key = f"clob_batch:{len(batch_items)}:{time.time_ns()}"
                    clob_momentum_pending[request_key] = {
                        "future": CLOB_POLL_EXECUTOR.submit(clob_asset_sell_prices, config, batch_assets),
                        "items": batch_items,
                        "window_keys": {str(item["window_key"]) for item in batch_items},
                    }
                    LOGGER.info(
                        "clob momentum batch poll queued items=%s unique_assets=%s interval_seconds=%s pending_batches=%s",
                        len(batch_items),
                        len(set(batch_assets)),
                        clob_poll_interval_seconds,
                        len(clob_momentum_pending),
                    )
                next_clob_poll_at = next_clob_due

            sleep_seconds = fallback_poll_seconds
            if window_groups or weather_record_groups:
                wakeup_candidates = []
                if eligible_tgftp_groups:
                    wakeup_candidates.append(next_tgftp_poll_at)
                if eligible_clob_groups:
                    wakeup_candidates.append(next_clob_poll_at)
                if wakeup_candidates:
                    min_sleep_seconds = 0.005 if eligible_clob_groups else 0.01 if eligible_tgftp_groups else 0.25
                    sleep_seconds = max(min_sleep_seconds, min(wakeup_candidates) - time.monotonic())
                else:
                    sleep_seconds = 0.25
            elif station_groups:
                next_timing = min((float(s.get("next_timing_refresh_at", now_mono + fallback_poll_seconds)) for s in station_state.values()), default=now_mono + fallback_poll_seconds)
                sleep_seconds = min(fallback_poll_seconds, max(1.0, next_timing - now_mono), max(1.0, event_refresh_at - now_mono))
            time.sleep(min(30.0, sleep_seconds))
        except Exception:
            LOGGER.exception("weatherrecord supervisor loop failed")
            time.sleep(min(30, fallback_poll_seconds))


def start_weather_record_thread(config: dict[str, Any]) -> threading.Thread:
    """Start the weatherrecord websocket listener and processing supervisor."""
    listener = threading.Thread(
        target=weather_record_websocket_listener,
        args=(config,),
        name="weatherrecord-websocket",
        daemon=True,
    )
    listener.start()
    thread = threading.Thread(target=weather_record_supervisor, args=(config,), name="deterministic-weatherrecord", daemon=True)
    thread.start()
    return thread


def twc_verification_group_key(trade: PaperTrade) -> str:
    """Build a station/date key so TWC verification can be staggered by airport."""
    station = (trade.forecast_station or "").upper()
    if not station:
        station = f"trade:{trade.trade_id}"
    return f"{trade.city}:{station}:{trade.event_date}"


def trade_needs_twc_verification(config: dict[str, Any], trade: PaperTrade) -> bool:
    """Return whether an open trade should participate in staggered TWC verification."""
    if trade.status not in {"OPEN", "HEDGED"} or trade.strategy != str(config["trading"]["strategy_name"]):
        return False
    source_text = (trade.forecast_source or "").lower()
    return any(token in source_text for token in ("metar", "weatherrecord", "tgftp"))


def twc_verification_supervisor(config: dict[str, Any]) -> None:
    """Verify open positions with TWC on staggered per-airport schedules."""
    settings = config.get("price_momentum", {})
    interval = max(60, int(settings.get("twc_verify_interval_seconds", int(config["scheduler"].get("poll_interval_minutes", 15)) * 60)))
    stagger_seconds = max(1, int(settings.get("twc_verify_stagger_seconds", min(300, interval))))
    tick_seconds = max(1, int(settings.get("twc_verify_scheduler_tick_seconds", 10)))
    LOGGER.info(
        "twc verification supervisor started interval_seconds=%s stagger_seconds=%s tick_seconds=%s",
        interval,
        stagger_seconds,
        tick_seconds,
    )
    pending: dict[str, concurrent.futures.Future[Any]] = {}
    while True:
        try:
            now_mono = time.monotonic()
            for done_key, future in list(pending.items()):
                if not future.done():
                    continue
                try:
                    future.result()
                except Exception:
                    LOGGER.exception("twc verification task failed group=%s", done_key)
                pending.pop(done_key, None)
            trades = read_trades(config["outputs"]["trades_csv"])
            groups: dict[str, PaperTrade] = {}
            for trade in trades:
                if not trade_needs_twc_verification(config, trade):
                    continue
                groups.setdefault(twc_verification_group_key(trade), trade)
            for stale_key in set(TWC_VERIFY_NEXT_AT) - set(groups):
                TWC_VERIFY_NEXT_AT.pop(stale_key, None)
            for group_key, sample_trade in sorted(groups.items()):
                if group_key not in TWC_VERIFY_NEXT_AT:
                    TWC_VERIFY_NEXT_AT[group_key] = now_mono + stable_stagger_seconds(f"{group_key}:twc_verify", min(stagger_seconds, interval))
                if now_mono < TWC_VERIFY_NEXT_AT[group_key]:
                    continue
                if group_key in pending:
                    continue
                station = (sample_trade.forecast_station or "").upper()
                LOGGER.info(
                    "twc staggered verification queued group=%s city=%s station=%s event_date=%s",
                    group_key,
                    sample_trade.city,
                    station,
                    sample_trade.event_date,
                )
                pending[group_key] = TWC_VERIFY_EXECUTOR.submit(
                    verify_open_positions_with_twc,
                    config,
                    {
                        "source": "staggered_twc_position_verification",
                        "city": sample_trade.city,
                        "station": station,
                        "event_date": sample_trade.event_date,
                    },
                )
                TWC_VERIFY_NEXT_AT[group_key] = now_mono + interval
        except Exception:
            LOGGER.exception("twc verification supervisor loop failed")
        time.sleep(tick_seconds)


def start_twc_verification_thread(config: dict[str, Any]) -> threading.Thread:
    """Start staggered TWC position verification in a daemon thread."""
    thread = threading.Thread(target=twc_verification_supervisor, args=(config,), name="deterministic-twc-verify", daemon=True)
    thread.start()
    return thread


def write_state(config: dict[str, Any], cycle_num: int) -> None:
    """Persist lightweight bot loop state to JSON.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
        cycle_num (int): Current main-loop cycle number.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Overwrites the configured state JSON file.
    """
    state = {"cycle_num": cycle_num, "updated_at": datetime.now().isoformat(timespec="seconds"), "strategy_mode": config["trading"].get("strategy_mode")}
    with IO_LOCK, open(config["outputs"]["state_json"], "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=True)


def summarize_settled(config: dict[str, Any]) -> None:
    """Regenerate performance reports from current persisted trades.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        None: This function is executed for its side effects.
    """
    trades = read_trades(config["outputs"]["trades_csv"])
    if trades:
        write_performance_reports(config, trades)


def start_live_trader(config: dict[str, Any]) -> Optional[LiveTradingManager]:
    """Start the process-wide live trading manager when live trading is enabled.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        Optional[LiveTradingManager]: Started manager, or None when running in paper mode.
    """
    global LIVE_TRADER
    if not live_trading_enabled(config):
        LIVE_TRADER = None
        return None
    manager = LiveTradingManager(config)
    manager.start()
    LIVE_TRADER = manager
    return manager


def stop_live_trader() -> None:
    """Stop the process-wide live trading manager if it is running.
    
    Args:
        None.
    
    Returns:
        None: This function is executed for its side effects.
    """
    global LIVE_TRADER
    if LIVE_TRADER:
        LIVE_TRADER.stop()
        LIVE_TRADER = None


def run(config: dict[str, Any]) -> None:
    """Run the long-lived bot loop with supervisors, settlement, reporting, and state writes.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including API endpoints, trading rules, outputs, and scheduler settings.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Starts daemon supervisors and runs the main scheduler loop until interrupted or max_cycles is reached.
    """
    cycle_num = 0
    max_cycles = int(config["scheduler"].get("max_cycles", 0))
    LOGGER.info("bot started config=%s", json.dumps(redacted_config(config), ensure_ascii=False, sort_keys=True))
    start_live_trader(config)
    sync_polymarket_positions_to_disk(config, reason="start")
    start_source_station_guard_thread(config)
    start_model_awc_thread(config)
    try:
        while True:
            cycle_num += 1
            if config["scheduler"].get("settle_after_each_cycle", True):
                settle_open_trades(config)
                summarize_settled(config)
            write_state(config, cycle_num)
            if max_cycles and cycle_num >= max_cycles:
                break
            time.sleep(max(10, int(config["scheduler"].get("poll_interval_minutes", 15)) * 60))
    except KeyboardInterrupt:
        LOGGER.info("bot stopping by KeyboardInterrupt")
    finally:
        stop_live_trader()
        persist_positions_before_stop(config, reason="stop")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser for the script.
    
    Args:
        None.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser.
    """
    parser = argparse.ArgumentParser(description="Deterministic Polymarket weather paper trader")
    parser.add_argument("command", choices=["run", "once", "settle"], help="Command to run")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config JSON")
    return parser


def main() -> None:
    """Parse CLI arguments, load config, and dispatch the requested command.
    
    Args:
        None.
    
    Returns:
        None: This function is executed for its side effects.
    
    Side effects:
        Configures logging and performs the command requested on the CLI.
    """
    args = build_parser().parse_args()
    config = load_config(args.config)
    setup_logging(config)
    atexit.register(persist_positions_before_stop, config, reason="atexit")
    if args.command == "settle":
        sync_polymarket_positions_to_disk(config, reason="start")
        settle_open_trades(config)
        summarize_settled(config)
        return
    if args.command == "once":
        sync_polymarket_positions_to_disk(config, reason="start")
        settle_open_trades(config)
        summarize_settled(config)
        write_state(config, 1)
        return
    run(config)


if __name__ == "__main__":
    main()
