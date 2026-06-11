#!/usr/bin/env python3
"""
Deterministic Polymarket weather trader.

Only keeps the current strategy:
- Watch every temperature market YES/NO token per event over Polymarket websocket.
- Use price movement as the cheap trigger to refresh AviationWeather METAR.
- Verify changed observed extremes with TWC historical observations.
- Buy NO when an option is already impossible, buy YES when the extreme bucket is reached.
- Sell held NO only if corrected observations make that NO possible again.

By default this runs as a paper trader. If live trading is explicitly enabled
in config, real Polymarket orders are posted through executor.py and confirmed
through the authenticated Polymarket user websocket, with REST polling as a
fallback.
"""

from __future__ import annotations

import argparse
import atexit
import csv
import json
import logging
import os
import re
import threading
import time
from collections import Counter
from dataclasses import MISSING, asdict, dataclass, fields
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

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
    trading = {
        "strategy_name": "deterministic_harvest",
        "strategy_mode": "deterministic_harvest",
        "buy_notional_usdc": 5.0,
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
        "allowed_cities": allowed,
        "websocket_enabled": True,
        "websocket_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "websocket_persistent": True,
        "websocket_reconnect_seconds": 5,
        "websocket_ping_seconds": 10,
        "websocket_timeout_seconds": 10,
        "websocket_asset_refresh_seconds": 300,
        "live_trading_enabled": False,
        "live_trading_dry_run": False,
        "live_order_timeout_seconds": 20,
        "live_order_check_seconds": 1,
        "depth_price_notional_multiplier": 2.0,
        "depth_price_extra_levels": 1,
    }
    return {
    "api": {
        "polymarket_gamma_base": "https://gamma-api.polymarket.com",
        "polymarket_data_base": "https://data-api.polymarket.com",
        "polymarket_clob_base": "https://clob.polymarket.com",
        "weather_company_base": "https://api.weather.com",
            "twc_api_key_env": "TWC_API_KEY",
            "twc_api_key": "",
            "twc_units": "e",
            "request_timeout_seconds": 30,
            "per_request_delay_seconds": 0.25,
        },
        "events": {"target_dates": ["today"], "city_filter": "", "allowed_cities": allowed, "include_closed": False, "max_offsets": 1200, "city_timezones": city_timezones},
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
            "tgftp_verify_interval_seconds": 10,
            "tgftp_verify_timeout_seconds": 180,
            "twc_verify_interval_seconds": 900,
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
    deterministic = [s for s in strategies if s.get("trading", {}).get("strategy_mode") == "deterministic_harvest"]
    if not deterministic:
        if config.get("trading", {}).get("strategy_mode") == "deterministic_harvest":
            return config
        raise RuntimeError("No enabled deterministic_harvest strategy found in config.")
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
    return active_config(deep_merge(default_config(), user_config))


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
    level = getattr(logging, str(outputs.get("log_level", "INFO")).upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.FileHandler(outputs["log_file"], encoding="utf-8")]
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
    notional = float(trading.get("buy_notional_usdc", 5.0))
    multiplier = max(1.0, float(trading.get("depth_price_notional_multiplier", 2.0)))
    return max(notional, notional * multiplier)


def depth_extra_levels(config: dict[str, Any]) -> int:
    """Return how many levels beyond the target depth to step for buy aggressiveness."""
    try:
        return max(0, int(float(config.get("trading", {}).get("depth_price_extra_levels", 1))))
    except (TypeError, ValueError):
        return 1


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


def best_buy_price(config: dict[str, Any], market: TemperatureMarket, side: str) -> Optional[float]:
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
        target_notional = depth_target_notional(config)
        extra_levels = depth_extra_levels(config)
        direct_bids, direct_asks = clob_book_levels(config, direct_asset_id)
        complement_bids, complement_asks = clob_book_levels(config, complement_asset_id)
        direct_bid = max((price for price, _ in direct_bids), default=None)
        direct_ask = min((price for price, _ in direct_asks), default=None)
        complement_bid = max((price for price, _ in complement_bids), default=None)
        complement_ask = min((price for price, _ in complement_asks), default=None)
        direct_depth_price = price_for_buy_notional(direct_asks, target_notional, extra_levels)
        complement_depth_price = price_for_complement_bid_notional(complement_bids, target_notional, extra_levels)
        complement_bid_as_buy = clamp_price(1.0 - complement_bid) if complement_bid is not None else None
        prices = [p for p in (direct_depth_price, complement_depth_price) if p is not None]
        if not prices:
            prices = [p for p in (direct_ask, complement_bid_as_buy) if p is not None]
        if not prices:
            LOGGER.info(
                "clob buy unavailable side=%s market=%s direct_asset=%s direct_bids=%s direct_asks=%s direct_best_bid=%s direct_best_ask=%s complement_asset=%s complement_bids=%s complement_asks=%s complement_best_bid=%s complement_best_ask=%s target_notional=%s extra_levels=%s question=%r",
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
                target_notional,
                extra_levels,
                market.market_question,
            )
            return None
        selected = min(prices)
        LOGGER.info(
            "clob depth buy price side=%s market=%s selected=%s direct_depth=%s complement_depth=%s direct_best_ask=%s complement_bid_as_buy=%s target_notional=%s extra_levels=%s",
            normalized,
            market.market_id,
            selected,
            direct_depth_price,
            complement_depth_price,
            direct_ask,
            complement_bid_as_buy,
            target_notional,
            extra_levels,
        )
        return selected
    except Exception:
        LOGGER.exception("clob buy query failed side=%s market=%s question=%r", normalized, market.market_id, market.market_question)
        return None


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


def append_aviation_metar_history(config: dict[str, Any], station: str, rows: list[dict[str, Any]], reason: str) -> None:
    """Append raw AviationWeather METAR rows to a JSONL audit file.
    
    Args:
        config (dict[str, Any]): Active bot configuration, including output paths.
        station (str): Weather station identifier.
        rows (list[dict[str, Any]]): Raw AviationWeather METAR rows.
        reason (str): Caller reason for the fetch.
    
    Returns:
        None: This function is executed for its side effects.
    """
    path = str(config.get("outputs", {}).get("aviation_metar_history_jsonl") or "")
    if not path or not rows:
        return
    with IO_LOCK, open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps({"saved_at": datetime.now(timezone.utc).isoformat(), "station": station, "reason": reason, "row": row}, ensure_ascii=False, sort_keys=True) + "\n")


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


def tgftp_metar_observation(station: str) -> Optional[dict[str, Any]]:
    """Fetch and parse the latest NOAA TGFTP station METAR.
    
    Args:
        station (str): Weather station identifier, usually a four-character ICAO code.
    
    Returns:
        Optional[dict[str, Any]]: Parsed latest station observation.
    
    Side effects:
        Calls the NOAA TGFTP endpoint.
    """
    url = f"https://tgftp.nws.noaa.gov/data/observations/metar/stations/{station.upper()}.TXT"
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return parse_tgftp_metar(r.text)


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
            station = station_from_wu_url(extract_wunderground_source(config, event_url))
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


def momentum_target_price(base_price: float, fraction: float) -> float:
    """Return the price needed after moving a fraction of the remaining path to 1.0."""
    base = min(0.999999, max(0.0, float(base_price)))
    move_fraction = min(1.0, max(0.0, float(fraction)))
    return base + move_fraction * (1.0 - base)


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
        self._pending: dict[str, LivePendingOrder] = {}
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

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
        from polymarket_ws import PolymarketUserFeed

        self.executor = Executor(
            private_key=private_key,
            safe_address=safe_address,
            dry_run=dry_run,
            signature_type=signature_type,
            funder_address=funder_address,
        )
        if not self.executor.initialize():
            raise RuntimeError("executor.initialize() failed; live trading is not available")
        self.user_feed = PolymarketUserFeed(self.executor.get_api_creds(), on_message=self._on_user_order_message)
        self.user_feed.start()
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, name="polymarket-live-orders", daemon=True)
        self._thread.start()
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
        if self._thread:
            self._thread.join(timeout=2)

    def submit_buy_trade(self, config: dict[str, Any], cycle_id: str, market: TemperatureMarket, wu_source: str, station: str, side: str, entry_price: float, observed_high: Optional[float], observed_low: Optional[float], reason: str) -> Optional[PaperTrade]:
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
        amount = float(config["trading"]["buy_notional_usdc"])
        result = self.executor.place_buy_order(token_id, amount, price=entry_price)
        if not _result_value(result, "success", False):
            LOGGER.error("live buy rejected side=%s market=%s price=%s error=%s", side, market.market_id, entry_price, _result_value(result, "error", ""))
            return None

        trade = make_trade(config, cycle_id, market, wu_source, station, side, float(_result_value(result, "price", entry_price)), observed_high, observed_low, reason)
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
        LOGGER.info("live buy pending trade=%s order=%s side=%s market=%s price=%s shares=%s", trade.trade_id, trade.live_buy_order_id, side, market.market_id, trade.yes_price, trade.shares)
        notify_trade(config, trade, "BUY", "SUBMITTED", reason)
        return trade

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
        result = self.executor.place_sell_order(token_id, trade.shares, price=exit_price)
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
        check_seconds = max(0.5, float(self.config.get("trading", {}).get("live_order_check_seconds", 1)))
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


def make_trade(config: dict[str, Any], cycle_id: str, market: TemperatureMarket, wu_source: str, station: str, side: str, entry_price: float, observed_high: Optional[float], observed_low: Optional[float], reason: str) -> PaperTrade:
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
    notional = float(config["trading"]["buy_notional_usdc"])
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
    changed = False

    for trade in trades:
        if trade.status != "OPEN" or trade.strategy != strategy_name:
            continue
        if "metar" not in (trade.forecast_source or "").lower():
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
            station = trade.forecast_station or station_from_wu_url(extract_wunderground_source(config, trade.polymarket_url))
            if not station:
                continue
            city_local_dt, _, _ = city_local_now(config, trade.city)
            twc_high, twc_low, _, _ = deterministic_observed_extremes_from_twc(config, station, trade.event_date, city_local_dt, event_unit)
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
            if trade_observation_verified(trade, market, twc_high, twc_low, event_unit):
                if trade.settlement_source != "twc_verified_hold":
                    trade.settlement_source = "twc_verified_hold"
                    trade.error = ""
                    changed = True
                continue

            triggered_by_position_price = bool(trigger_market_id and trigger_market_id == trade.market_id)
            price_momentum_trade = "price_momentum" in (trade.forecast_source or "").lower()
            expired = trade_age_seconds(trade) >= verify_seconds
            if triggered_by_position_price or expired or price_momentum_trade:
                reason = "twc_invalidated_price_momentum" if price_momentum_trade else "twc_inconsistent_after_position_price_move" if triggered_by_position_price else "twc_not_verified_within_2h"
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
    wu_source = extract_wunderground_source(config, event_url)
    station = station_from_wu_url(wu_source)
    if not station:
        LOGGER.warning("deterministic skip missing station city=%s kind=%s url=%s", city, kind, event_url)
        return []
    observed_high = trigger_context.get("aviation_high")
    observed_low = trigger_context.get("aviation_low")
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
        "source": "aviation_metar",
        "observed_high": observed_high,
        "observed_low": observed_low,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }

    trades = read_trades(config["outputs"]["trades_csv"])
    strategy_name = str(config["trading"]["strategy_name"])
    markets_by_id = {m.market_id: m for m in markets}
    changed = False
    new_trades: list[PaperTrade] = []

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
                live_trader.submit_buy_trade(config, cycle_id, extreme_market, wu_source, station, "YES", price, observed_high, observed_low, "metar_extreme_yes_reached")
                if live_trader
                else make_trade(config, cycle_id, extreme_market, wu_source, station, "YES", price, observed_high, observed_low, "metar_extreme_yes_reached")
            )
            if trade:
                new_trades.append(trade)
                changed = True
                if not live_trader:
                    notify_trade(config, trade, "BUY", "FILLED", "metar_extreme_yes_reached")
                LOGGER.info("metar buy_yes city=%s kind=%s market=%s price=%s observed_high=%s observed_low=%s status=%s", city, kind, extreme_market.market_id, price, observed_high, observed_low, trade.status)
        else:
            LOGGER.info("metar skip_buy_yes_price_too_high city=%s kind=%s market=%s yes_clob_buy_price=%s max=%s observed_high=%s observed_low=%s", city, kind, extreme_market.market_id, price, yes_max, observed_high, observed_low)

    if not extreme_reached:
        for market in ordered:
            if not deterministic_market_impossible(market, kind, observed_high, observed_low, event_unit):
                continue
            if open_trade_exists(trades + new_trades, strategy_name, market.market_id, "NO"):
                continue
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
                continue
            if no_price > no_max:
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
                continue
            trade = (
                live_trader.submit_buy_trade(config, cycle_id, market, wu_source, station, "NO", no_price, observed_high, observed_low, "metar_impossible_no")
                if live_trader
                else make_trade(config, cycle_id, market, wu_source, station, "NO", no_price, observed_high, observed_low, "metar_impossible_no")
            )
            if trade:
                new_trades.append(trade)
                changed = True
                if not live_trader:
                    notify_trade(config, trade, "BUY", "FILLED", "metar_impossible_no")
                LOGGER.info("metar buy_no city=%s kind=%s market=%s no_price=%s observed_high=%s observed_low=%s status=%s", city, kind, market.market_id, no_price, observed_high, observed_low, trade.status)
            break

    if new_trades:
        trades.extend(new_trades)
    if changed:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
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
    move_fraction = float(settings.get("move_to_one_fraction", settings.get("yes_change_pct" if side == "YES" else "no_change_pct", 0.30)))
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


def tgftp_validation_worker(config: dict[str, Any], trade_id: str, station: str, min_obs_utc: str) -> None:
    """Poll TGFTP until a new station report validates or rejects a price momentum trade."""
    settings = config.get("price_momentum", {})
    interval = max(1, int(settings.get("tgftp_verify_interval_seconds", 10)))
    timeout = max(interval, int(settings.get("tgftp_verify_timeout_seconds", 180)))
    station_key = station.upper()
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
        if close_trade(config, trade, market, "tgftp_invalidated_price_momentum"):
            LOGGER.info("tgftp validation sell trade=%s station=%s obs_utc=%s temp=%s high=%s low=%s raw=%r", trade_id, station, obs["obs_dt"].isoformat(), rounded_temp, observed_high, observed_low, obs["raw_ob"])
    write_csv(config["outputs"]["trades_csv"], trades)
    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)


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


def websocket_assets(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
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
            event_url = poly_url_from_event(event)
            try:
                station = station_from_wu_url(extract_wunderground_source(config, event_url))
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
        station = station_from_wu_url(extract_wunderground_source(config, event_url))
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
    refresh_seconds = int(config["trading"].get("websocket_asset_refresh_seconds", 300))
    trigger_pct = float(config["trading"].get("monitor_price_change_pct", 0.03))
    signal_fields = websocket_momentum_signal_fields(config)
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
        last_refresh = started
        last_ping = started
        while duration_seconds <= 0 or time.monotonic() - started < duration_seconds:
            ensure_price_recording_windows(config, assets)
            if time.monotonic() - last_ping >= ping_seconds:
                ws.send("PING")
                last_ping = time.monotonic()
            if time.monotonic() - last_refresh >= refresh_seconds:
                assets = websocket_assets(config)
                new_ids = sorted(assets)
                if new_ids and new_ids != asset_ids:
                    asset_ids = new_ids
                    ws.send(json.dumps({"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}))
                last_refresh = time.monotonic()
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
                price_change_pct = abs(float(price) - previous_price) / previous_price
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
                if price_change_pct >= trigger_pct or trade:
                    assets = websocket_assets(config)
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
                        station = station_from_wu_url(extract_wunderground_source(config, event_url))
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
    last_twc_verify_ts = 0.0
    LOGGER.info("bot started config=%s", json.dumps(redacted_config(config), ensure_ascii=False, sort_keys=True))
    initialize_tgftp_observation_cache(config)
    start_live_trader(config)
    sync_polymarket_positions_to_disk(config, reason="start")
    start_websocket_thread(config)
    start_aviation_thread(config)
    try:
        while True:
            cycle_num += 1
            if config["scheduler"].get("settle_after_each_cycle", True):
                twc_interval = max(60, int(config.get("price_momentum", {}).get("twc_verify_interval_seconds", int(config["scheduler"].get("poll_interval_minutes", 15)) * 60)))
                if time.time() - last_twc_verify_ts >= twc_interval:
                    verify_open_positions_with_twc(config, {"source": "scheduled_twc_position_verification"})
                    last_twc_verify_ts = time.time()
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
