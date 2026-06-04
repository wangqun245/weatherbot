#!/usr/bin/env python3
"""
Config-driven Polymarket weather paper trader using The Weather Company API.

Run:
  python polymarket_weather_paper_trader.py run --config polymarket_weather_config.json
  python polymarket_weather_paper_trader.py once --config polymarket_weather_config.json

This is a simulator. It never submits real orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import socket
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone, timedelta
from typing import Any, Iterable, Optional
from urllib.parse import urljoin
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

BASE_POLY = "https://polymarket.com"
DEFAULT_CONFIG_PATH = "polymarket_weather_config.json"
LOGGER = logging.getLogger("weatherbot")
STRATEGY4_PEAK_PNL_BY_TRADE: dict[str, float] = {}
IO_LOCK = threading.RLock()
US_WEATHER_CITIES = {
    "Atlanta",
    "Austin",
    "Chicago",
    "Dallas",
    "Denver",
    "Houston",
    "Los Angeles",
    "Miami",
    "NYC",
    "San Francisco",
    "Seattle",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

TEMP_TITLE_RE = re.compile(
    r"^(Highest|Lowest)\s+temperature\s+in\s+(.+?)\s+on\s+([A-Za-z]+\s+\d{1,2})(?:\?)?$",
    re.I,
)
TEMP_NUMBER_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:deg(?:rees?)?|\u00b0)?\s*([FC])?", re.I)
WU_URL_RE = re.compile(r"https?://(?:www\.)?wunderground\.com/[^\s\"'<)]+", re.I)


@dataclass
class TemperatureMarket:
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
    exit_at: str = ""
    exit_reason: str = ""
    exit_yes_price: Optional[float] = None
    exit_fee_usdc: float = 0.0
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


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def default_config() -> dict[str, Any]:
    return {
        "api": {
            "polymarket_gamma_base": "https://gamma-api.polymarket.com",
            "weather_company_base": "https://api.weather.com",
            "twc_api_key_env": "TWC_API_KEY",
            "twc_api_key": "",
            "twc_duration": "2day",
            "twc_units": "e",
            "twc_language": "en-US",
            "request_timeout_seconds": 30,
            "per_request_delay_seconds": 0.25,
        },
        "events": {
            "target_dates": ["today"],
            "city_filter": "",
            "allowed_cities": sorted(US_WEATHER_CITIES),
            "include_closed": False,
            "max_offsets": 1200,
            "city_timezones": {
                "London": "Europe/London",
                "Paris": "Europe/Paris",
                "Sao Paulo": "America/Sao_Paulo",
                "Buenos Aires": "America/Argentina/Buenos_Aires",
                "Seoul": "Asia/Seoul",
                "Toronto": "America/Toronto",
                "Seattle": "America/Los_Angeles",
                "NYC": "America/New_York",
                "Dallas": "America/Chicago",
                "Atlanta": "America/New_York",
                "Miami": "America/New_York",
                "Chicago": "America/Chicago",
                "Ankara": "Europe/Istanbul",
                "Wellington": "Pacific/Auckland",
                "Lucknow": "Asia/Kolkata",
                "Munich": "Europe/Berlin",
                "Tel Aviv": "Asia/Jerusalem",
                "Tokyo": "Asia/Tokyo",
                "Hong Kong": "Asia/Hong_Kong",
                "Shanghai": "Asia/Shanghai",
                "Singapore": "Asia/Singapore",
                "Milan": "Europe/Rome",
                "Madrid": "Europe/Madrid",
                "Warsaw": "Europe/Warsaw",
                "Taipei": "Asia/Taipei",
                "Chongqing": "Asia/Shanghai",
                "Beijing": "Asia/Shanghai",
                "Wuhan": "Asia/Shanghai",
                "Chengdu": "Asia/Shanghai",
                "Shenzhen": "Asia/Shanghai",
                "Austin": "America/Chicago",
                "Denver": "America/Denver",
                "Houston": "America/Chicago",
                "Los Angeles": "America/Los_Angeles",
                "San Francisco": "America/Los_Angeles",
                "Moscow": "Europe/Moscow",
                "Istanbul": "Europe/Istanbul",
                "Mexico City": "America/Mexico_City",
                "Busan": "Asia/Seoul",
                "Amsterdam": "Europe/Amsterdam",
                "Helsinki": "Europe/Helsinki",
                "Panama City": "America/Panama",
                "Kuala Lumpur": "Asia/Kuala_Lumpur",
                "Jeddah": "Asia/Riyadh",
                "Cape Town": "Africa/Johannesburg",
                "Guangzhou": "Asia/Shanghai",
                "Qingdao": "Asia/Shanghai",
                "Karachi": "Asia/Karachi",
                "Manila": "Asia/Manila",
            },
        },
        "trading": {
            "strategy_name": "twc_every_15m_most_likely",
            "strategy_mode": "intraday_reactive",
            "buy_notional_usdc": 5.0,
            "mispricing_price_threshold": 0.5,
            "fee_rate": 0.05,
            "fee_enabled": True,
            "one_trade_per_event_per_cycle": True,
            "time_windows_enabled": True,
            "lowest_local_hour_window": "0-6",
            "highest_local_hour_window": "12-18",
            "forecast_horizon_hours": 6,
            "forecast_scope": "next_hours_plus_observed",
            "include_observed_today": True,
        },
        "scheduler": {
            "poll_interval_minutes": 15,
            "align_to_top_of_hour": False,
            "run_once": False,
            "max_cycles": 0,
            "settle_after_each_cycle": True,
            "stop_when_all_target_events_settled": False,
        },
        "outputs": {
            "trades_csv": "polymarket_weather_trades.csv",
            "snapshots_csv": "polymarket_weather_forecast_snapshots.csv",
            "settled_trades_csv": "polymarket_weather_trades_settled.csv",
            "performance_by_cycle_csv": "polymarket_weather_performance_by_cycle.csv",
            "performance_by_event_csv": "polymarket_weather_performance_by_event.csv",
            "twc_raw_wide_csv": "polymarket_weather_twc_raw_wide.csv",
            "polymarket_price_snapshots_csv": "polymarket_weather_price_snapshots.csv",
            "polymarket_websocket_raw_jsonl": "polymarket_weather_websocket_raw.jsonl",
            "state_json": "polymarket_weather_state.json",
            "log_file": "bot.log",
            "log_level": "INFO",
            "console_log_enabled": False,
        },
        "polymarket_price_snapshots": {
            "enabled": True,
            "run_every_minutes": 1,
            "target_dates": ["today"],
            "time_windows_enabled": True,
            "lowest_local_hour_window": "0-6",
            "highest_local_hour_window": "12-18",
        },
        "twc_raw_collection": {
            "enabled": True,
            "run_every_minutes": 15,
            "target_dates": ["today"],
            "time_windows_enabled": True,
            "lowest_local_hour_window": "0-6",
            "highest_local_hour_window": "12-18",
            "forecast_scope": "next_hours_plus_observed",
            "forecast_horizon_hours": 6,
            "include_observed_today": True,
            "strategy_name": "twc_raw_collector",
        },
        "strategies": [
            {
                "name": "intraday_reactive",
                "enabled": True,
                "run_every_minutes": 15,
                "align_to_top_of_hour": False,
                "events": {"target_dates": ["today"]},
                "trading": {
                    "strategy_name": "intraday_reactive",
                    "strategy_mode": "intraday_reactive",
                    "mispricing_price_threshold": 0.5,
                    "time_windows_enabled": True,
                    "lowest_local_hour_window": "0-6",
                    "highest_local_hour_window": "12-18",
                    "forecast_scope": "next_hours_plus_observed",
                    "forecast_horizon_hours": 6,
                    "include_observed_today": True,
                },
            },
            {
                "name": "tomorrow_mispricing",
                "enabled": True,
                "run_every_minutes": 60,
                "align_to_top_of_hour": True,
                "events": {"target_dates": ["tomorrow"]},
                "trading": {
                    "strategy_name": "tomorrow_mispricing",
                    "strategy_mode": "tomorrow_mispricing",
                    "mispricing_price_threshold": 0.5,
                    "time_windows_enabled": True,
                    "tomorrow_mispricing_local_hour_window": "12-24",
                    "forecast_scope": "event_day_full",
                    "forecast_horizon_hours": 6,
                    "include_observed_today": False,
                },
            },
            {
                "name": "dual_bracket_fixed_time",
                "enabled": True,
                "run_every_minutes": 15,
                "align_to_top_of_hour": False,
                "events": {"target_dates": ["today"]},
                "trading": {
                    "strategy_name": "dual_bracket_fixed_time",
                    "strategy_mode": "dual_bracket_fixed_time",
                    "mispricing_price_threshold": 0.5,
                    "time_windows_enabled": True,
                    "dual_bracket_lowest_local_hour": 0,
                    "dual_bracket_lowest_local_minute": 30,
                    "dual_bracket_highest_local_hour": 12,
                    "dual_bracket_highest_local_minute": 30,
                    "dual_bracket_max_markets_per_event": 2,
                    "dual_bracket_allow_repeat_buys": False,
                    "forecast_scope": "event_day_full",
                    "forecast_horizon_hours": 24,
                    "include_observed_today": False,
                },
            },
            {
                "name": "twc_reprice_momentum",
                "enabled": True,
                "run_every_minutes": 15,
                "align_to_top_of_hour": False,
                "events": {"target_dates": ["today"]},
                "trading": {
                    "strategy_name": "twc_reprice_momentum",
                    "strategy_mode": "twc_reprice_momentum",
                    "mispricing_price_threshold": 0.5,
                    "max_buy_yes_price": 0.5,
                    "monitor_price_change_trigger": 0.05,
                    "stop_loss_fraction_of_cost": 0.25,
                    "profit_drawdown_fraction": 0.3,
                    "profit_drawdown_min_profit_usdc": 1.0,
                    "websocket_peak_price_fields": ["price", "best_bid", "bid", "last_trade_price"],
                    "allowed_cities": sorted(US_WEATHER_CITIES),
                    "websocket_persistent": True,
                    "websocket_reconnect_seconds": 5,
                    "websocket_enabled": True,
                    "websocket_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                    "websocket_ping_seconds": 10,
                    "websocket_timeout_seconds": 10,
                    "lowest_local_start": "00:15",
                    "highest_local_start": "12:15",
                    "lowest_local_end": "06:00",
                    "highest_local_end": "18:00",
                    "forecast_scope": "event_day_full",
                    "forecast_horizon_hours": 24,
                    "include_observed_today": False,
                },
            },
        ],
    }


def strategy4_only_config(config: dict[str, Any]) -> dict[str, Any]:
    strategies = [
        strategy
        for strategy in config.get("strategies", [])
        if strategy.get("trading", {}).get("strategy_mode") == "twc_reprice_momentum"
    ]
    if not strategies:
        raise RuntimeError("No strategy4/twc_reprice_momentum strategy found in config.")

    config["strategies"] = strategies[:1]
    config["trading"] = deep_merge(config.get("trading", {}), strategies[0].get("trading", {}))
    config["polymarket_price_snapshots"]["enabled"] = False
    config["twc_raw_collection"]["enabled"] = False
    return config


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        user_config = json.load(f)
    return strategy4_only_config(deep_merge(default_config(), user_config))


def setup_logging(config: dict[str, Any]) -> None:
    level_name = str(config["outputs"].get("log_level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_file = str(config["outputs"].get("log_file", "bot.log"))
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    LOGGER.setLevel(level)
    LOGGER.handlers.clear()
    LOGGER.propagate = False

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)

    if config["outputs"].get("console_log_enabled", False):
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        LOGGER.addHandler(console_handler)


def log_info(message: str) -> None:
    LOGGER.info(message)


def redacted_config(config: dict[str, Any]) -> dict[str, Any]:
    def redact(value: Any, key: str = "") -> Any:
        key_lower = key.lower()
        if key_lower.endswith("_env"):
            return value
        if any(secret_word in key_lower for secret_word in ("key", "token", "secret", "password")):
            return "***REDACTED***" if value else ""
        if isinstance(value, dict):
            return {k: redact(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v, key) for v in value]
        return value

    return redact(config)


def resolve_date(value: str) -> date:
    lowered = value.lower().strip()
    if lowered == "today":
        return date.today()
    if lowered == "tomorrow":
        return date.today() + timedelta(days=1)
    return datetime.strptime(value, "%Y-%m-%d").date()


def infer_year(month_day_text: str, today: Optional[date] = None) -> date:
    today = today or date.today()
    parsed = datetime.strptime(f"{month_day_text} {today.year}", "%B %d %Y").date()
    if parsed < today - timedelta(days=2):
        parsed = datetime.strptime(f"{month_day_text} {today.year + 1}", "%B %d %Y").date()
    return parsed


def http_get_json(url: str, params: Optional[dict[str, Any]], timeout: int) -> Any:
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_get_text(url: str, timeout: int) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def gamma_get(config: dict[str, Any], path: str, params: Optional[dict[str, Any]] = None) -> Any:
    timeout = int(config["api"]["request_timeout_seconds"])
    return http_get_json(f"{config['api']['polymarket_gamma_base']}{path}", params, timeout)


def twc_get(config: dict[str, Any], path: str, params: dict[str, Any]) -> Any:
    env_name = str(config["api"].get("twc_api_key_env", "TWC_API_KEY")).strip()
    api_key = os.environ.get(env_name, "").strip() if env_name else ""
    if not api_key:
        api_key = str(config["api"].get("twc_api_key", "")).strip()
    if not api_key:
        raise RuntimeError(f"Missing Weather Company API key. Set environment variable {env_name!r} or config api.twc_api_key.")
    timeout = int(config["api"]["request_timeout_seconds"])
    query = {"apiKey": api_key, **params}
    return http_get_json(f"{config['api']['weather_company_base']}{path}", query, timeout)


def parse_jsonish(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes"}


def parse_event_title(title: str) -> Optional[tuple[str, str, str]]:
    m = TEMP_TITLE_RE.match(" ".join(str(title).split()))
    if not m:
        return None
    return m.group(1).title(), m.group(2).strip(), m.group(3).strip()


def poly_url_from_event(event: dict[str, Any]) -> str:
    slug = event.get("slug") or event.get("ticker") or event.get("id", "")
    return urljoin(BASE_POLY, f"/event/{slug}") if slug else BASE_POLY


def discover_temperature_events(config: dict[str, Any], target: date) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    city_filter = str(config["events"].get("city_filter") or "").lower()
    allowed_cities = {
        str(city)
        for city in (
            config["events"].get("allowed_cities")
            or config["trading"].get("allowed_cities")
            or []
        )
        if str(city)
    }
    max_offsets = int(config["events"]["max_offsets"])
    queries = [
        {"tag_slug": "weather"},
        {"tag_id": 100215},
        {"q": "Highest temperature"},
        {"q": "Lowest temperature"},
    ]

    for base_params in queries:
        for offset in range(0, max_offsets, 100):
            params = {"limit": 100, "offset": offset, **base_params}
            if not config["events"]["include_closed"]:
                params["closed"] = "false"
                params["archived"] = "false"
            try:
                batch = gamma_get(config, "/events", params)
            except requests.HTTPError:
                if "tag_id" in base_params:
                    break
                raise

            if not isinstance(batch, list) or not batch:
                break

            for event in batch:
                title = event.get("title") or event.get("question") or ""
                parsed = parse_event_title(title)
                if not parsed:
                    continue
                kind, city, md = parsed
                event_date = infer_year(md)
                if event_date != target:
                    continue
                if city_filter and city_filter not in city.lower():
                    continue
                if allowed_cities and city not in allowed_cities:
                    continue
                event["_parsed_kind"] = kind
                event["_parsed_city"] = city
                event["_parsed_event_date"] = event_date.isoformat()
                found[str(event.get("id") or event.get("slug"))] = event

            if len(batch) < 100:
                break
            time.sleep(float(config["api"]["per_request_delay_seconds"]))

    return list(found.values())


def extract_wunderground_source(config: dict[str, Any], event_url: str) -> str:
    html = http_get_text(event_url, int(config["api"]["request_timeout_seconds"]))
    urls = [u.rstrip(".,") for u in WU_URL_RE.findall(html)]
    return urls[0] if urls else ""


def station_from_wu_url(url: str) -> str:
    parts = [p for p in url.split("?")[0].rstrip("/").split("/") if p]
    if not parts:
        return ""
    if "date" in parts:
        parts = parts[: parts.index("date")]
    station = parts[-1].upper()
    return station if re.fullmatch(r"[A-Z0-9]{4}", station) else ""


def infer_temperature_unit(text: str, default_unit: str = "F") -> str:
    normalized = text.lower()
    if "celsius" in normalized or "centigrade" in normalized or "°c" in normalized or "℃" in normalized:
        return "C"
    if "fahrenheit" in normalized or "°f" in normalized or "℉" in normalized:
        return "F"
    return default_unit.upper()


def parse_temperature_rule(text: str, default_unit: str = "F") -> tuple[Optional[float], Optional[float], str]:
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

    if len(values) >= 2 and (
        re.search(r"\bbetween\b|\bfrom\b|\bto\b|\bthrough\b|\brange\b", low_text)
        or re.search(r"\d\s*-\s*\d", low_text)
    ):
        a, b = values[0], values[1]
        return min(a, b), max(a, b), unit

    v = values[0]
    if re.search(r"\b(at\s+or\s+above|or\s+higher|or\s+more|at\s+least|above|over|greater\s+than)\b", low_text):
        return v, None, unit
    if re.search(r"\b(at\s+or\s+below|or\s+lower|or\s+less|at\s+most|below|under|less\s+than)\b", low_text):
        return None, v, unit
    return v, v, unit


def outcome_price(market: dict[str, Any], outcome_name: str) -> Optional[float]:
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
    markets = event.get("markets") or []
    if not markets and event.get("slug"):
        detail = gamma_get(config, f"/events/slug/{event['slug']}")
        markets = detail.get("markets") or []
    if not markets and event.get("id"):
        detail = gamma_get(config, f"/events/{event['id']}")
        markets = detail.get("markets") or []

    title = event.get("title") or event.get("question") or ""
    parsed: list[TemperatureMarket] = []
    for market in markets:
        question = market.get("question") or market.get("title") or title
        unit_context = " ".join(
            str(market.get(field) or "")
            for field in ("question", "title", "description", "resolutionSource", "rules")
        )
        market_unit = infer_temperature_unit(unit_context or question)
        rule_min, rule_max, unit = parse_temperature_rule(question, default_unit=market_unit)
        yes_price = outcome_price(market, "Yes")
        parsed.append(
            TemperatureMarket(
                event_id=str(event.get("id") or event.get("slug") or ""),
                market_id=str(market.get("id") or market.get("slug") or ""),
                condition_id=str(market.get("conditionId") or ""),
                city=str(event.get("_parsed_city") or ""),
                kind=str(event.get("_parsed_kind") or ""),
                event_date=str(event.get("_parsed_event_date") or ""),
                event_title=title,
                market_question=question,
                polymarket_url=poly_url_from_event(event),
                yes_price=yes_price,
                rule_min=rule_min,
                rule_max=rule_max,
                unit=unit,
                closed=parse_bool(market.get("closed")),
                raw_market_json=json.dumps(market, ensure_ascii=False, sort_keys=True),
            )
        )
    return parsed


def parse_twc_local_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if re.search(r"[+-]\d{4}$", value):
            value = value[:-5] + value[-5:-2] + ":" + value[-2:]
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def first_twc_local_time(payload: dict[str, Any]) -> Optional[datetime]:
    for raw_time in payload.get("validTimeLocal") or []:
        parsed = parse_twc_local_time(str(raw_time))
        if parsed:
            return parsed
    return None


def parse_hour_window(value: str) -> tuple[int, int]:
    start_text, end_text = str(value).split("-", 1)
    start, end = int(start_text), int(end_text)
    if not 0 <= start <= 23 or not 0 <= end <= 24:
        raise ValueError(f"Invalid hour window: {value}")
    return start, end


def hour_in_window(hour: int, window: tuple[int, int]) -> bool:
    start, end = window
    if start == end:
        return True
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def trading_window_status(config: dict[str, Any], kind: str, local_dt: Optional[datetime]) -> tuple[bool, str, str]:
    if not config["trading"].get("time_windows_enabled", True):
        return True, "", "time_windows_disabled"
    if local_dt is None:
        return False, "", "missing_twc_local_time"

    if config["trading"].get("strategy_mode") == "tomorrow_mispricing":
        window_text = str(config["trading"].get("tomorrow_mispricing_local_hour_window", "12-24"))
    elif kind == "Lowest":
        window_text = str(config["trading"]["lowest_local_hour_window"])
    else:
        window_text = str(config["trading"]["highest_local_hour_window"])

    window = parse_hour_window(window_text)
    allowed = hour_in_window(local_dt.hour, window)
    reason = "inside_local_window" if allowed else f"outside_local_window_{window_text}"
    return allowed, window_text, reason


def city_local_now(config: dict[str, Any], city: str) -> tuple[Optional[datetime], str, str]:
    timezone_name = (config["events"].get("city_timezones") or {}).get(city, "")
    if not timezone_name:
        return None, "", "missing_city_timezone"
    try:
        return datetime.now(ZoneInfo(timezone_name)), timezone_name, "city_timezone"
    except ZoneInfoNotFoundError:
        return None, timezone_name, "invalid_city_timezone"


def fixed_time(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def local_time_text(value: Optional[datetime]) -> str:
    return fixed_time(value)


def event_market_unit(markets: list[TemperatureMarket]) -> str:
    counts: dict[str, int] = {}
    for market in markets:
        unit = (market.unit or "").upper()
        if unit in {"F", "C"} and (market.rule_min is not None or market.rule_max is not None):
            counts[unit] = counts.get(unit, 0) + 1
    if not counts:
        return "F"
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def twc_units_for_temperature_unit(unit: str) -> str:
    return "m" if unit.upper() == "C" else "e"


def twc_hourly_forecast_by_icao(config: dict[str, Any], icao_code: str, units: Optional[str] = None) -> dict[str, Any]:
    request_units = units or config["api"]["twc_units"]
    return twc_get(
        config,
        f"/v3/wx/forecast/hourly/{config['api']['twc_duration']}",
        {
            "icaoCode": icao_code,
            "units": request_units,
            "language": config["api"]["twc_language"],
            "format": "json",
        },
    )


def twc_historical_hourly_by_icao(config: dict[str, Any], icao_code: str, units: str) -> dict[str, Any]:
    return twc_get(
        config,
        "/v3/wx/conditions/historical/hourly/1day",
        {
            "icaoCode": icao_code,
            "units": units,
            "language": config["api"]["twc_language"],
            "format": "json",
        },
    )


def summarize_twc_daily_forecast(payload: dict[str, Any], event_date: str) -> tuple[Optional[float], Optional[float], str, str]:
    temps = payload.get("temperature") or []
    times = payload.get("validTimeLocal") or []
    matched: list[tuple[datetime, float]] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(temps) or temps[idx] is None:
            continue
        local_dt = parse_twc_local_time(str(raw_time))
        if local_dt and local_dt.date().isoformat() == event_date:
            matched.append((local_dt, float(temps[idx])))
    if not matched:
        return None, None, "", ""
    return (
        max(temp for _, temp in matched),
        min(temp for _, temp in matched),
        matched[0][0].isoformat(),
        matched[-1][0].isoformat(),
    )


def daily_twc_points(payload: dict[str, Any], event_date: str) -> list[tuple[datetime, float]]:
    temps = payload.get("temperature") or []
    times = payload.get("validTimeLocal") or []
    matched: list[tuple[datetime, float]] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(temps) or temps[idx] is None:
            continue
        local_dt = parse_twc_local_time(str(raw_time))
        if local_dt and local_dt.date().isoformat() == event_date:
            matched.append((local_dt, float(temps[idx])))
    return sorted(matched, key=lambda item: item[0])


def filtered_twc_points(
    payload: dict[str, Any],
    event_date: str,
    horizon_hours: int,
) -> list[tuple[datetime, float]]:
    temps = payload.get("temperature") or []
    times = payload.get("validTimeLocal") or []
    current_local = first_twc_local_time(payload)
    if current_local is None:
        return []
    horizon_end = current_local + timedelta(hours=horizon_hours)
    matched: list[tuple[datetime, float]] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(temps) or temps[idx] is None:
            continue
        local_dt = parse_twc_local_time(str(raw_time))
        if not local_dt:
            continue
        if local_dt.date().isoformat() != event_date:
            continue
        if current_local <= local_dt <= horizon_end:
            matched.append((local_dt, float(temps[idx])))
    return matched


def observed_twc_points(payload: dict[str, Any], event_date: str, current_local: Optional[datetime]) -> list[tuple[datetime, float]]:
    temps = payload.get("temperature") or []
    times = payload.get("validTimeLocal") or []
    matched: list[tuple[datetime, float]] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(temps) or temps[idx] is None:
            continue
        local_dt = parse_twc_local_time(str(raw_time))
        if not local_dt:
            continue
        if local_dt.date().isoformat() != event_date:
            continue
        if current_local is None or local_dt <= current_local:
            matched.append((local_dt, float(temps[idx])))
    return sorted(matched, key=lambda item: item[0])


def summarize_points(points: list[tuple[datetime, float]]) -> tuple[Optional[float], Optional[float], str, str, list[str], list[Any]]:
    if not points:
        return None, None, "", "", [], []
    ordered = sorted(points, key=lambda item: item[0])
    return (
        max(temp for _, temp in ordered),
        min(temp for _, temp in ordered),
        ordered[0][0].isoformat(),
        ordered[-1][0].isoformat(),
        [dt.isoformat() for dt, _ in ordered],
        [temp for _, temp in ordered],
    )


def merge_observed_and_forecast_points(
    observed: list[tuple[datetime, float]],
    forecast: list[tuple[datetime, float]],
) -> list[tuple[datetime, float]]:
    by_time: dict[str, tuple[datetime, float]] = {}
    for item in observed:
        by_time[item[0].isoformat()] = item
    for item in forecast:
        by_time.setdefault(item[0].isoformat(), item)
    return sorted(by_time.values(), key=lambda item: item[0])


def twc_raw_wide_row(
    *,
    cycle_id: str,
    strategy_name: str,
    target_date: date,
    city: str,
    kind: str,
    station: str,
    event_unit: str,
    twc_units: str,
    city_local_dt: Optional[datetime],
    city_timezone: str,
    observed_points: list[tuple[datetime, float]],
    forecast_points: list[tuple[datetime, float]],
    combined_points: list[tuple[datetime, float]],
    forecast_payload: dict[str, Any],
    historical_payload: dict[str, Any],
    event_url: str,
    wunderground_source_url: str,
    error: str = "",
) -> dict[str, Any]:
    high, low, first_local, last_local, _, _ = summarize_points(combined_points)
    row: dict[str, Any] = {
        "cycle_id": cycle_id,
        "strategy": strategy_name,
        "observed_at_utc": utc_now_text(),
        "city_local_time": local_time_text(city_local_dt),
        "city_timezone": city_timezone,
        "target_date": target_date.isoformat(),
        "city": city,
        "kind": kind,
        "station": station,
        "event_market_unit": event_unit,
        "twc_units_requested": twc_units,
        "temperature_unit": twc_forecast_unit_for_units(twc_units),
        "combined_high": high if high is not None else "",
        "combined_low": low if low is not None else "",
        "first_valid_time_local": first_local,
        "last_valid_time_local": last_local,
        "observed_point_count": len(observed_points),
        "forecast_point_count": len(forecast_points),
        "combined_point_count": len(combined_points),
        "polymarket_url": event_url,
        "wunderground_source_url": wunderground_source_url,
        "raw_forecast_payload_json": json.dumps(forecast_payload, ensure_ascii=False, sort_keys=True),
        "raw_historical_payload_json": json.dumps(historical_payload, ensure_ascii=False, sort_keys=True),
        "error": error,
    }
    observed_by_hour: dict[int, float] = {}
    for dt, temp in sorted(observed_points, key=lambda item: item[0]):
        if dt.date() == target_date:
            observed_by_hour[dt.hour] = temp
    for hour in range(24):
        row[f"observed_h{hour:02d}"] = observed_by_hour.get(hour, "")

    future_points = sorted(forecast_points, key=lambda item: item[0])
    if city_local_dt is not None:
        future_points = [item for item in future_points if item[0] >= city_local_dt]
    for idx in range(6):
        if idx < len(future_points):
            row[f"forecast_p{idx + 1:02d}"] = future_points[idx][1]
        else:
            row[f"forecast_p{idx + 1:02d}"] = ""
    return row


def append_twc_raw_wide(config: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    append_csv(str(config["outputs"].get("twc_raw_wide_csv", "polymarket_weather_twc_raw_wide.csv")), rows)


def summarize_twc_horizon_forecast(
    payload: dict[str, Any],
    event_date: str,
    horizon_hours: int,
) -> tuple[Optional[float], Optional[float], str, str, list[str], list[Any]]:
    return summarize_points(filtered_twc_points(payload, event_date, horizon_hours))


def twc_daily_series(payload: dict[str, Any], event_date: str) -> tuple[list[str], list[Any]]:
    times = payload.get("validTimeLocal") or []
    temps = payload.get("temperature") or []
    daily_times: list[str] = []
    daily_temps: list[Any] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(temps):
            continue
        local_dt = parse_twc_local_time(str(raw_time))
        if local_dt and local_dt.date().isoformat() == event_date:
            daily_times.append(str(raw_time))
            daily_temps.append(temps[idx])
    return daily_times, daily_temps


def twc_forecast_unit_for_units(units: str) -> str:
    return "F" if units == "e" else "C"


def twc_forecast_unit(config: dict[str, Any]) -> str:
    return twc_forecast_unit_for_units(config["api"]["twc_units"])


def convert_temperature(value: Optional[float], from_unit: str, to_unit: str) -> Optional[float]:
    if value is None:
        return None
    source = (from_unit or to_unit).upper()
    target = (to_unit or source).upper()
    if source == target:
        return value
    if source == "C" and target == "F":
        return value * 9.0 / 5.0 + 32.0
    if source == "F" and target == "C":
        return (value - 32.0) * 5.0 / 9.0
    return value


def comparable_rule_bounds(market: TemperatureMarket, target_unit: str) -> tuple[Optional[float], Optional[float], str]:
    market_unit = (market.unit or target_unit).upper()
    comparable_unit = target_unit.upper()
    return (
        convert_temperature(market.rule_min, market_unit, comparable_unit),
        convert_temperature(market.rule_max, market_unit, comparable_unit),
        comparable_unit,
    )


def market_distance(forecast: float, market: TemperatureMarket, forecast_unit: str) -> tuple[float, float, float]:
    lo, hi, _ = comparable_rule_bounds(market, forecast_unit)
    if lo is not None and forecast < lo:
        outside = lo - forecast
    elif hi is not None and forecast > hi:
        outside = forecast - hi
    else:
        outside = 0.0

    if lo is None and hi is None:
        center = 999.0
        width = 999.0
    elif lo is None:
        center = abs(forecast - hi)
        width = 999.0
    elif hi is None:
        center = abs(forecast - lo)
        width = 999.0
    else:
        center = abs(forecast - ((lo + hi) / 2.0))
        width = abs(hi - lo)
    return outside, center, width


def native_forecast_for_market(
    forecasts_by_unit: dict[str, dict[str, Optional[float]]],
    market: TemperatureMarket,
    kind: str,
) -> tuple[Optional[float], Optional[float], Optional[float], str]:
    unit = (market.unit or "F").upper()
    forecast = forecasts_by_unit.get(unit) or {}
    high = forecast.get("high")
    low = forecast.get("low")
    temp = high if kind == "Highest" else low
    return temp, high, low, unit


def canonical_market_distance(
    forecasts_by_unit: dict[str, dict[str, Optional[float]]],
    market: TemperatureMarket,
    kind: str,
) -> tuple[float, float, float]:
    native_temp, _, _, native_unit = native_forecast_for_market(forecasts_by_unit, market, kind)
    if native_temp is None:
        return 999.0, 999.0, 999.0
    comparable_temp = convert_temperature(native_temp, native_unit, "F")
    return market_distance(comparable_temp or native_temp, market, "F")


def choose_most_likely_market(
    markets: list[TemperatureMarket],
    forecasts_by_unit: dict[str, dict[str, Optional[float]]],
    kind: str,
) -> Optional[TemperatureMarket]:
    usable = [
        m
        for m in markets
        if m.yes_price is not None
        and m.yes_price > 0
        and (m.rule_min is not None or m.rule_max is not None)
        and native_forecast_for_market(forecasts_by_unit, m, kind)[0] is not None
    ]
    return sorted(usable, key=lambda m: canonical_market_distance(forecasts_by_unit, m, kind))[0] if usable else None


def candidate_pair_from_forecast(value: Optional[float]) -> list[int]:
    if value is None:
        return []
    lower = math.floor(float(value))
    return [lower, lower + 1]


def adjacent_integer_targets(value: Optional[float]) -> list[int]:
    if value is None:
        return []
    lower = math.floor(float(value))
    upper = math.ceil(float(value))
    if lower == upper:
        return [lower, lower + 1]
    return [lower, upper]


def market_contains_temperature(market: TemperatureMarket, target_temp: float, target_unit: str) -> bool:
    lo, hi, _ = comparable_rule_bounds(market, target_unit)
    if lo is not None and target_temp < lo:
        return False
    if hi is not None and target_temp > hi:
        return False
    return lo is not None or hi is not None


def choose_dual_bracket_markets(
    markets: list[TemperatureMarket],
    target_temps: list[int],
    target_unit: str,
    price_threshold: float,
    max_markets: int,
) -> list[TemperatureMarket]:
    ranked: list[tuple[int, float, str, TemperatureMarket]] = []
    seen: set[str] = set()
    for target in target_temps:
        for market in markets:
            if market.market_id in seen:
                continue
            if market.yes_price is None or market.yes_price <= 0 or market.yes_price >= price_threshold:
                continue
            if (market.unit or target_unit).upper() != target_unit.upper():
                continue
            if not market_contains_temperature(market, float(target), target_unit):
                continue
            distance = market_distance(float(target), market, target_unit)
            ranked.append((target_temps.index(target), float(market.yes_price), market.market_id, market))
            seen.add(market.market_id)
    ranked.sort(key=lambda item: (item[0], item[1], item[2]))
    return [item[3] for item in ranked[:max_markets]]


def sorted_markets_for_unit(markets: list[TemperatureMarket], target_unit: str) -> list[TemperatureMarket]:
    unit = target_unit.upper()
    usable = [
        market
        for market in markets
        if (market.unit or unit).upper() == unit
        and (market.rule_min is not None or market.rule_max is not None)
    ]

    def sort_key(market: TemperatureMarket) -> tuple[float, float, str]:
        lo, hi, _ = comparable_rule_bounds(market, unit)
        lo_key = lo if lo is not None else -9999.0
        hi_key = hi if hi is not None else 9999.0
        return lo_key, hi_key, market.market_id

    return sorted(usable, key=sort_key)


def markets_adjacent_to_forecast(
    markets: list[TemperatureMarket],
    forecast_value: Optional[float],
    target_unit: str,
) -> list[TemperatureMarket]:
    if forecast_value is None:
        return []
    unit = target_unit.upper()
    forecast_int = int(round(float(forecast_value)))
    ordered = sorted_markets_for_unit(markets, unit)
    containing_idx: Optional[int] = None
    for idx, market in enumerate(ordered):
        if market_contains_temperature(market, float(forecast_int), unit):
            containing_idx = idx
            break
    if containing_idx is None:
        return []

    market = ordered[containing_idx]
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if lo is not None and hi is not None:
        center = (lo + hi) / 2.0
        neighbor_idx = containing_idx - 1 if forecast_int <= center else containing_idx + 1
    elif lo is None:
        neighbor_idx = containing_idx + 1
    else:
        neighbor_idx = containing_idx - 1

    candidates: list[TemperatureMarket] = []
    if neighbor_idx < containing_idx and 0 <= neighbor_idx < len(ordered):
        candidates.append(ordered[neighbor_idx])
    candidates.append(market)
    if neighbor_idx > containing_idx and 0 <= neighbor_idx < len(ordered):
        candidates.append(ordered[neighbor_idx])
    return candidates


def choose_dual_bracket_markets_from_forecast(
    markets: list[TemperatureMarket],
    forecast_value: Optional[float],
    target_unit: str,
    price_threshold: float,
    max_markets: int,
) -> list[TemperatureMarket]:
    chosen: list[TemperatureMarket] = []
    seen: set[str] = set()
    for market in markets_adjacent_to_forecast(markets, forecast_value, target_unit):
        if market.market_id in seen:
            continue
        seen.add(market.market_id)
        if market.yes_price is None or market.yes_price <= 0 or market.yes_price >= price_threshold:
            continue
        chosen.append(market)
        if len(chosen) >= max_markets:
            break
    return chosen


def existing_open_or_closed_trade_keys(config: dict[str, Any], strategy_name: str) -> set[tuple[str, str, str, str]]:
    keys: set[tuple[str, str, str, str]] = set()
    for trade in read_trades(config["outputs"]["trades_csv"]):
        if trade.strategy != strategy_name:
            continue
        keys.add((trade.event_date, trade.city, trade.kind, trade.market_id))
    return keys


def taker_fee_usdc(shares: float, price: float, fee_rate: float, fee_enabled: bool) -> float:
    if not fee_enabled:
        return 0.0
    return shares * fee_rate * price * (1.0 - price)


def build_trade(
    config: dict[str, Any],
    cycle_id: str,
    market: TemperatureMarket,
    wu_source: str,
    station: str,
    forecast_temp: Optional[float],
    forecast_high: Optional[float],
    forecast_low: Optional[float],
    first_valid_time_local: str,
    last_valid_time_local: str,
) -> PaperTrade:
    now = datetime.now().isoformat(timespec="seconds")
    forecast_unit = (market.unit or twc_forecast_unit(config)).upper()
    comparable_min, comparable_max, comparable_unit = comparable_rule_bounds(market, forecast_unit)
    notional = float(config["trading"]["buy_notional_usdc"])
    threshold = float(config["trading"].get("mispricing_price_threshold", 1.0))
    price = float(market.yes_price or 0.0)
    shares = notional / price if price > 0 else 0.0
    fee = taker_fee_usdc(
        shares,
        price,
        float(config["trading"]["fee_rate"]),
        bool(config["trading"]["fee_enabled"]),
    )
    trade_id = f"{cycle_id}:{market.market_id}:{len(str(time.time_ns()))}:{time.time_ns()}"
    return PaperTrade(
        trade_id=trade_id,
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
        forecast_source="twc_hourly_forecast",
        forecast_observed_at=now,
        forecast_station=station,
        forecast_temp=forecast_temp,
        forecast_high=forecast_high,
        forecast_low=forecast_low,
        forecast_first_valid_time_local=first_valid_time_local,
        forecast_last_valid_time_local=last_valid_time_local,
        forecast_unit=forecast_unit,
        rule_min=market.rule_min,
        rule_max=market.rule_max,
        market_unit=market.unit,
        comparable_rule_min=comparable_min,
        comparable_rule_max=comparable_max,
        comparable_unit=comparable_unit,
        yes_price=market.yes_price,
        mispricing_price_threshold=threshold,
        pricing_edge=round(1.0 - price, 8),
        notional_usdc=round(notional, 6),
        shares=round(shares, 8),
        taker_fee_rate=float(config["trading"]["fee_rate"]),
        buy_fee_usdc=round(fee, 8),
        total_cost_usdc=round(notional + fee, 8),
    )


def append_csv(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with IO_LOCK:
        exists = os.path.exists(path) and os.path.getsize(path) > 0
        if exists:
            with open(path, newline="", encoding="utf-8") as existing_file:
                reader = csv.reader(existing_file)
                fieldnames = next(reader, [])
            new_fields = sorted({k for row in rows for k in row.keys() if k not in fieldnames})
            if new_fields:
                existing_rows = read_csv_dicts(path)
                fieldnames = fieldnames + new_fields
                with open(path, "w", newline="", encoding="utf-8") as rewrite_file:
                    writer = csv.DictWriter(rewrite_file, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(existing_rows)
        else:
            fieldnames = sorted({k for row in rows for k in row.keys()})
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerows(rows)


def append_jsonl_text(path: str, text: str) -> None:
    with IO_LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text.rstrip("\r\n"))
            f.write("\n")


def read_csv_dicts(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with IO_LOCK:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))


def read_trades(path: str) -> list[PaperTrade]:
    trades: list[PaperTrade] = []
    for row in read_csv_dicts(path):
        cleaned: dict[str, Any] = {}
        for field in PaperTrade.__dataclass_fields__:
            value = row.get(field, "")
            if field in {
                "forecast_temp",
                "forecast_high",
                "forecast_low",
                "rule_min",
                "rule_max",
                "comparable_rule_min",
                "comparable_rule_max",
                "yes_price",
                "mispricing_price_threshold",
                "pricing_edge",
                "notional_usdc",
                "shares",
                "taker_fee_rate",
                "buy_fee_usdc",
                "total_cost_usdc",
                "exit_yes_price",
                "exit_fee_usdc",
                "exit_proceeds_usdc",
                "monitor_last_yes_price",
                "monitor_price_trigger",
                "monitor_peak_pnl_usdc",
                "payout_usdc",
                "pnl_usdc",
            }:
                cleaned[field] = float(value) if value not in {"", "None", None} else None
            else:
                cleaned[field] = value
        cleaned.setdefault("forecast_first_valid_time_local", "")
        cleaned.setdefault("forecast_last_valid_time_local", "")
        cleaned.setdefault("comparable_rule_min", None)
        cleaned.setdefault("comparable_rule_max", None)
        cleaned.setdefault("comparable_unit", cleaned.get("forecast_unit", ""))
        cleaned.setdefault("exit_at", "")
        cleaned.setdefault("exit_reason", "")
        cleaned.setdefault("monitor_last_checked_at", "")
        for field in {
            "notional_usdc",
            "shares",
            "taker_fee_rate",
            "buy_fee_usdc",
            "total_cost_usdc",
            "exit_fee_usdc",
            "exit_proceeds_usdc",
            "monitor_price_trigger",
            "monitor_peak_pnl_usdc",
            "payout_usdc",
            "pnl_usdc",
        }:
            cleaned[field] = float(cleaned[field] or 0.0)
        trades.append(PaperTrade(**cleaned))
    return trades


def write_csv(path: str, rows: Iterable[Any]) -> None:
    materialized = [asdict(r) if hasattr(r, "__dataclass_fields__") else dict(r) for r in rows]
    fieldnames = sorted({k for row in materialized for k in row.keys()})
    with IO_LOCK:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(materialized)


def pct(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 8) if denominator else 0.0


def performance_row(group_name: str, group_value: str, rows: list[PaperTrade]) -> dict[str, Any]:
    settled = [t for t in rows if t.status in {"SETTLED", "SOLD"}]
    open_rows = [t for t in rows if t.status not in {"SETTLED", "SOLD"}]
    total_notional = sum(t.notional_usdc for t in rows)
    total_fees = sum(t.buy_fee_usdc for t in rows)
    total_cost = sum(t.total_cost_usdc for t in rows)
    settled_cost = sum(t.total_cost_usdc for t in settled)
    open_cost = sum(t.total_cost_usdc for t in open_rows)
    total_payout = sum(t.payout_usdc for t in settled)
    wins = [t for t in settled if t.pnl_usdc > 0]
    losses = [t for t in settled if t.pnl_usdc < 0]
    realized_pnl = total_payout - settled_cost
    return {
        group_name: group_value,
        "trade_count": len(rows),
        "settled_count": len(settled),
        "win_count": len(wins),
        "loss_count": len(losses),
        "open_count": len(open_rows),
        "total_notional_usdc": round(total_notional, 8),
        "total_fees_usdc": round(total_fees, 8),
        "total_cost_usdc": round(total_cost, 8),
        "settled_cost_usdc": round(settled_cost, 8),
        "open_cost_usdc": round(open_cost, 8),
        "total_payout_usdc": round(total_payout, 8),
        "realized_pnl_usdc": round(realized_pnl, 8),
        "realized_roi_on_settled_cost": pct(realized_pnl, settled_cost),
        "win_rate_settled": pct(len(wins), len(settled)),
    }


def write_performance_reports(config: dict[str, Any], trades: list[PaperTrade]) -> None:
    by_cycle: dict[str, list[PaperTrade]] = {}
    by_event: dict[str, list[PaperTrade]] = {}
    for trade in trades:
        by_cycle.setdefault(trade.cycle_id, []).append(trade)
        event_key = f"{trade.strategy}|{trade.event_date}|{trade.city}|{trade.kind}"
        by_event.setdefault(event_key, []).append(trade)

    cycle_rows = [performance_row("cycle_id", key, rows) for key, rows in sorted(by_cycle.items())]
    event_rows = []
    for key, rows in sorted(by_event.items()):
        row = performance_row("event_key", key, rows)
        first = rows[0]
        row.update(
            {
                "event_date": first.event_date,
                "strategy": first.strategy,
                "city": first.city,
                "kind": first.kind,
                "event_title": first.event_title,
                "polymarket_url": first.polymarket_url,
            }
        )
        event_rows.append(row)

    write_csv(config["outputs"]["performance_by_cycle_csv"], cycle_rows)
    write_csv(config["outputs"]["performance_by_event_csv"], event_rows)


def fetch_market_by_id(config: dict[str, Any], market_id: str) -> Optional[dict[str, Any]]:
    if not market_id:
        return None
    try:
        return gamma_get(config, f"/markets/{market_id}")
    except requests.HTTPError:
        return None


def fetch_event_by_trade(config: dict[str, Any], trade: PaperTrade) -> Optional[dict[str, Any]]:
    slug = trade.polymarket_url.rstrip("/").split("/")[-1]
    if not slug:
        return None
    try:
        event = gamma_get(config, f"/events/slug/{slug}")
    except requests.HTTPError:
        return None
    event["_parsed_kind"] = trade.kind
    event["_parsed_city"] = trade.city
    event["_parsed_event_date"] = trade.event_date
    return event


def evaluate_trade_forecast_market(config: dict[str, Any], trade: PaperTrade) -> Optional[TemperatureMarket]:
    event = fetch_event_by_trade(config, trade)
    if not event:
        return None
    markets = markets_for_event(config, event)
    unit = (trade.market_unit or event_market_unit(markets)).upper()
    twc_units = twc_units_for_temperature_unit(unit)
    station = trade.forecast_station or station_from_wu_url(trade.wunderground_source_url)
    if not station:
        return None

    payload = twc_hourly_forecast_by_icao(config, station, units=twc_units)
    if config["trading"].get("forecast_scope") == "event_day_full":
        forecast_points = daily_twc_points(payload, trade.event_date)
    else:
        forecast_points = filtered_twc_points(payload, trade.event_date, int(config["trading"]["forecast_horizon_hours"]))

    observed_points: list[tuple[datetime, float]] = []
    if config["trading"].get("include_observed_today", True) and datetime.strptime(trade.event_date, "%Y-%m-%d").date() <= date.today():
        city_local_dt, _, _ = city_local_now(config, trade.city)
        historical_payload = twc_historical_hourly_by_icao(config, station, units=twc_units)
        observed_points = observed_twc_points(historical_payload, trade.event_date, city_local_dt)

    combined_points = merge_observed_and_forecast_points(observed_points, forecast_points)
    high, low, _, _, _, _ = summarize_points(combined_points)
    forecasts_by_unit = {unit: {"high": high, "low": low}}
    return choose_most_likely_market(markets, forecasts_by_unit, trade.kind)


def resolved_outcome_from_market(market: dict[str, Any]) -> str:
    if not market or not parse_bool(market.get("closed")):
        return ""
    yes = outcome_price(market, "Yes")
    no = outcome_price(market, "No")
    if yes is not None and yes >= 0.999:
        return "Yes"
    if no is not None and no >= 0.999:
        return "No"
    return ""


def settle_open_trades(config: dict[str, Any]) -> list[PaperTrade]:
    trades_path = config["outputs"]["trades_csv"]
    trades = read_trades(trades_path)
    if not trades:
        LOGGER.info("settle skipped: no trades found at %s", trades_path)
        return []

    market_cache: dict[str, Optional[dict[str, Any]]] = {}
    settled_now = 0
    for trade in trades:
        if trade.status in {"SETTLED", "SOLD"}:
            continue
        market_cache.setdefault(trade.market_id, fetch_market_by_id(config, trade.market_id))
        outcome = resolved_outcome_from_market(market_cache[trade.market_id] or {})
        if not outcome:
            trade.status = "OPEN"
            continue
        trade.status = "SETTLED"
        trade.settlement_source = "polymarket_closed_market"
        trade.winning_outcome = outcome
        trade.payout_usdc = round(trade.shares if outcome == "Yes" else 0.0, 8)
        trade.pnl_usdc = round(trade.payout_usdc - trade.total_cost_usdc, 8)
        settled_now += 1

    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)
    LOGGER.info("settle complete: trades=%s newly_settled=%s", len(trades), settled_now)
    return trades


def process_strategy_exits(config: dict[str, Any]) -> list[PaperTrade]:
    exit_config = next(
        (
            strategy_config
            for strategy_config in active_strategy_configs(config)
            if strategy_config["trading"].get("strategy_name") == "tomorrow_mispricing"
        ),
        config,
    )
    trades_path = config["outputs"]["trades_csv"]
    trades = read_trades(trades_path)
    if not trades:
        LOGGER.info("exit check skipped: no trades found at %s", trades_path)
        return []

    exits_now = 0
    market_cache: dict[str, Optional[dict[str, Any]]] = {}
    for trade in trades:
        if trade.status != "OPEN" or trade.strategy != "tomorrow_mispricing":
            continue
        try:
            event_dt = datetime.strptime(trade.event_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        if event_dt > date.today():
            continue

        market_cache.setdefault(trade.market_id, fetch_market_by_id(config, trade.market_id))
        market = market_cache[trade.market_id] or {}
        current_price = outcome_price(market, "Yes")
        threshold = float(trade.mispricing_price_threshold or config["trading"].get("mispricing_price_threshold", 0.5))

        exit_reason = ""
        current_choice_id = ""
        if current_price is not None and current_price >= threshold:
            exit_reason = "price_reached_threshold"
        else:
            current_choice = evaluate_trade_forecast_market(exit_config, trade)
            current_choice_id = current_choice.market_id if current_choice else ""
            if current_choice and current_choice.market_id != trade.market_id:
                exit_reason = f"forecast_invalidated_now_{current_choice.market_id}"

        if not exit_reason or current_price is None:
            continue

        exit_fee = taker_fee_usdc(
            trade.shares,
            float(current_price),
            float(trade.taker_fee_rate),
            bool(exit_config["trading"]["fee_enabled"]),
        )
        proceeds = trade.shares * float(current_price) - exit_fee
        trade.status = "SOLD"
        trade.exit_at = datetime.now().isoformat(timespec="seconds")
        trade.exit_reason = exit_reason
        trade.exit_yes_price = float(current_price)
        trade.exit_fee_usdc = round(exit_fee, 8)
        trade.exit_proceeds_usdc = round(proceeds, 8)
        trade.payout_usdc = round(proceeds, 8)
        trade.pnl_usdc = round(proceeds - trade.total_cost_usdc, 8)
        exits_now += 1
        LOGGER.info(
            "exit strategy=tomorrow_mispricing trade=%s city=%s kind=%s bought_market=%s current_choice=%s exit_price=%s threshold=%s reason=%s shares=%s exit_fee=%s proceeds=%s total_cost=%s pnl=%s",
            trade.trade_id,
            trade.city,
            trade.kind,
            trade.market_id,
            current_choice_id,
            current_price,
            threshold,
            exit_reason,
            trade.shares,
            trade.exit_fee_usdc,
            trade.exit_proceeds_usdc,
            trade.total_cost_usdc,
            trade.pnl_usdc,
        )

    write_csv(trades_path, trades)
    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)
    LOGGER.info("exit check complete: trades=%s exits=%s", len(trades), exits_now)
    return trades


def process_strategy4_reprice(config: dict[str, Any]) -> list[PaperTrade]:
    return process_strategy4_reprice_on_trigger(config, force=False)


def strategy4_exit_metrics(
    config: dict[str, Any],
    trade: PaperTrade,
    current_choice: TemperatureMarket,
) -> dict[str, Any]:
    raw_current_market = parse_jsonish(current_choice.raw_market_json, {})
    mark_yes = float(current_choice.yes_price or 0.0)
    sell_yes = mark_yes
    yes_best_bid = None
    yes_best_ask = None
    if isinstance(raw_current_market, dict):
        try:
            yes_best_bid = float(raw_current_market["bestBid"]) if raw_current_market.get("bestBid") not in {"", None} else None
        except (TypeError, ValueError):
            yes_best_bid = None
        try:
            yes_best_ask = float(raw_current_market["bestAsk"]) if raw_current_market.get("bestAsk") not in {"", None} else None
        except (TypeError, ValueError):
            yes_best_ask = None
    if yes_best_bid is not None and yes_best_bid > 0:
        sell_yes = yes_best_bid
    current_no = outcome_price(raw_current_market, "No") if isinstance(raw_current_market, dict) else None
    fee_enabled = bool(config["trading"]["fee_enabled"])
    fee_rate = float(trade.taker_fee_rate)
    sell_fee = taker_fee_usdc(trade.shares, sell_yes, fee_rate, fee_enabled)
    sell_proceeds = trade.shares * sell_yes - sell_fee
    sell_pnl = sell_proceeds - trade.total_cost_usdc
    hedge_pnl = -999999.0
    hedge_proceeds = 0.0
    hedge_fee = 0.0
    if current_no is not None and current_no > 0:
        hedge_fee = taker_fee_usdc(trade.shares, float(current_no), fee_rate, fee_enabled)
        no_cost = trade.shares * float(current_no) + hedge_fee
        hedge_proceeds = trade.shares - no_cost
        hedge_pnl = hedge_proceeds - trade.total_cost_usdc
    use_hedge = current_no is not None and hedge_pnl > sell_pnl
    return {
        "current_yes": sell_yes,
        "mark_yes": mark_yes,
        "yes_best_bid": yes_best_bid,
        "yes_best_ask": yes_best_ask,
        "current_no": current_no,
        "sell_fee": sell_fee,
        "sell_proceeds": sell_proceeds,
        "sell_pnl": sell_pnl,
        "hedge_fee": hedge_fee,
        "hedge_proceeds": hedge_proceeds,
        "hedge_pnl": hedge_pnl,
        "use_hedge": use_hedge,
        "best_pnl": hedge_pnl if use_hedge else sell_pnl,
    }


def apply_strategy4_exit(
    trade: PaperTrade,
    *,
    now_text: str,
    exit_reason: str,
    metrics: dict[str, Any],
    use_hedge: bool,
) -> str:
    trade.status = "SOLD"
    trade.exit_at = now_text
    trade.exit_reason = exit_reason
    trade.exit_yes_price = float(metrics["current_yes"])
    trade.exit_fee_usdc = round(float(metrics["hedge_fee"] if use_hedge else metrics["sell_fee"]), 8)
    trade.exit_proceeds_usdc = round(float(metrics["hedge_proceeds"] if use_hedge else metrics["sell_proceeds"]), 8)
    trade.payout_usdc = trade.exit_proceeds_usdc
    trade.pnl_usdc = round(float(metrics["hedge_pnl"] if use_hedge else metrics["sell_pnl"]), 8)
    return "buy_no_hedge" if use_hedge else "sell_yes"


def process_strategy4_reprice_on_trigger(
    config: dict[str, Any],
    *,
    force: bool = False,
    trigger_context: Optional[dict[str, Any]] = None,
) -> list[PaperTrade]:
    strategy_configs = [
        strategy_config
        for strategy_config in active_strategy_configs(config)
        if strategy_config["trading"].get("strategy_mode") == "twc_reprice_momentum"
    ]
    if not strategy_configs:
        return []
    strategy_config = strategy_configs[0]
    strategy_name = str(strategy_config["trading"]["strategy_name"])
    trades_path = config["outputs"]["trades_csv"]
    trades = read_trades(trades_path)
    if not trades:
        LOGGER.info("strategy4 reprice skipped: no trades found at %s", trades_path)
        return []

    now_text = datetime.now().isoformat(timespec="seconds")
    cycle_id = datetime.now().strftime("%Y%m%dT%H%M%S") + f":{strategy_name}:monitor"
    max_buy_yes_price = float(strategy_config["trading"].get("max_buy_yes_price", 0.5))
    trigger_default = float(strategy_config["trading"].get("monitor_price_change_trigger", 0.05))
    stop_loss_fraction = float(strategy_config["trading"].get("stop_loss_fraction_of_cost", 0.25))
    profit_drawdown_fraction = float(strategy_config["trading"].get("profit_drawdown_fraction", 0.3))
    profit_drawdown_min_profit = float(strategy_config["trading"].get("profit_drawdown_min_profit_usdc", 1.0))
    trigger_context = trigger_context or {}
    changed_count = 0
    new_trades: list[PaperTrade] = []
    twc_wide_rows: list[dict[str, Any]] = []
    target_city = str(trigger_context.get("city") or "")
    target_kind = str(trigger_context.get("kind") or "")
    target_event_date = str(trigger_context.get("event_date") or "")
    allowed_cities = {
        str(city)
        for city in (
            strategy_config["trading"].get("allowed_cities")
            or config["events"].get("allowed_cities")
            or []
        )
        if str(city)
    }

    for trade in trades:
        if trade.status != "OPEN" or trade.strategy != strategy_name:
            continue
        if allowed_cities and trade.city not in allowed_cities:
            continue
        if force and target_city and trade.city != target_city:
            continue
        if force and target_kind and trade.kind != target_kind:
            continue
        if force and target_event_date and trade.event_date != target_event_date:
            continue
        try:
            event = fetch_event_by_trade(strategy_config, trade)
            if not event:
                LOGGER.warning("strategy4 monitor skip trade=%s reason=event_not_found", trade.trade_id)
                continue
            markets = markets_for_event(strategy_config, event)
            current_choice = next((market for market in markets if market.market_id == trade.market_id), None)
            if not current_choice or current_choice.yes_price is None:
                LOGGER.warning("strategy4 monitor skip trade=%s reason=current_market_price_missing market=%s", trade.trade_id, trade.market_id)
                continue

            current_yes = float(current_choice.yes_price)
            metrics = strategy4_exit_metrics(strategy_config, trade, current_choice)
            baseline = float(trade.monitor_last_yes_price if trade.monitor_last_yes_price is not None else trade.yes_price or current_yes)
            trigger = float(trade.monitor_price_trigger or trigger_default)
            delta = round(current_yes - baseline, 8)
            trade.monitor_last_checked_at = now_text
            if not force and abs(delta) < trigger:
                LOGGER.info(
                    "strategy4 monitor hold trade=%s city=%s kind=%s market=%s current_yes=%s baseline=%s delta=%s trigger=%s",
                    trade.trade_id,
                    trade.city,
                    trade.kind,
                    trade.market_id,
                    current_yes,
                    baseline,
                    delta,
                    trigger,
                )
                continue
            if force:
                LOGGER.info(
                    "strategy4 monitor forced_reprice trade=%s city=%s kind=%s market=%s current_yes=%s baseline=%s delta=%s trigger_context=%s",
                    trade.trade_id,
                    trade.city,
                    trade.kind,
                    trade.market_id,
                    current_yes,
                    baseline,
                    delta,
                    json.dumps(trigger_context, ensure_ascii=False, sort_keys=True),
                )

            station = trade.forecast_station or station_from_wu_url(trade.wunderground_source_url)
            if not station:
                LOGGER.warning("strategy4 monitor skip trade=%s reason=missing_station", trade.trade_id)
                trade.monitor_last_yes_price = current_yes
                continue

            city_local_dt, city_timezone, _ = city_local_now(strategy_config, trade.city)
            latest_choice, forecast_meta, wide_rows = best_twc_matching_market_for_event(
                strategy_config,
                cycle_id,
                event,
                markets,
                station,
                event_market_unit(markets),
                trade.polymarket_url,
                trade.wunderground_source_url,
                city_local_dt,
                city_timezone,
            )
            twc_wide_rows.extend(wide_rows)
            latest_id = latest_choice.market_id if latest_choice else ""
            if latest_choice and latest_choice.market_id == trade.market_id:
                previous_peak = max(
                    float(trade.monitor_peak_pnl_usdc or 0.0),
                    float(STRATEGY4_PEAK_PNL_BY_TRADE.get(trade.trade_id, 0.0)),
                )
                best_pnl = float(metrics["best_pnl"])
                peak_pnl = previous_peak
                trade.monitor_peak_pnl_usdc = round(peak_pnl, 8)
                sell_pnl = float(metrics["sell_pnl"])
                stop_loss_amount = trade.total_cost_usdc * stop_loss_fraction
                drawdown = peak_pnl - best_pnl
                drawdown_threshold = peak_pnl * profit_drawdown_fraction
                if stop_loss_fraction > 0 and sell_pnl <= -stop_loss_amount:
                    action = apply_strategy4_exit(
                        trade,
                        now_text=now_text,
                        exit_reason=f"strategy4_stop_loss_forecast_same_loss_fraction_{stop_loss_fraction:g}",
                        metrics=metrics,
                        use_hedge=False,
                    )
                    changed_count += 1
                    LOGGER.info(
                        "strategy4 risk_exit trade=%s city=%s kind=%s market=%s reason=stop_loss action=%s current_yes=%s current_no=%s sell_pnl=%s stop_loss_amount=%s peak_pnl=%s exit_fee=%s proceeds=%s pnl=%s",
                        trade.trade_id,
                        trade.city,
                        trade.kind,
                        trade.market_id,
                        action,
                        current_yes,
                        metrics["current_no"] if metrics["current_no"] is not None else "",
                        round(sell_pnl, 8),
                        round(stop_loss_amount, 8),
                        round(peak_pnl, 8),
                        trade.exit_fee_usdc,
                        trade.exit_proceeds_usdc,
                        trade.pnl_usdc,
                    )
                    continue
                if (
                    profit_drawdown_fraction > 0
                    and peak_pnl >= profit_drawdown_min_profit
                    and drawdown >= drawdown_threshold
                ):
                    use_hedge = bool(metrics["use_hedge"])
                    action = apply_strategy4_exit(
                        trade,
                        now_text=now_text,
                        exit_reason=f"strategy4_profit_drawdown_forecast_same_fraction_{profit_drawdown_fraction:g}",
                        metrics=metrics,
                        use_hedge=use_hedge,
                    )
                    changed_count += 1
                    LOGGER.info(
                        "strategy4 risk_exit trade=%s city=%s kind=%s market=%s reason=profit_drawdown action=%s current_yes=%s current_no=%s sell_pnl=%s hedge_pnl=%s best_pnl=%s peak_pnl=%s drawdown=%s threshold=%s exit_fee=%s proceeds=%s pnl=%s",
                        trade.trade_id,
                        trade.city,
                        trade.kind,
                        trade.market_id,
                        action,
                        current_yes,
                        metrics["current_no"] if metrics["current_no"] is not None else "",
                        round(float(metrics["sell_pnl"]), 8),
                        round(float(metrics["hedge_pnl"]), 8) if metrics["current_no"] is not None else "",
                        round(best_pnl, 8),
                        round(peak_pnl, 8),
                        round(drawdown, 8),
                        round(drawdown_threshold, 8),
                        trade.exit_fee_usdc,
                        trade.exit_proceeds_usdc,
                        trade.pnl_usdc,
                    )
                    continue
                trade.monitor_last_yes_price = current_yes
                LOGGER.info(
                    "strategy4 monitor repriced_but_forecast_same trade=%s city=%s kind=%s market=%s current_yes=%s current_no=%s delta=%s sell_pnl=%s hedge_pnl=%s best_pnl=%s peak_pnl=%s latest_forecast=%s%s",
                    trade.trade_id,
                    trade.city,
                    trade.kind,
                    trade.market_id,
                    current_yes,
                    metrics["current_no"] if metrics["current_no"] is not None else "",
                    delta,
                    round(float(metrics["sell_pnl"]), 8),
                    round(float(metrics["hedge_pnl"]), 8) if metrics["current_no"] is not None else "",
                    round(best_pnl, 8),
                    round(peak_pnl, 8),
                    forecast_meta.get("forecast_high") if trade.kind == "Highest" else forecast_meta.get("forecast_low"),
                    forecast_meta.get("forecast_unit", ""),
                )
                continue

            use_hedge = bool(metrics["use_hedge"])
            exit_reason = (
                f"strategy4_forecast_changed_buy_no_hedge_new_market_{latest_id}"
                if use_hedge
                else f"strategy4_forecast_changed_sell_yes_new_market_{latest_id}"
            )
            action = apply_strategy4_exit(trade, now_text=now_text, exit_reason=exit_reason, metrics=metrics, use_hedge=use_hedge)
            changed_count += 1
            LOGGER.info(
                "strategy4 exit trade=%s city=%s kind=%s old_market=%s latest_market=%s current_yes=%s current_no=%s delta=%s action=%s sell_pnl=%s hedge_pnl=%s exit_fee=%s proceeds=%s pnl=%s",
                trade.trade_id,
                trade.city,
                trade.kind,
                trade.market_id,
                latest_id,
                current_yes,
                metrics["current_no"] if metrics["current_no"] is not None else "",
                delta,
                action,
                round(float(metrics["sell_pnl"]), 8),
                round(float(metrics["hedge_pnl"]), 8) if metrics["current_no"] is not None else "",
                trade.exit_fee_usdc,
                trade.exit_proceeds_usdc,
                trade.pnl_usdc,
            )

            if latest_choice and latest_choice.yes_price is not None and latest_choice.yes_price > 0 and latest_choice.yes_price <= max_buy_yes_price:
                replacement = make_strategy4_trade(
                    strategy_config,
                    cycle_id,
                    latest_choice,
                    trade.wunderground_source_url,
                    station,
                    forecast_meta,
                )
                new_trades.append(replacement)
                LOGGER.info(
                    "strategy4 replacement_buy trade=%s city=%s kind=%s market=%s price=%s max_buy=%s forecast=%s%s shares=%s total_cost=%s",
                    replacement.trade_id,
                    replacement.city,
                    replacement.kind,
                    replacement.market_id,
                    replacement.yes_price,
                    max_buy_yes_price,
                    replacement.forecast_temp,
                    replacement.forecast_unit,
                    replacement.shares,
                    replacement.total_cost_usdc,
                )
            else:
                LOGGER.info(
                    "strategy4 replacement_skip city=%s kind=%s latest_market=%s latest_price=%s max_buy=%s reason=%s",
                    trade.city,
                    trade.kind,
                    latest_id,
                    latest_choice.yes_price if latest_choice else "",
                    max_buy_yes_price,
                    "price_above_max_buy_or_no_usable_market" if latest_choice else "no_usable_market",
                )
        except Exception:
            LOGGER.exception("strategy4 monitor failed trade=%s city=%s kind=%s market=%s", trade.trade_id, trade.city, trade.kind, trade.market_id)

    if new_trades:
        trades.extend(new_trades)
    write_csv(trades_path, trades)
    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)
    append_twc_raw_wide(config, twc_wide_rows)
    LOGGER.info("strategy4 monitor complete trades=%s exits=%s replacement_buys=%s twc_wide=%s", len(trades), changed_count, len(new_trades), len(twc_wide_rows))
    return trades


def market_clob_tokens(market_json: dict[str, Any]) -> list[tuple[str, str]]:
    outcomes = parse_jsonish(market_json.get("outcomes"), [])
    token_ids = (
        parse_jsonish(market_json.get("clobTokenIds"), [])
        or parse_jsonish(market_json.get("clobTokenIDs"), [])
        or parse_jsonish(market_json.get("tokenIds"), [])
    )
    tokens: list[tuple[str, str]] = []
    if isinstance(token_ids, list):
        for idx, token_id in enumerate(token_ids):
            outcome = str(outcomes[idx]) if isinstance(outcomes, list) and idx < len(outcomes) else f"outcome_{idx}"
            if token_id:
                tokens.append((str(token_id), outcome))
    if not tokens:
        for idx, token in enumerate(market_json.get("tokens") or []):
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("tokenId") or token.get("asset_id") or token.get("assetId")
            outcome = token.get("outcome") or (str(outcomes[idx]) if isinstance(outcomes, list) and idx < len(outcomes) else f"outcome_{idx}")
            if token_id:
                tokens.append((str(token_id), str(outcome)))
    return tokens


def strategy4_open_trades(config: dict[str, Any]) -> tuple[dict[str, Any], list[PaperTrade]]:
    strategy_config = next(
        (
            strategy_config
            for strategy_config in active_strategy_configs(config)
            if strategy_config["trading"].get("strategy_mode") == "twc_reprice_momentum"
        ),
        {},
    )
    if not strategy_config:
        return {}, []
    strategy_name = str(strategy_config["trading"]["strategy_name"])
    allowed_cities = {
        str(city)
        for city in (
            strategy_config["trading"].get("allowed_cities")
            or config["events"].get("allowed_cities")
            or []
        )
        if str(city)
    }
    trades = [
        trade
        for trade in read_trades(config["outputs"]["trades_csv"])
        if trade.status == "OPEN" and trade.strategy == strategy_name
        and (not allowed_cities or trade.city in allowed_cities)
    ]
    return strategy_config, trades


def strategy4_websocket_assets(config: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    strategy_config, trades = strategy4_open_trades(config)
    if not strategy_config or not trades:
        return strategy_config, {}
    assets: dict[str, dict[str, Any]] = {}
    trades_by_event: dict[str, PaperTrade] = {}
    for trade in trades:
        event_key = f"{trade.event_date}|{trade.city}|{trade.kind}|{trade.polymarket_url}"
        if event_key in trades_by_event:
            continue
        trades_by_event[event_key] = trade
        event = fetch_event_by_trade(strategy_config, trade)
        if not event:
            continue
        try:
            markets = markets_for_event(strategy_config, event)
        except Exception:
            LOGGER.exception("strategy4 websocket asset discovery failed city=%s kind=%s url=%s", trade.city, trade.kind, trade.polymarket_url)
            continue
        for market in markets:
            raw_market = parse_jsonish(market.raw_market_json, {})
            if not isinstance(raw_market, dict):
                continue
            outcome_prices = parse_jsonish(raw_market.get("outcomePrices"), [])
            for idx, (asset_id, outcome) in enumerate(market_clob_tokens(raw_market)):
                baseline = None
                if isinstance(outcome_prices, list) and idx < len(outcome_prices):
                    try:
                        baseline = float(outcome_prices[idx])
                    except (TypeError, ValueError):
                        baseline = None
                is_holding_yes = market.market_id == trade.market_id and str(outcome).lower() == "yes"
                assets[asset_id] = {
                    "asset_id": asset_id,
                    "baseline": baseline,
                    "last_price": baseline,
                    "outcome": outcome,
                    "event_date": trade.event_date,
                    "city": trade.city,
                    "kind": trade.kind,
                    "event_title": trade.event_title,
                    "market_id": market.market_id,
                    "market_question": market.market_question,
                    "polymarket_url": trade.polymarket_url,
                    "holding_trade_id": trade.trade_id if is_holding_yes else "",
                    "holding_shares": trade.shares if is_holding_yes else "",
                    "holding_total_cost_usdc": trade.total_cost_usdc if is_holding_yes else "",
                    "holding_peak_pnl_usdc": trade.monitor_peak_pnl_usdc if is_holding_yes else "",
                }
    return strategy_config, assets


def websocket_message_prices(message: Any) -> list[tuple[str, float, str]]:
    rows: list[tuple[str, float, str]] = []
    payloads = message if isinstance(message, list) else [message]
    price_keys = ("price", "best_bid", "best_ask", "bid", "ask", "last_trade_price")
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        nested = payload.get("changes")
        if isinstance(nested, list):
            rows.extend(websocket_message_prices(nested))
        asset_id = payload.get("asset_id") or payload.get("assetId") or payload.get("token_id") or payload.get("tokenId")
        if not asset_id:
            market_asset = payload.get("market_asset")
            if isinstance(market_asset, dict):
                asset_id = market_asset.get("asset_id") or market_asset.get("assetId")
        if not asset_id:
            continue
        for key in price_keys:
            if key not in payload:
                continue
            try:
                rows.append((str(asset_id), float(payload[key]), key))
            except (TypeError, ValueError):
                continue
    return rows


def update_strategy4_websocket_peak(
    config: dict[str, Any],
    strategy_config: dict[str, Any],
    asset: dict[str, Any],
    price: float,
    price_field: str,
) -> None:
    trade_id = str(asset.get("holding_trade_id") or "")
    if not trade_id:
        return
    allowed_fields = {
        str(v)
        for v in strategy_config["trading"].get(
            "websocket_peak_price_fields",
            ["price", "best_bid", "bid", "last_trade_price"],
        )
    }
    if price_field not in allowed_fields or price <= 0:
        return
    try:
        shares = float(asset.get("holding_shares") or 0.0)
        total_cost = float(asset.get("holding_total_cost_usdc") or 0.0)
    except (TypeError, ValueError):
        return
    if shares <= 0 or total_cost <= 0:
        return
    fee_rate = float(strategy_config["trading"].get("fee_rate", config["trading"].get("fee_rate", 0.05)))
    fee = taker_fee_usdc(shares, price, fee_rate, bool(strategy_config["trading"]["fee_enabled"]))
    pnl = shares * price - fee - total_cost
    previous_peak = max(
        float(STRATEGY4_PEAK_PNL_BY_TRADE.get(trade_id, 0.0)),
        float(asset.get("holding_peak_pnl_usdc") or 0.0),
    )
    if pnl > previous_peak:
        new_peak = round(pnl, 8)
        STRATEGY4_PEAK_PNL_BY_TRADE[trade_id] = new_peak
        asset["holding_peak_pnl_usdc"] = new_peak
        LOGGER.info(
            "strategy4 websocket peak_update trade=%s city=%s kind=%s market=%s price_field=%s price=%s pnl=%s previous_peak=%s new_peak=%s",
            trade_id,
            asset.get("city", ""),
            asset.get("kind", ""),
            asset.get("market_id", ""),
            price_field,
            price,
            round(pnl, 8),
            round(previous_peak, 8),
            new_peak,
        )


def monitor_strategy4_websocket(config: dict[str, Any], duration_seconds: int) -> bool:
    strategy_config, assets = strategy4_websocket_assets(config)
    if not strategy_config or not assets:
        time.sleep(max(1, min(duration_seconds or 10, 10)))
        return False
    if not strategy_config["trading"].get("websocket_enabled", True):
        time.sleep(duration_seconds)
        return False
    try:
        import websocket  # type: ignore
    except ImportError:
        LOGGER.warning("strategy4 websocket disabled: install websocket-client from requirements.txt")
        time.sleep(duration_seconds)
        return False

    ws_url = str(strategy_config["trading"].get("websocket_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"))
    timeout_seconds = int(strategy_config["trading"].get("websocket_timeout_seconds", 10))
    ping_seconds = int(strategy_config["trading"].get("websocket_ping_seconds", 10))
    trigger_default = float(strategy_config["trading"].get("monitor_price_change_trigger", 0.05))
    asset_ids = sorted(assets)
    persistent = bool(strategy_config["trading"].get("websocket_persistent", True))
    deadline = None if persistent else time.monotonic() + max(1, duration_seconds)
    next_ping = time.monotonic() + max(1, ping_seconds)
    websocket_raw_jsonl = str(
        config["outputs"].get(
            "polymarket_websocket_raw_jsonl",
            config["outputs"].get("polymarket_websocket_raw_csv", "polymarket_weather_websocket_raw.jsonl"),
        )
    )
    LOGGER.info(
        "strategy4 websocket connect assets=%s events=%s persistent=%s duration=%ss",
        len(asset_ids),
        len({a["polymarket_url"] for a in assets.values()}),
        persistent,
        duration_seconds,
    )

    ws = None
    try:
        ws = websocket.create_connection(ws_url, timeout=timeout_seconds)
        ws.send(json.dumps({"type": "market", "assets_ids": asset_ids, "asset_ids": asset_ids, "custom_feature_enabled": True}))
        while deadline is None or time.monotonic() < deadline:
            if time.monotonic() >= next_ping:
                ws.send("PING")
                next_ping = time.monotonic() + max(1, ping_seconds)
            if deadline is None:
                ws.settimeout(max(1, timeout_seconds))
            else:
                ws.settimeout(max(1, min(timeout_seconds, int(deadline - time.monotonic()) or 1)))
            try:
                raw_message = ws.recv()
            except (socket.timeout, TimeoutError):
                continue
            if raw_message in {"", "PONG", "PING"}:
                continue
            raw_text = raw_message.decode("utf-8", errors="replace") if isinstance(raw_message, bytes) else str(raw_message)
            append_jsonl_text(websocket_raw_jsonl, raw_text)
            try:
                message = json.loads(raw_text)
            except (TypeError, ValueError):
                continue
            for asset_id, price, price_field in websocket_message_prices(message):
                if asset_id not in assets:
                    continue
                asset = assets[asset_id]
                update_strategy4_websocket_peak(config, strategy_config, asset, price, price_field)
                baseline = asset.get("last_price")
                delta = None if baseline is None else round(price - float(baseline), 8)
                if baseline is None:
                    asset["last_price"] = price
                    continue
                asset["last_price"] = price
                trigger = trigger_default
                if abs(delta) < trigger:
                    continue
                context = {
                    "source": "polymarket_websocket_any_event_option",
                    "asset_id": asset_id,
                    "outcome": asset.get("outcome", ""),
                    "price_field": price_field,
                    "price": price,
                    "baseline": baseline,
                    "delta": delta,
                    "trigger": trigger,
                    "city": asset.get("city", ""),
                    "kind": asset.get("kind", ""),
                    "event_date": asset.get("event_date", ""),
                    "market_id": asset.get("market_id", ""),
                    "market_question": asset.get("market_question", ""),
                    "polymarket_url": asset.get("polymarket_url", ""),
                }
                LOGGER.info("strategy4 websocket trigger %s", json.dumps(context, ensure_ascii=False, sort_keys=True))
                process_strategy4_reprice_on_trigger(config, force=True, trigger_context=context)
                strategy_config, refreshed_assets = strategy4_websocket_assets(config)
                refreshed_ids = sorted(refreshed_assets)
                if refreshed_ids and refreshed_ids != asset_ids:
                    assets = refreshed_assets
                    asset_ids = refreshed_ids
                    ws.send(json.dumps({"type": "market", "assets_ids": asset_ids, "asset_ids": asset_ids, "custom_feature_enabled": True}))
                    LOGGER.info(
                        "strategy4 websocket resubscribe assets=%s events=%s",
                        len(asset_ids),
                        len({a["polymarket_url"] for a in assets.values()}),
                    )
    except Exception:
        LOGGER.exception("strategy4 websocket monitor failed; falling back to normal sleep")
        remaining = 0 if deadline is None else max(0, int(deadline - time.monotonic()))
        if remaining and not persistent:
            time.sleep(remaining)
        return False
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
    return False


def strategy4_websocket_supervisor(config: dict[str, Any]) -> None:
    strategy_config = next(
        (
            strategy_config
            for strategy_config in active_strategy_configs(config)
            if strategy_config["trading"].get("strategy_mode") == "twc_reprice_momentum"
        ),
        {},
    )
    if not strategy_config or not strategy_config["trading"].get("websocket_enabled", True):
        return
    reconnect_seconds = int(strategy_config["trading"].get("websocket_reconnect_seconds", 5))
    LOGGER.info("strategy4 websocket supervisor started")
    while True:
        monitor_strategy4_websocket(config, 0)
        time.sleep(max(1, reconnect_seconds))


def start_strategy4_websocket_thread(config: dict[str, Any]) -> threading.Thread:
    thread = threading.Thread(
        target=strategy4_websocket_supervisor,
        args=(config,),
        name="strategy4-websocket",
        daemon=True,
    )
    thread.start()
    return thread


def all_events_settled(events: list[dict[str, Any]]) -> bool:
    if not events:
        return False
    return all(parse_bool(e.get("closed")) for e in events)


def price_snapshot_due(config: dict[str, Any], now: datetime, last_run_at: Optional[datetime]) -> tuple[bool, str]:
    snapshot_config = config.get("polymarket_price_snapshots", {})
    if not snapshot_config.get("enabled", False):
        return False, "disabled"
    interval = int(snapshot_config.get("run_every_minutes", 1)) * 60
    if last_run_at is None:
        return True, "first_run"
    elapsed = int((now - last_run_at).total_seconds())
    if elapsed >= interval:
        return True, f"elapsed_{elapsed}s"
    return False, f"only_elapsed_{elapsed}s"


def twc_raw_collection_due(config: dict[str, Any], now: datetime, last_run_at: Optional[datetime]) -> tuple[bool, str]:
    collector_config = config.get("twc_raw_collection", {})
    if not collector_config.get("enabled", False):
        return False, "disabled"
    interval = int(collector_config.get("run_every_minutes", 15)) * 60
    if last_run_at is None:
        return True, "first_run"
    elapsed = int((now - last_run_at).total_seconds())
    if elapsed >= interval:
        return True, f"elapsed_{elapsed}s"
    return False, f"only_elapsed_{elapsed}s"


def price_snapshot_window_status(config: dict[str, Any], kind: str, local_dt: Optional[datetime]) -> tuple[bool, str, str]:
    snapshot_config = config.get("polymarket_price_snapshots", {})
    if not snapshot_config.get("time_windows_enabled", True):
        return True, "", "time_windows_disabled"
    if local_dt is None:
        return False, "", "missing_city_timezone"
    if kind == "Lowest":
        window_text = str(snapshot_config.get("lowest_local_hour_window", "0-6"))
    else:
        window_text = str(snapshot_config.get("highest_local_hour_window", "12-18"))
    window = parse_hour_window(window_text)
    allowed = hour_in_window(local_dt.hour, window)
    return allowed, window_text, "inside_local_window" if allowed else f"outside_local_window_{window_text}"


def twc_raw_collection_window_status(config: dict[str, Any], kind: str, local_dt: Optional[datetime]) -> tuple[bool, str, str]:
    collector_config = config.get("twc_raw_collection", {})
    if not collector_config.get("time_windows_enabled", True):
        return True, "", "time_windows_disabled"
    if local_dt is None:
        return False, "", "missing_city_timezone"
    if kind == "Lowest":
        window_text = str(collector_config.get("lowest_local_hour_window", "0-6"))
    else:
        window_text = str(collector_config.get("highest_local_hour_window", "12-18"))
    window = parse_hour_window(window_text)
    allowed = hour_in_window(local_dt.hour, window)
    return allowed, window_text, "inside_twc_raw_collection_window" if allowed else f"outside_twc_raw_collection_window_{window_text}"


def market_price_snapshot_rows(
    *,
    config: dict[str, Any],
    cycle_id: str,
    target: date,
    event: dict[str, Any],
    market: TemperatureMarket,
    city_local_dt: Optional[datetime],
    city_timezone: str,
    window_text: str,
    raw_event_json: str,
) -> dict[str, Any]:
    raw_market = parse_jsonish(market.raw_market_json, {})
    no_price = outcome_price(raw_market, "No") if isinstance(raw_market, dict) else None
    return {
        "cycle_id": cycle_id,
        "observed_at_utc": utc_now_text(),
        "city_local_time": local_time_text(city_local_dt),
        "city_timezone": city_timezone,
        "target_date": target.isoformat(),
        "city": market.city,
        "kind": market.kind,
        "event_id": market.event_id,
        "event_title": market.event_title,
        "event_slug": event.get("slug", ""),
        "market_id": market.market_id,
        "condition_id": market.condition_id,
        "market_question": market.market_question,
        "market_unit": market.unit,
        "rule_min": market.rule_min if market.rule_min is not None else "",
        "rule_max": market.rule_max if market.rule_max is not None else "",
        "yes_price": market.yes_price if market.yes_price is not None else "",
        "no_price": no_price if no_price is not None else "",
        "closed": market.closed,
        "trade_window": window_text,
        "polymarket_url": market.polymarket_url,
        "raw_event_json": raw_event_json,
        "raw_market_json": market.raw_market_json,
    }


def collect_polymarket_price_snapshots(config: dict[str, Any], cycle_id: str) -> int:
    snapshot_config = config.get("polymarket_price_snapshots", {})
    if not snapshot_config.get("enabled", False):
        return 0
    raw_dates = snapshot_config.get("target_dates") or config["events"]["target_dates"]
    target_dates = [resolve_date(str(v)) for v in raw_dates]
    rows: list[dict[str, Any]] = []
    for target in target_dates:
        events = discover_temperature_events(config, target)
        LOGGER.info("[%s] price_snapshot target=%s events=%s", cycle_id, target.isoformat(), len(events))
        for event in events:
            city = event.get("_parsed_city", "")
            kind = event.get("_parsed_kind", "")
            city_local_dt, city_timezone, _ = city_local_now(config, city)
            allowed, window_text, reason = price_snapshot_window_status(config, kind, city_local_dt)
            if not allowed:
                continue
            try:
                markets = markets_for_event(config, event)
                raw_event_json = json.dumps(event, ensure_ascii=False, sort_keys=True)
                for market in markets:
                    rows.append(
                        market_price_snapshot_rows(
                            config=config,
                            cycle_id=cycle_id,
                            target=target,
                            event=event,
                            market=market,
                            city_local_dt=city_local_dt,
                            city_timezone=city_timezone,
                            window_text=window_text,
                            raw_event_json=raw_event_json,
                        )
                    )
            except Exception:
                LOGGER.exception(
                    "[%s] price_snapshot failed city=%s kind=%s reason=%s",
                    cycle_id,
                    city,
                    kind,
                    reason,
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))
    append_csv(str(config["outputs"].get("polymarket_price_snapshots_csv", "polymarket_weather_price_snapshots.csv")), rows)
    LOGGER.info("[%s] price_snapshot rows=%s", cycle_id, len(rows))
    return len(rows)


def collect_twc_raw_snapshots(config: dict[str, Any], cycle_id: str) -> int:
    collector_config = config.get("twc_raw_collection", {})
    if not collector_config.get("enabled", False):
        return 0
    raw_dates = collector_config.get("target_dates") or config["events"]["target_dates"]
    target_dates = [resolve_date(str(v)) for v in raw_dates]
    strategy_name = str(collector_config.get("strategy_name", "twc_raw_collector"))
    rows: list[dict[str, Any]] = []
    for target in target_dates:
        events = discover_temperature_events(config, target)
        LOGGER.info("[%s] twc_raw_collection target=%s events=%s", cycle_id, target.isoformat(), len(events))
        for event in events:
            city = event.get("_parsed_city", "")
            kind = event.get("_parsed_kind", "")
            event_url = poly_url_from_event(event)
            city_local_dt, city_timezone, _ = city_local_now(config, city)
            allowed, window_text, reason = twc_raw_collection_window_status(config, kind, city_local_dt)
            if not allowed:
                LOGGER.info(
                    "[%s] twc_raw_collection skip city=%s kind=%s reason=%s local_time=%s timezone=%s",
                    cycle_id,
                    city,
                    kind,
                    reason,
                    city_local_dt.isoformat() if city_local_dt else "",
                    city_timezone,
                )
                continue
            station = ""
            wu_source = ""
            event_unit = ""
            twc_units = ""
            try:
                markets = markets_for_event(config, event)
                event_unit = event_market_unit(markets)
                twc_units = twc_units_for_temperature_unit(event_unit)
                wu_source = extract_wunderground_source(config, event_url)
                station = station_from_wu_url(wu_source)
                if not station:
                    raise RuntimeError("No ICAO station code found in Wunderground source URL.")

                payload = twc_hourly_forecast_by_icao(config, station, units=twc_units)
                horizon_hours = int(collector_config.get("forecast_horizon_hours", 6))
                if collector_config.get("forecast_scope") == "event_day_full":
                    forecast_points = daily_twc_points(payload, event["_parsed_event_date"])
                else:
                    forecast_points = filtered_twc_points(payload, event["_parsed_event_date"], horizon_hours)
                historical_payload: dict[str, Any] = {}
                observed_points: list[tuple[datetime, float]] = []
                if collector_config.get("include_observed_today", True) and target <= date.today():
                    historical_payload = twc_historical_hourly_by_icao(config, station, units=twc_units)
                    observed_points = observed_twc_points(historical_payload, event["_parsed_event_date"], city_local_dt)
                combined_points = merge_observed_and_forecast_points(observed_points, forecast_points)
                rows.append(
                    twc_raw_wide_row(
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target_date=target,
                        city=city,
                        kind=kind,
                        station=station,
                        event_unit=event_unit,
                        twc_units=twc_units,
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        observed_points=observed_points,
                        forecast_points=forecast_points,
                        combined_points=combined_points,
                        forecast_payload=payload,
                        historical_payload=historical_payload,
                        event_url=event_url,
                        wunderground_source_url=wu_source,
                    )
                )
                LOGGER.info(
                    "[%s] twc_raw_collection city=%s kind=%s station=%s unit=%s window=%s observed=%s forecast=%s combined=%s",
                    cycle_id,
                    city,
                    kind,
                    station,
                    twc_forecast_unit_for_units(twc_units),
                    window_text,
                    len(observed_points),
                    len(forecast_points),
                    len(combined_points),
                )
            except Exception as exc:
                LOGGER.exception("[%s] twc_raw_collection failed city=%s kind=%s url=%s", cycle_id, city, kind, event_url)
                rows.append(
                    twc_raw_wide_row(
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target_date=target,
                        city=city,
                        kind=kind,
                        station=station,
                        event_unit=event_unit,
                        twc_units=twc_units,
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        observed_points=[],
                        forecast_points=[],
                        combined_points=[],
                        forecast_payload={},
                        historical_payload={},
                        event_url=event_url,
                        wunderground_source_url=wu_source,
                        error=repr(exc),
                    )
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))
    append_twc_raw_wide(config, rows)
    LOGGER.info("[%s] twc_raw_collection rows=%s", cycle_id, len(rows))
    return len(rows)


def active_strategy_configs(config: dict[str, Any]) -> list[dict[str, Any]]:
    strategies = config.get("strategies") or []
    if not strategies:
        return [config]

    effective_configs: list[dict[str, Any]] = []
    for strategy in strategies:
        if not strategy.get("enabled", True):
            continue
        overrides = {k: v for k, v in strategy.items() if k not in {"name", "enabled", "run_every_minutes", "align_to_top_of_hour"}}
        effective = deep_merge(config, overrides)
        effective["strategies"] = []
        effective["_strategy_meta"] = {
            "name": strategy.get("name") or effective["trading"].get("strategy_name", "strategy"),
            "run_every_minutes": int(strategy.get("run_every_minutes", config["scheduler"]["poll_interval_minutes"])),
            "align_to_top_of_hour": bool(strategy.get("align_to_top_of_hour", False)),
        }
        effective["trading"]["strategy_name"] = effective["_strategy_meta"]["name"]
        effective_configs.append(effective)
    return effective_configs


def strategy_due(
    strategy_config: dict[str, Any],
    now: datetime,
    last_run_at: dict[str, datetime],
) -> tuple[bool, str]:
    meta = strategy_config.get("_strategy_meta", {})
    name = str(meta.get("name") or strategy_config["trading"]["strategy_name"])
    interval = int(meta.get("run_every_minutes", 15))

    if meta.get("align_to_top_of_hour", False) and now.minute != 0:
        return False, "waiting_for_top_of_hour"

    previous = last_run_at.get(name)
    if previous is None:
        return True, "first_run"

    elapsed_seconds = (now - previous).total_seconds()
    interval_seconds = interval * 60
    if elapsed_seconds >= interval_seconds:
        return True, f"elapsed_{int(elapsed_seconds)}s"
    return False, f"only_elapsed_{int(elapsed_seconds)}s"


def parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, minute_text = str(value).split(":", 1)
    hour, minute = int(hour_text), int(minute_text)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError(f"Invalid HH:MM value: {value}")
    return hour, minute


def minutes_since_midnight(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def hhmm_to_minutes(value: str) -> int:
    hour, minute = parse_hhmm(value)
    return hour * 60 + minute


def local_minutes_in_range(local_dt: Optional[datetime], start_text: str, end_text: str) -> bool:
    if local_dt is None:
        return False
    current = minutes_since_midnight(local_dt)
    start = hhmm_to_minutes(start_text)
    end = hhmm_to_minutes(end_text)
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def strategy4_window_status(config: dict[str, Any], kind: str, local_dt: Optional[datetime]) -> tuple[bool, str, str]:
    if local_dt is None:
        return False, "", "missing_city_timezone"
    if kind == "Lowest":
        start_text = str(config["trading"].get("lowest_local_start", "00:15"))
        end_text = str(config["trading"].get("lowest_local_end", "06:00"))
    else:
        start_text = str(config["trading"].get("highest_local_start", "12:15"))
        end_text = str(config["trading"].get("highest_local_end", "18:00"))
    allowed = local_minutes_in_range(local_dt, start_text, end_text)
    window_text = f"{start_text}-{end_text}"
    return allowed, window_text, "inside_strategy4_window" if allowed else f"outside_strategy4_window_{window_text}"


def best_twc_matching_market_for_event(
    config: dict[str, Any],
    cycle_id: str,
    event: dict[str, Any],
    markets: list[TemperatureMarket],
    station: str,
    event_unit: str,
    event_url: str,
    wu_source: str,
    city_local_dt: Optional[datetime],
    city_timezone: str,
) -> tuple[Optional[TemperatureMarket], dict[str, Any], list[dict[str, Any]]]:
    twc_units = twc_units_for_temperature_unit(event_unit)
    payload = twc_hourly_forecast_by_icao(config, station, units=twc_units)
    if config["trading"].get("forecast_scope") == "event_day_full":
        forecast_points = daily_twc_points(payload, event["_parsed_event_date"])
    else:
        forecast_points = filtered_twc_points(payload, event["_parsed_event_date"], int(config["trading"]["forecast_horizon_hours"]))
    high, low, first_local, last_local, _, _ = summarize_points(forecast_points)
    forecasts_by_unit = {event_unit: {"high": high, "low": low}}
    chosen = choose_most_likely_market(markets, forecasts_by_unit, event["_parsed_kind"])
    wide_rows = [
        twc_raw_wide_row(
            cycle_id=cycle_id,
            strategy_name=str(config["trading"]["strategy_name"]),
            target_date=datetime.strptime(event["_parsed_event_date"], "%Y-%m-%d").date(),
            city=event["_parsed_city"],
            kind=event["_parsed_kind"],
            station=station,
            event_unit=event_unit,
            twc_units=twc_units,
            city_local_dt=city_local_dt,
            city_timezone=city_timezone,
            observed_points=[],
            forecast_points=forecast_points,
            combined_points=forecast_points,
            forecast_payload=payload,
            historical_payload={},
            event_url=event_url,
            wunderground_source_url=wu_source,
        )
    ]
    return chosen, {
        "payload": payload,
        "forecast_points": forecast_points,
        "forecast_high": high,
        "forecast_low": low,
        "first_local": first_local,
        "last_local": last_local,
        "forecast_unit": event_unit,
        "twc_units": twc_units,
    }, wide_rows


def open_strategy_event_keys(config: dict[str, Any], strategy_name: str) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    for trade in read_trades(config["outputs"]["trades_csv"]):
        if trade.strategy == strategy_name and trade.status == "OPEN":
            keys.add((trade.event_date, trade.city, trade.kind))
    return keys


def snapshot_row_for_strategy4(
    *,
    config: dict[str, Any],
    cycle_id: str,
    strategy_name: str,
    target: date,
    event: dict[str, Any],
    event_url: str,
    city_local_dt: Optional[datetime],
    city_timezone: str,
    city_time_source: str,
    window_text: str,
    window_allowed: bool,
    window_reason: str,
    event_unit: str,
    station: str = "",
    wu_source: str = "",
    chosen: Optional[TemperatureMarket] = None,
    forecast_meta: Optional[dict[str, Any]] = None,
    max_buy_yes_price: Optional[float] = None,
    should_buy: bool = False,
    error: str = "",
) -> dict[str, Any]:
    meta = forecast_meta or {}
    forecast_high = meta.get("forecast_high")
    forecast_low = meta.get("forecast_low")
    forecast_temp = forecast_high if event.get("_parsed_kind") == "Highest" else forecast_low
    comparable_min, comparable_max, comparable_unit = (
        comparable_rule_bounds(chosen, event_unit) if chosen else (None, None, event_unit)
    )
    return {
        "cycle_id": cycle_id,
        "strategy": strategy_name,
        "observed_at": datetime.now().isoformat(timespec="seconds"),
        "observed_at_utc": utc_now_text(),
        "target_date": target.isoformat(),
        "city": event.get("_parsed_city", ""),
        "kind": event.get("_parsed_kind", ""),
        "station": station,
        "event_market_unit": event_unit,
        "twc_units_requested": meta.get("twc_units", ""),
        "forecast_temp": forecast_temp if forecast_temp is not None else "",
        "forecast_high": forecast_high if forecast_high is not None else "",
        "forecast_low": forecast_low if forecast_low is not None else "",
        "forecast_unit": event_unit,
        "forecast_horizon_hours": config["trading"].get("forecast_horizon_hours", ""),
        "include_observed_today": False,
        "city_local_time": local_time_text(city_local_dt),
        "city_timezone": city_timezone,
        "city_time_source": city_time_source,
        "trade_window": window_text,
        "trade_window_allowed": window_allowed,
        "trade_window_reason": window_reason,
        "first_valid_time_local": meta.get("first_local", ""),
        "last_valid_time_local": meta.get("last_local", ""),
        "chosen_market_id": chosen.market_id if chosen else "",
        "chosen_condition_id": chosen.condition_id if chosen else "",
        "chosen_question": chosen.market_question if chosen else "",
        "chosen_yes_price": chosen.yes_price if chosen else "",
        "mispricing_price_threshold": max_buy_yes_price if max_buy_yes_price is not None else "",
        "max_buy_yes_price": max_buy_yes_price if max_buy_yes_price is not None else "",
        "pricing_edge": round(1.0 - float(chosen.yes_price), 8) if chosen and chosen.yes_price is not None else "",
        "should_buy": should_buy,
        "chosen_rule_min": chosen.rule_min if chosen else "",
        "chosen_rule_max": chosen.rule_max if chosen else "",
        "chosen_market_unit": chosen.unit if chosen else "",
        "chosen_comparable_rule_min": comparable_min if chosen else "",
        "chosen_comparable_rule_max": comparable_max if chosen else "",
        "chosen_comparable_unit": comparable_unit if chosen else "",
        "trade_notional_usdc": "",
        "polymarket_url": event_url,
        "wunderground_source_url": wu_source,
        "error": error,
    }


def config_value(mapping: dict[str, Any], key: str, default: Any) -> Any:
    value = mapping.get(key, default)
    return default if value is None else value


def make_strategy4_trade(
    config: dict[str, Any],
    cycle_id: str,
    chosen: TemperatureMarket,
    wu_source: str,
    station: str,
    forecast_meta: dict[str, Any],
) -> PaperTrade:
    forecast_high = forecast_meta.get("forecast_high")
    forecast_low = forecast_meta.get("forecast_low")
    forecast_temp = forecast_high if chosen.kind == "Highest" else forecast_low
    trade = build_trade(
        config,
        cycle_id,
        chosen,
        wu_source,
        station,
        forecast_temp,
        forecast_high,
        forecast_low,
        str(forecast_meta.get("first_local") or ""),
        str(forecast_meta.get("last_local") or ""),
    )
    trade.forecast_source = "twc_reprice_momentum"
    trade.monitor_last_yes_price = chosen.yes_price
    trade.monitor_last_checked_at = datetime.now().isoformat(timespec="seconds")
    trade.monitor_price_trigger = float(config["trading"].get("monitor_price_change_trigger", 0.05))
    return trade


def run_strategy4_cycle(config: dict[str, Any], cycle_id: str) -> int:
    target_dates = [resolve_date(str(v)) for v in config["events"]["target_dates"]]
    strategy_name = str(config["trading"]["strategy_name"])
    max_buy_yes_price = float(config["trading"].get("max_buy_yes_price", config["trading"].get("mispricing_price_threshold", 0.5)))
    open_event_keys = open_strategy_event_keys(config, strategy_name)
    all_new_trades: list[PaperTrade] = []
    snapshot_rows: list[dict[str, Any]] = []
    twc_wide_rows: list[dict[str, Any]] = []

    for target in target_dates:
        events = discover_temperature_events(config, target)
        log_info(f"[{cycle_id}] strategy={strategy_name} target={target.isoformat()} events={len(events)}")
        for event in events:
            event_url = poly_url_from_event(event)
            city = event.get("_parsed_city", "")
            kind = event.get("_parsed_kind", "")
            city_local_dt, city_timezone, city_time_source = city_local_now(config, city)
            window_allowed, window_text, window_reason = strategy4_window_status(config, kind, city_local_dt)
            try:
                markets = markets_for_event(config, event)
                event_unit = event_market_unit(markets)
                if not window_allowed:
                    snapshot_rows.append(
                        snapshot_row_for_strategy4(
                            config=config,
                            cycle_id=cycle_id,
                            strategy_name=strategy_name,
                            target=target,
                            event=event,
                            event_url=event_url,
                            city_local_dt=city_local_dt,
                            city_timezone=city_timezone,
                            city_time_source=city_time_source,
                            window_text=window_text,
                            window_allowed=False,
                            window_reason=window_reason,
                            event_unit=event_unit,
                        )
                    )
                    LOGGER.info(
                        "[%s] skip strategy=%s city=%s kind=%s reason=%s local_time=%s timezone=%s",
                        cycle_id,
                        strategy_name,
                        city,
                        kind,
                        window_reason,
                        city_local_dt.isoformat() if city_local_dt else "",
                        city_timezone,
                    )
                    continue

                wu_source = extract_wunderground_source(config, event_url)
                station = station_from_wu_url(wu_source)
                if not station:
                    raise RuntimeError("No ICAO station code found in Wunderground source URL.")

                chosen, forecast_meta, wide_rows = best_twc_matching_market_for_event(
                    config,
                    cycle_id,
                    event,
                    markets,
                    station,
                    event_unit,
                    event_url,
                    wu_source,
                    city_local_dt,
                    city_timezone,
                )
                twc_wide_rows.extend(wide_rows)
                should_buy = bool(chosen and chosen.yes_price is not None and chosen.yes_price > 0 and chosen.yes_price <= max_buy_yes_price)
                event_key = (event["_parsed_event_date"], city, kind)
                already_open = event_key in open_event_keys
                if already_open:
                    should_buy = False
                snapshot_rows.append(
                    snapshot_row_for_strategy4(
                        config=config,
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target=target,
                        event=event,
                        event_url=event_url,
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        city_time_source=city_time_source,
                        window_text=window_text,
                        window_allowed=True,
                        window_reason=window_reason,
                        event_unit=event_unit,
                        station=station,
                        wu_source=wu_source,
                        chosen=chosen,
                        forecast_meta=forecast_meta,
                        max_buy_yes_price=max_buy_yes_price,
                        should_buy=should_buy,
                        error=(
                            ""
                            if should_buy
                            else (
                                "open_position_exists_twc_recorded"
                                if already_open
                                else ("price_above_max_buy_or_no_usable_market" if chosen else "no_usable_market")
                            )
                        ),
                    )
                )
                if already_open:
                    LOGGER.info(
                        "[%s] skip_buy strategy=%s city=%s kind=%s station=%s forecast=%s%s market=%s price=%s reason=open_position_exists_twc_recorded",
                        cycle_id,
                        strategy_name,
                        city,
                        kind,
                        station,
                        forecast_meta.get("forecast_high") if kind == "Highest" else forecast_meta.get("forecast_low"),
                        event_unit,
                        chosen.market_id if chosen else "",
                        chosen.yes_price if chosen else "",
                    )
                    continue
                if not should_buy or not chosen:
                    LOGGER.info(
                        "[%s] skip strategy=%s city=%s kind=%s station=%s forecast=%s%s market=%s price=%s max_buy=%s reason=%s",
                        cycle_id,
                        strategy_name,
                        city,
                        kind,
                        station,
                        forecast_meta.get("forecast_high") if kind == "Highest" else forecast_meta.get("forecast_low"),
                        event_unit,
                        chosen.market_id if chosen else "",
                        chosen.yes_price if chosen else "",
                        max_buy_yes_price,
                        "price_above_max_buy" if chosen else "no_usable_market",
                    )
                    continue

                new_trade = make_strategy4_trade(config, cycle_id, chosen, wu_source, station, forecast_meta)
                all_new_trades.append(new_trade)
                open_event_keys.add(event_key)
                LOGGER.info(
                    "[%s] buy strategy=%s trade=%s city=%s kind=%s station=%s forecast=%s%s market=%s question=%s price=%s max_buy=%s trigger=%s notional=%s shares=%s buy_fee=%s total_cost=%s",
                    cycle_id,
                    strategy_name,
                    new_trade.trade_id,
                    city,
                    kind,
                    station,
                    new_trade.forecast_temp,
                    event_unit,
                    chosen.market_id,
                    chosen.market_question,
                    chosen.yes_price,
                    max_buy_yes_price,
                    new_trade.monitor_price_trigger,
                    new_trade.notional_usdc,
                    new_trade.shares,
                    new_trade.buy_fee_usdc,
                    new_trade.total_cost_usdc,
                )
            except Exception as exc:
                LOGGER.exception("[%s] event failed strategy=%s city=%s kind=%s url=%s", cycle_id, strategy_name, city, kind, event_url)
                snapshot_rows.append(
                    snapshot_row_for_strategy4(
                        config=config,
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target=target,
                        event=event,
                        event_url=event_url,
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        city_time_source=city_time_source,
                        window_text=window_text,
                        window_allowed=window_allowed,
                        window_reason="event_error",
                        event_unit="",
                        error=repr(exc),
                    )
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))

    append_csv(config["outputs"]["snapshots_csv"], snapshot_rows)
    append_twc_raw_wide(config, twc_wide_rows)
    append_csv(config["outputs"]["trades_csv"], [asdict(t) for t in all_new_trades])
    log_info(f"[{cycle_id}] strategy={strategy_name} snapshots={len(snapshot_rows)} twc_wide={len(twc_wide_rows)} new_trades={len(all_new_trades)}")
    return len(all_new_trades)


def dual_bracket_targets(
    kind: str,
    market_unit: str,
    payload_f: dict[str, Any],
    payload_c: dict[str, Any],
    event_date: str,
) -> tuple[list[int], dict[str, Any]]:
    high_f, low_f, first_f, last_f = summarize_twc_daily_forecast(payload_f, event_date)
    high_c, low_c, first_c, last_c = summarize_twc_daily_forecast(payload_c, event_date)
    source_f = high_f if kind == "Highest" else low_f
    source_c = high_c if kind == "Highest" else low_c
    if market_unit == "C":
        converted_f = convert_temperature(source_f, "F", "C")
        primary_value = converted_f if converted_f is not None else source_c
        raw_candidates = adjacent_integer_targets(primary_value)
        candidate_source = "twc_f_converted_to_c" if converted_f is not None else "twc_c"
    else:
        converted_c = convert_temperature(source_c, "C", "F")
        primary_value = source_f if source_f is not None else converted_c
        raw_candidates = [int(round(float(primary_value)))] if primary_value is not None else []
        candidate_source = "twc_f" if source_f is not None else "twc_c_converted_to_f"

    targets: list[int] = []
    for target in raw_candidates:
        if target not in targets:
            targets.append(target)
    return targets, {
        "high_f": high_f,
        "low_f": low_f,
        "first_f": first_f,
        "last_f": last_f,
        "high_c": high_c,
        "low_c": low_c,
        "first_c": first_c,
        "last_c": last_c,
        "source_f": source_f,
        "source_c": source_c,
        "converted_f_to_c": convert_temperature(source_f, "F", "C"),
        "converted_c_to_f": convert_temperature(source_c, "C", "F"),
        "primary_value": primary_value,
        "candidate_source": candidate_source,
    }


def run_dual_bracket_strategy_cycle(config: dict[str, Any], cycle_id: str) -> int:
    target_dates = [resolve_date(str(v)) for v in config["events"]["target_dates"]]
    strategy_name = str(config["trading"]["strategy_name"])
    price_threshold = float(config["trading"].get("mispricing_price_threshold", 0.5))
    max_markets = int(config["trading"].get("dual_bracket_max_markets_per_event", 2))
    low_hour = int(config["trading"].get("dual_bracket_lowest_local_hour", 1))
    low_minute = int(config["trading"].get("dual_bracket_lowest_local_minute", 0))
    high_hour = int(config["trading"].get("dual_bracket_highest_local_hour", 13))
    high_minute = int(config["trading"].get("dual_bracket_highest_local_minute", 0))
    allow_repeat_buys = bool(config["trading"].get("dual_bracket_allow_repeat_buys", False))
    existing_keys = set() if allow_repeat_buys else existing_open_or_closed_trade_keys(config, strategy_name)
    all_new_trades: list[PaperTrade] = []
    snapshot_rows: list[dict[str, Any]] = []
    twc_wide_rows: list[dict[str, Any]] = []

    for target in target_dates:
        events = discover_temperature_events(config, target)
        log_info(f"[{cycle_id}] strategy={strategy_name} target={target.isoformat()} events={len(events)}")
        for event in events:
            markets = markets_for_event(config, event)
            event_url = poly_url_from_event(event)
            observed_at = datetime.now().isoformat(timespec="seconds")
            try:
                kind = event["_parsed_kind"]
                city = event["_parsed_city"]
                city_local_dt, city_timezone, city_time_source = city_local_now(config, city)
                target_hour = low_hour if kind == "Lowest" else high_hour
                target_minute = low_minute if kind == "Lowest" else high_minute
                window_text = f"{target_hour:02d}:{target_minute:02d}-{target_hour:02d}:59"
                inside_fixed_window = (
                    city_local_dt is not None
                    and city_local_dt.hour == target_hour
                    and city_local_dt.minute >= target_minute
                )
                if not inside_fixed_window:
                    snapshot_rows.append(
                        {
                            "cycle_id": cycle_id,
                            "strategy": strategy_name,
                            "observed_at": observed_at,
                            "target_date": target.isoformat(),
                            "city": city,
                            "kind": kind,
                            "station": "",
                            "event_market_unit": event_market_unit(markets),
                            "twc_units_requested": "",
                            "forecast_temp": "",
                            "forecast_high": "",
                            "forecast_low": "",
                            "forecast_unit": "",
                            "forecast_high_f": "",
                            "forecast_low_f": "",
                            "forecast_high_c": "",
                            "forecast_low_c": "",
                            "forecast_horizon_hours": "",
                            "include_observed_today": False,
                            "observed_point_count_f": "",
                            "forecast_point_count_f": "",
                            "combined_point_count_f": "",
                            "observed_point_count_c": "",
                            "forecast_point_count_c": "",
                            "combined_point_count_c": "",
                            "city_local_time": city_local_dt.isoformat() if city_local_dt else "",
                            "city_timezone": city_timezone,
                            "city_time_source": city_time_source,
                            "trade_window": window_text,
                            "trade_window_allowed": False,
                            "trade_window_reason": f"outside_fixed_local_window_{window_text}",
                            "first_valid_time_local": "",
                            "last_valid_time_local": "",
                            "chosen_market_id": "",
                            "chosen_condition_id": "",
                            "chosen_question": "",
                            "chosen_yes_price": "",
                            "mispricing_price_threshold": price_threshold,
                            "pricing_edge": "",
                            "should_buy": False,
                            "chosen_rule_min": "",
                            "chosen_rule_max": "",
                            "chosen_market_unit": "",
                            "chosen_comparable_rule_min": "",
                            "chosen_comparable_rule_max": "",
                            "chosen_comparable_unit": "",
                            "trade_notional_usdc": "",
                            "polymarket_url": event_url,
                            "wunderground_source_url": "",
                            "twc_valid_time_local_f_json": "",
                            "twc_temperature_f_json": "",
                            "twc_raw_payload_f_json": "",
                            "twc_observed_time_local_f_json": "",
                            "twc_observed_temperature_f_json": "",
                            "twc_raw_historical_payload_f_json": "",
                            "twc_valid_time_local_c_json": "",
                            "twc_temperature_c_json": "",
                            "twc_raw_payload_c_json": "",
                            "twc_observed_time_local_c_json": "",
                            "twc_observed_temperature_c_json": "",
                            "twc_raw_historical_payload_c_json": "",
                            "error": "",
                        }
                    )
                    continue

                wu_source = extract_wunderground_source(config, event_url)
                station = station_from_wu_url(wu_source)
                if not station:
                    raise RuntimeError("No ICAO station code found in Wunderground source URL.")

                event_unit = event_market_unit(markets)
                payload_f = twc_hourly_forecast_by_icao(config, station, units="e")
                payload_c = twc_hourly_forecast_by_icao(config, station, units="m")
                forecast_points_f = daily_twc_points(payload_f, event["_parsed_event_date"])
                forecast_points_c = daily_twc_points(payload_c, event["_parsed_event_date"])
                twc_wide_rows.append(
                    twc_raw_wide_row(
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target_date=target,
                        city=city,
                        kind=kind,
                        station=station,
                        event_unit=event_unit,
                        twc_units="e",
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        observed_points=[],
                        forecast_points=forecast_points_f,
                        combined_points=forecast_points_f,
                        forecast_payload=payload_f,
                        historical_payload={},
                        event_url=event_url,
                        wunderground_source_url=wu_source,
                    )
                )
                twc_wide_rows.append(
                    twc_raw_wide_row(
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target_date=target,
                        city=city,
                        kind=kind,
                        station=station,
                        event_unit=event_unit,
                        twc_units="m",
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        observed_points=[],
                        forecast_points=forecast_points_c,
                        combined_points=forecast_points_c,
                        forecast_payload=payload_c,
                        historical_payload={},
                        event_url=event_url,
                        wunderground_source_url=wu_source,
                    )
                )
                targets, forecast_meta = dual_bracket_targets(kind, event_unit, payload_f, payload_c, event["_parsed_event_date"])
                if event_unit == "F":
                    chosen_markets = choose_dual_bracket_markets_from_forecast(
                        markets,
                        forecast_meta.get("primary_value"),
                        event_unit,
                        price_threshold,
                        max_markets,
                    )
                else:
                    chosen_markets = choose_dual_bracket_markets(markets, targets, event_unit, price_threshold, max_markets)
                chosen_markets = [
                    market
                    for market in chosen_markets
                    if (market.event_date, market.city, market.kind, market.market_id) not in existing_keys
                ][:max_markets]

                forecast_high = forecast_meta["high_c"] if event_unit == "C" else forecast_meta["high_f"]
                forecast_low = forecast_meta["low_c"] if event_unit == "C" else forecast_meta["low_f"]
                forecast_temp = forecast_high if kind == "Highest" else forecast_low
                first_local = forecast_meta["first_c"] if event_unit == "C" else forecast_meta["first_f"]
                last_local = forecast_meta["last_c"] if event_unit == "C" else forecast_meta["last_f"]
                daily_times_f, daily_temps_f = twc_daily_series(payload_f, event["_parsed_event_date"])
                daily_times_c, daily_temps_c = twc_daily_series(payload_c, event["_parsed_event_date"])

                if not chosen_markets:
                    snapshot_rows.append(
                        {
                            "cycle_id": cycle_id,
                            "strategy": strategy_name,
                            "observed_at": datetime.now().isoformat(timespec="seconds"),
                            "target_date": target.isoformat(),
                            "city": city,
                            "kind": kind,
                            "station": station,
                            "event_market_unit": event_unit,
                            "twc_units_requested": "e,m",
                            "forecast_temp": forecast_temp,
                            "forecast_high": forecast_high,
                            "forecast_low": forecast_low,
                            "forecast_unit": event_unit,
                            "dual_bracket_candidate_source": forecast_meta["candidate_source"],
                            "dual_bracket_primary_value": forecast_meta["primary_value"],
                            "dual_bracket_targets": json.dumps(targets, ensure_ascii=False),
                            "forecast_high_f": forecast_meta["high_f"],
                            "forecast_low_f": forecast_meta["low_f"],
                            "forecast_high_c": forecast_meta["high_c"],
                            "forecast_low_c": forecast_meta["low_c"],
                            "forecast_horizon_hours": 24,
                            "include_observed_today": False,
                            "observed_point_count_f": "",
                            "forecast_point_count_f": len(daily_temps_f),
                            "combined_point_count_f": len(daily_temps_f),
                            "observed_point_count_c": "",
                            "forecast_point_count_c": len(daily_temps_c),
                            "combined_point_count_c": len(daily_temps_c),
                            "city_local_time": city_local_dt.isoformat(),
                            "city_timezone": city_timezone,
                            "city_time_source": city_time_source,
                            "trade_window": window_text,
                            "trade_window_allowed": True,
                            "trade_window_reason": "inside_fixed_local_window",
                            "first_valid_time_local": first_local,
                            "last_valid_time_local": last_local,
                            "chosen_market_id": "",
                            "chosen_condition_id": "",
                            "chosen_question": "",
                            "chosen_yes_price": "",
                            "mispricing_price_threshold": price_threshold,
                            "pricing_edge": "",
                            "should_buy": False,
                            "chosen_rule_min": "",
                            "chosen_rule_max": "",
                            "chosen_market_unit": "",
                            "chosen_comparable_rule_min": "",
                            "chosen_comparable_rule_max": "",
                            "chosen_comparable_unit": event_unit,
                            "trade_notional_usdc": "",
                            "polymarket_url": event_url,
                            "wunderground_source_url": wu_source,
                            "twc_valid_time_local_f_json": json.dumps(daily_times_f, ensure_ascii=False),
                            "twc_temperature_f_json": json.dumps(daily_temps_f, ensure_ascii=False),
                            "twc_raw_payload_f_json": json.dumps(payload_f, ensure_ascii=False, sort_keys=True),
                            "twc_observed_time_local_f_json": "",
                            "twc_observed_temperature_f_json": "",
                            "twc_raw_historical_payload_f_json": "",
                            "twc_valid_time_local_c_json": json.dumps(daily_times_c, ensure_ascii=False),
                            "twc_temperature_c_json": json.dumps(daily_temps_c, ensure_ascii=False),
                            "twc_raw_payload_c_json": json.dumps(payload_c, ensure_ascii=False, sort_keys=True),
                            "twc_observed_time_local_c_json": "",
                            "twc_observed_temperature_c_json": "",
                            "twc_raw_historical_payload_c_json": "",
                            "error": f"no_markets_under_threshold_for_targets_{targets}",
                        }
                    )
                    LOGGER.info(
                        "[%s] skip strategy=%s city=%s kind=%s station=%s targets=%s reason=no_markets_under_threshold threshold=%s",
                        cycle_id,
                        strategy_name,
                        city,
                        kind,
                        station,
                        targets,
                        price_threshold,
                    )
                    continue

                for chosen in chosen_markets:
                    comparable_min, comparable_max, comparable_unit = comparable_rule_bounds(chosen, event_unit)
                    snapshot_rows.append(
                        {
                            "cycle_id": cycle_id,
                            "strategy": strategy_name,
                            "observed_at": datetime.now().isoformat(timespec="seconds"),
                            "target_date": target.isoformat(),
                            "city": city,
                            "kind": kind,
                            "station": station,
                            "event_market_unit": event_unit,
                            "twc_units_requested": "e,m",
                            "forecast_temp": forecast_temp,
                            "forecast_high": forecast_high,
                            "forecast_low": forecast_low,
                            "forecast_unit": event_unit,
                            "dual_bracket_candidate_source": forecast_meta["candidate_source"],
                            "dual_bracket_primary_value": forecast_meta["primary_value"],
                            "dual_bracket_targets": json.dumps(targets, ensure_ascii=False),
                            "forecast_high_f": forecast_meta["high_f"],
                            "forecast_low_f": forecast_meta["low_f"],
                            "forecast_high_c": forecast_meta["high_c"],
                            "forecast_low_c": forecast_meta["low_c"],
                            "forecast_horizon_hours": 24,
                            "include_observed_today": False,
                            "observed_point_count_f": "",
                            "forecast_point_count_f": len(daily_temps_f),
                            "combined_point_count_f": len(daily_temps_f),
                            "observed_point_count_c": "",
                            "forecast_point_count_c": len(daily_temps_c),
                            "combined_point_count_c": len(daily_temps_c),
                            "city_local_time": city_local_dt.isoformat(),
                            "city_timezone": city_timezone,
                            "city_time_source": city_time_source,
                            "trade_window": window_text,
                            "trade_window_allowed": True,
                            "trade_window_reason": "inside_fixed_local_window",
                            "first_valid_time_local": first_local,
                            "last_valid_time_local": last_local,
                            "chosen_market_id": chosen.market_id,
                            "chosen_condition_id": chosen.condition_id,
                            "chosen_question": chosen.market_question,
                            "chosen_yes_price": chosen.yes_price,
                            "mispricing_price_threshold": price_threshold,
                            "pricing_edge": round(1.0 - float(chosen.yes_price), 8) if chosen.yes_price is not None else "",
                            "should_buy": True,
                            "chosen_rule_min": chosen.rule_min,
                            "chosen_rule_max": chosen.rule_max,
                            "chosen_market_unit": chosen.unit,
                            "chosen_comparable_rule_min": comparable_min,
                            "chosen_comparable_rule_max": comparable_max,
                            "chosen_comparable_unit": comparable_unit,
                            "trade_notional_usdc": config["trading"]["buy_notional_usdc"],
                            "polymarket_url": event_url,
                            "wunderground_source_url": wu_source,
                            "twc_valid_time_local_f_json": json.dumps(daily_times_f, ensure_ascii=False),
                            "twc_temperature_f_json": json.dumps(daily_temps_f, ensure_ascii=False),
                            "twc_raw_payload_f_json": json.dumps(payload_f, ensure_ascii=False, sort_keys=True),
                            "twc_observed_time_local_f_json": "",
                            "twc_observed_temperature_f_json": "",
                            "twc_raw_historical_payload_f_json": "",
                            "twc_valid_time_local_c_json": json.dumps(daily_times_c, ensure_ascii=False),
                            "twc_temperature_c_json": json.dumps(daily_temps_c, ensure_ascii=False),
                            "twc_raw_payload_c_json": json.dumps(payload_c, ensure_ascii=False, sort_keys=True),
                            "twc_observed_time_local_c_json": "",
                            "twc_observed_temperature_c_json": "",
                            "twc_raw_historical_payload_c_json": "",
                            "error": "",
                        }
                    )
                    new_trade = build_trade(
                        config,
                        cycle_id,
                        chosen,
                        wu_source,
                        station,
                        forecast_temp,
                        forecast_high,
                        forecast_low,
                        first_local,
                        last_local,
                    )
                    all_new_trades.append(new_trade)
                    existing_keys.add((chosen.event_date, chosen.city, chosen.kind, chosen.market_id))
                    LOGGER.info(
                        "[%s] buy strategy=%s trade=%s city=%s kind=%s station=%s candidate_source=%s primary=%s targets=%s forecast=%s%s market=%s price=%s threshold=%s notional=%s shares=%s total_cost=%s rule=%s-%s%s",
                        cycle_id,
                        strategy_name,
                        new_trade.trade_id,
                        city,
                        kind,
                        station,
                        forecast_meta["candidate_source"],
                        forecast_meta["primary_value"],
                        targets,
                        forecast_temp,
                        event_unit,
                        chosen.market_id,
                        chosen.yes_price,
                        price_threshold,
                        new_trade.notional_usdc,
                        new_trade.shares,
                        new_trade.total_cost_usdc,
                        comparable_min,
                        comparable_max,
                        comparable_unit,
                    )
            except Exception as exc:
                LOGGER.exception("[%s] event failed city=%s kind=%s url=%s", cycle_id, event.get("_parsed_city", ""), event.get("_parsed_kind", ""), event_url)
                snapshot_rows.append(
                    {
                        "cycle_id": cycle_id,
                        "strategy": strategy_name,
                        "observed_at": datetime.now().isoformat(timespec="seconds"),
                        "target_date": target.isoformat(),
                        "city": event.get("_parsed_city", ""),
                        "kind": event.get("_parsed_kind", ""),
                        "station": "",
                        "event_market_unit": "",
                        "twc_units_requested": "e,m",
                        "forecast_temp": "",
                        "forecast_high": "",
                        "forecast_low": "",
                        "forecast_unit": "",
                        "forecast_high_f": "",
                        "forecast_low_f": "",
                        "forecast_high_c": "",
                        "forecast_low_c": "",
                        "forecast_horizon_hours": 24,
                        "include_observed_today": False,
                        "trade_window_reason": "event_error",
                        "should_buy": "",
                        "polymarket_url": event_url,
                        "error": repr(exc),
                    }
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))

    append_csv(config["outputs"]["snapshots_csv"], snapshot_rows)
    append_twc_raw_wide(config, twc_wide_rows)
    append_csv(config["outputs"]["trades_csv"], [asdict(t) for t in all_new_trades])
    log_info(f"[{cycle_id}] strategy={strategy_name} snapshots={len(snapshot_rows)} twc_wide={len(twc_wide_rows)} new_trades={len(all_new_trades)}")
    return len(all_new_trades)


def run_strategy_cycle(config: dict[str, Any], cycle_id: str) -> int:
    if config["trading"].get("strategy_mode") == "dual_bracket_fixed_time":
        return run_dual_bracket_strategy_cycle(config, cycle_id)
    if config["trading"].get("strategy_mode") == "twc_reprice_momentum":
        return run_strategy4_cycle(config, cycle_id)

    target_dates = [resolve_date(str(v)) for v in config["events"]["target_dates"]]
    all_new_trades: list[PaperTrade] = []
    snapshot_rows: list[dict[str, Any]] = []
    twc_wide_rows: list[dict[str, Any]] = []
    strategy_name = str(config["trading"]["strategy_name"])

    for target in target_dates:
        events = discover_temperature_events(config, target)
        log_info(f"[{cycle_id}] strategy={strategy_name} target={target.isoformat()} events={len(events)}")

        for event in events:
            markets = markets_for_event(config, event)
            event_url = poly_url_from_event(event)
            try:
                event_unit = event_market_unit(markets)
                twc_units = twc_units_for_temperature_unit(event_unit)
                city_local_dt, city_timezone, city_time_source = city_local_now(config, event["_parsed_city"])
                window_allowed, window_text, window_reason = trading_window_status(
                    config,
                    event["_parsed_kind"],
                    city_local_dt,
                )
                observed_at = datetime.now().isoformat(timespec="seconds")
                if not window_allowed:
                    snapshot_rows.append(
                        {
                            "cycle_id": cycle_id,
                            "strategy": strategy_name,
                            "observed_at": observed_at,
                            "target_date": target.isoformat(),
                            "city": event["_parsed_city"],
                            "kind": event["_parsed_kind"],
                            "station": "",
                            "event_market_unit": event_unit,
                            "twc_units_requested": "",
                            "forecast_temp": "",
                            "forecast_high": "",
                            "forecast_low": "",
                            "forecast_unit": event_unit,
                            "forecast_high_f": "",
                            "forecast_low_f": "",
                            "forecast_high_c": "",
                            "forecast_low_c": "",
                            "forecast_horizon_hours": "",
                            "include_observed_today": config["trading"].get("include_observed_today", True),
                            "observed_point_count_f": "",
                            "forecast_point_count_f": "",
                            "combined_point_count_f": "",
                            "observed_point_count_c": "",
                            "forecast_point_count_c": "",
                            "combined_point_count_c": "",
                            "city_local_time": city_local_dt.isoformat() if city_local_dt else "",
                            "city_timezone": city_timezone,
                            "city_time_source": city_time_source,
                            "trade_window": window_text,
                            "trade_window_allowed": False,
                            "trade_window_reason": window_reason,
                            "first_valid_time_local": "",
                            "last_valid_time_local": "",
                            "chosen_market_id": "",
                            "chosen_condition_id": "",
                            "chosen_question": "",
                            "chosen_yes_price": "",
                            "mispricing_price_threshold": "",
                            "pricing_edge": "",
                            "should_buy": False,
                            "chosen_rule_min": "",
                            "chosen_rule_max": "",
                            "chosen_market_unit": "",
                            "chosen_comparable_rule_min": "",
                            "chosen_comparable_rule_max": "",
                            "chosen_comparable_unit": "",
                            "trade_notional_usdc": "",
                            "polymarket_url": event_url,
                            "wunderground_source_url": "",
                            "twc_valid_time_local_f_json": "",
                            "twc_temperature_f_json": "",
                            "twc_raw_payload_f_json": "",
                            "twc_observed_time_local_f_json": "",
                            "twc_observed_temperature_f_json": "",
                            "twc_raw_historical_payload_f_json": "",
                            "twc_valid_time_local_c_json": "",
                            "twc_temperature_c_json": "",
                            "twc_raw_payload_c_json": "",
                            "twc_observed_time_local_c_json": "",
                            "twc_observed_temperature_c_json": "",
                            "twc_raw_historical_payload_c_json": "",
                            "error": "",
                        }
                    )
                    LOGGER.info(
                        "[%s] skip city=%s kind=%s reason=%s local_time=%s timezone=%s",
                        cycle_id,
                        event["_parsed_city"],
                        event["_parsed_kind"],
                        window_reason,
                        city_local_dt.isoformat() if city_local_dt else "",
                        city_timezone,
                    )
                    continue

                wu_source = extract_wunderground_source(config, event_url)
                station = station_from_wu_url(wu_source)
                if not station:
                    raise RuntimeError("No ICAO station code found in Wunderground source URL.")

                horizon_hours = int(config["trading"]["forecast_horizon_hours"])
                payload = twc_hourly_forecast_by_icao(config, station, units=twc_units)
                if config["trading"].get("forecast_scope") == "event_day_full":
                    forecast_points = daily_twc_points(payload, event["_parsed_event_date"])
                else:
                    forecast_points = filtered_twc_points(payload, event["_parsed_event_date"], horizon_hours)
                observed_points: list[tuple[datetime, float]] = []
                historical_payload: dict[str, Any] = {}
                if config["trading"].get("include_observed_today", True) and target == date.today():
                    historical_payload = twc_historical_hourly_by_icao(config, station, units=twc_units)
                    observed_points = observed_twc_points(historical_payload, event["_parsed_event_date"], city_local_dt)
                combined_points = merge_observed_and_forecast_points(observed_points, forecast_points)
                twc_wide_rows.append(
                    twc_raw_wide_row(
                        cycle_id=cycle_id,
                        strategy_name=strategy_name,
                        target_date=target,
                        city=event["_parsed_city"],
                        kind=event["_parsed_kind"],
                        station=station,
                        event_unit=event_unit,
                        twc_units=twc_units,
                        city_local_dt=city_local_dt,
                        city_timezone=city_timezone,
                        observed_points=observed_points,
                        forecast_points=forecast_points,
                        combined_points=combined_points,
                        forecast_payload=payload,
                        historical_payload=historical_payload,
                        event_url=event_url,
                        wunderground_source_url=wu_source,
                    )
                )
                high, low, first_local, last_local, daily_times, daily_temps = summarize_points(combined_points)
                high_f = high if event_unit == "F" else ""
                low_f = low if event_unit == "F" else ""
                high_c = high if event_unit == "C" else ""
                low_c = low if event_unit == "C" else ""
                daily_times_f = daily_times if event_unit == "F" else []
                daily_temps_f = daily_temps if event_unit == "F" else []
                daily_times_c = daily_times if event_unit == "C" else []
                daily_temps_c = daily_temps if event_unit == "C" else []
                observed_points_f = observed_points if event_unit == "F" else []
                observed_points_c = observed_points if event_unit == "C" else []
                forecast_points_f = forecast_points if event_unit == "F" else []
                forecast_points_c = forecast_points if event_unit == "C" else []
                combined_points_f = combined_points if event_unit == "F" else []
                combined_points_c = combined_points if event_unit == "C" else []
                historical_payload_f = historical_payload if event_unit == "F" else {}
                historical_payload_c = historical_payload if event_unit == "C" else {}
                payload_f = payload if event_unit == "F" else {}
                payload_c = payload if event_unit == "C" else {}
                forecasts_by_unit = {
                    event_unit: {"high": high, "low": low},
                }
                forecast_temp = high if event["_parsed_kind"] == "Highest" else low
                forecast_unit = event_unit
                chosen = choose_most_likely_market(markets, forecasts_by_unit, event["_parsed_kind"])
                price_threshold = float(config["trading"].get("mispricing_price_threshold", 1.0))
                should_buy = bool(chosen and chosen.yes_price is not None and chosen.yes_price < price_threshold)
                if chosen:
                    forecast_temp, forecast_high, forecast_low, forecast_unit = native_forecast_for_market(
                        forecasts_by_unit,
                        chosen,
                        event["_parsed_kind"],
                    )
                else:
                    forecast_high, forecast_low = high_f, low_f
                comparable_min, comparable_max, comparable_unit = (
                    comparable_rule_bounds(chosen, forecast_unit) if chosen else (None, None, forecast_unit)
                )
                observed_at = datetime.now().isoformat(timespec="seconds")

                snapshot_rows.append(
                    {
                        "cycle_id": cycle_id,
                        "strategy": strategy_name,
                        "observed_at": observed_at,
                        "target_date": target.isoformat(),
                        "city": event["_parsed_city"],
                        "kind": event["_parsed_kind"],
                        "station": station,
                        "event_market_unit": event_unit,
                        "twc_units_requested": twc_units,
                        "forecast_temp": forecast_temp,
                        "forecast_high": forecast_high,
                        "forecast_low": forecast_low,
                        "forecast_unit": forecast_unit,
                        "forecast_high_f": high_f,
                        "forecast_low_f": low_f,
                        "forecast_high_c": high_c,
                        "forecast_low_c": low_c,
                        "forecast_horizon_hours": horizon_hours,
                        "include_observed_today": config["trading"].get("include_observed_today", True),
                        "observed_point_count_f": len(observed_points_f),
                        "forecast_point_count_f": len(forecast_points_f),
                        "combined_point_count_f": len(combined_points_f),
                        "observed_point_count_c": len(observed_points_c),
                        "forecast_point_count_c": len(forecast_points_c),
                        "combined_point_count_c": len(combined_points_c),
                        "city_local_time": city_local_dt.isoformat() if city_local_dt else "",
                        "city_timezone": city_timezone,
                        "city_time_source": city_time_source,
                        "trade_window": window_text,
                        "trade_window_allowed": window_allowed,
                        "trade_window_reason": window_reason,
                        "first_valid_time_local": first_local,
                        "last_valid_time_local": last_local,
                        "chosen_market_id": chosen.market_id if chosen else "",
                        "chosen_condition_id": chosen.condition_id if chosen else "",
                        "chosen_question": chosen.market_question if chosen else "",
                        "chosen_yes_price": chosen.yes_price if chosen else "",
                        "mispricing_price_threshold": price_threshold,
                        "pricing_edge": round(1.0 - float(chosen.yes_price), 8) if chosen and chosen.yes_price is not None else "",
                        "should_buy": should_buy,
                        "chosen_rule_min": chosen.rule_min if chosen else "",
                        "chosen_rule_max": chosen.rule_max if chosen else "",
                        "chosen_market_unit": chosen.unit if chosen else "",
                        "chosen_comparable_rule_min": comparable_min if chosen else "",
                        "chosen_comparable_rule_max": comparable_max if chosen else "",
                        "chosen_comparable_unit": comparable_unit if chosen else "",
                        "trade_notional_usdc": config["trading"]["buy_notional_usdc"] if chosen else "",
                        "polymarket_url": event_url,
                        "wunderground_source_url": wu_source,
                        "twc_valid_time_local_f_json": json.dumps(daily_times_f, ensure_ascii=False),
                        "twc_temperature_f_json": json.dumps(daily_temps_f, ensure_ascii=False),
                        "twc_raw_payload_f_json": json.dumps(payload_f, ensure_ascii=False, sort_keys=True),
                        "twc_observed_time_local_f_json": json.dumps([dt.isoformat() for dt, _ in observed_points_f], ensure_ascii=False),
                        "twc_observed_temperature_f_json": json.dumps([temp for _, temp in observed_points_f], ensure_ascii=False),
                        "twc_raw_historical_payload_f_json": json.dumps(historical_payload_f, ensure_ascii=False, sort_keys=True),
                        "twc_valid_time_local_c_json": json.dumps(daily_times_c, ensure_ascii=False),
                        "twc_temperature_c_json": json.dumps(daily_temps_c, ensure_ascii=False),
                        "twc_raw_payload_c_json": json.dumps(payload_c, ensure_ascii=False, sort_keys=True),
                        "twc_observed_time_local_c_json": json.dumps([dt.isoformat() for dt, _ in observed_points_c], ensure_ascii=False),
                        "twc_observed_temperature_c_json": json.dumps([temp for _, temp in observed_points_c], ensure_ascii=False),
                        "twc_raw_historical_payload_c_json": json.dumps(historical_payload_c, ensure_ascii=False, sort_keys=True),
                        "error": "",
                    }
                )

                if should_buy and chosen:
                    new_trade = build_trade(
                        config,
                        cycle_id,
                        chosen,
                        wu_source,
                        station,
                        forecast_temp,
                        forecast_high,
                        forecast_low,
                        first_local,
                        last_local,
                    )
                    all_new_trades.append(new_trade)
                    LOGGER.info(
                        "[%s] buy strategy=%s trade=%s city=%s kind=%s station=%s forecast=%s%s market=%s price=%s threshold=%s edge=%s notional=%s shares=%s buy_fee=%s total_cost=%s market_unit=%s comparable_rule=%s-%s%s",
                        cycle_id,
                        strategy_name,
                        new_trade.trade_id,
                        event["_parsed_city"],
                        event["_parsed_kind"],
                        station,
                        forecast_temp,
                        forecast_unit,
                        chosen.market_id,
                        chosen.yes_price,
                        price_threshold,
                        new_trade.pricing_edge,
                        new_trade.notional_usdc,
                        new_trade.shares,
                        new_trade.buy_fee_usdc,
                        new_trade.total_cost_usdc,
                        chosen.unit,
                        comparable_min,
                        comparable_max,
                        comparable_unit,
                    )
                else:
                    log_method = LOGGER.info if not window_allowed else LOGGER.warning
                    log_method(
                        "[%s] skip city=%s kind=%s forecast=%s reason=%s local_time=%s price=%s threshold=%s",
                        cycle_id,
                        event["_parsed_city"],
                        event["_parsed_kind"],
                        forecast_temp,
                        window_reason if not window_allowed else ("price_above_threshold" if chosen else "no_usable_market"),
                        city_local_dt.isoformat() if city_local_dt else "",
                        chosen.yes_price if chosen else "",
                        price_threshold,
                    )
            except Exception as exc:
                LOGGER.exception(
                    "[%s] event failed city=%s kind=%s url=%s",
                    cycle_id,
                    event.get("_parsed_city", ""),
                    event.get("_parsed_kind", ""),
                    event_url,
                )
                snapshot_rows.append(
                    {
                        "cycle_id": cycle_id,
                        "strategy": strategy_name,
                        "observed_at": datetime.now().isoformat(timespec="seconds"),
                        "target_date": target.isoformat(),
                        "city": event.get("_parsed_city", ""),
                        "kind": event.get("_parsed_kind", ""),
                        "station": "",
                        "event_market_unit": "",
                        "twc_units_requested": "",
                        "forecast_temp": "",
                        "forecast_high": "",
                        "forecast_low": "",
                        "forecast_unit": "",
                        "forecast_high_f": "",
                        "forecast_low_f": "",
                        "forecast_high_c": "",
                        "forecast_low_c": "",
                        "forecast_horizon_hours": "",
                        "include_observed_today": "",
                        "observed_point_count_f": "",
                        "forecast_point_count_f": "",
                        "combined_point_count_f": "",
                        "observed_point_count_c": "",
                        "forecast_point_count_c": "",
                        "combined_point_count_c": "",
                        "city_local_time": "",
                        "city_timezone": "",
                        "city_time_source": "",
                        "trade_window": "",
                        "trade_window_allowed": "",
                        "trade_window_reason": "event_error",
                        "first_valid_time_local": "",
                        "last_valid_time_local": "",
                        "chosen_market_id": "",
                        "chosen_condition_id": "",
                        "chosen_question": "",
                        "chosen_yes_price": "",
                        "mispricing_price_threshold": "",
                        "pricing_edge": "",
                        "should_buy": "",
                        "chosen_rule_min": "",
                        "chosen_rule_max": "",
                        "chosen_market_unit": "",
                        "chosen_comparable_rule_min": "",
                        "chosen_comparable_rule_max": "",
                        "chosen_comparable_unit": "",
                        "trade_notional_usdc": "",
                        "polymarket_url": event_url,
                        "wunderground_source_url": "",
                        "twc_valid_time_local_f_json": "",
                        "twc_temperature_f_json": "",
                        "twc_raw_payload_f_json": "",
                        "twc_observed_time_local_f_json": "",
                        "twc_observed_temperature_f_json": "",
                        "twc_raw_historical_payload_f_json": "",
                        "twc_valid_time_local_c_json": "",
                        "twc_temperature_c_json": "",
                        "twc_raw_payload_c_json": "",
                        "twc_observed_time_local_c_json": "",
                        "twc_observed_temperature_c_json": "",
                        "twc_raw_historical_payload_c_json": "",
                        "error": repr(exc),
                    }
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))

    append_csv(config["outputs"]["snapshots_csv"], snapshot_rows)
    append_twc_raw_wide(config, twc_wide_rows)
    append_csv(config["outputs"]["trades_csv"], [asdict(t) for t in all_new_trades])
    log_info(f"[{cycle_id}] strategy={strategy_name} snapshots={len(snapshot_rows)} twc_wide={len(twc_wide_rows)} new_trades={len(all_new_trades)}")
    return len(all_new_trades)


def run_cycle(config: dict[str, Any], cycle_num: int, last_run_at: dict[str, datetime]) -> int:
    now = datetime.now()
    base_cycle_id = now.strftime("%Y%m%dT%H%M%S") + f"-{cycle_num}"
    total_new_trades = 0
    for strategy_config in active_strategy_configs(config):
        strategy_name = strategy_config["trading"]["strategy_name"]
        due, reason = strategy_due(strategy_config, now, last_run_at)
        if not due:
            LOGGER.info("[%s] strategy=%s not due reason=%s", base_cycle_id, strategy_name, reason)
            continue
        total_new_trades += run_strategy_cycle(strategy_config, f"{base_cycle_id}:{strategy_name}")
        last_run_at[strategy_name] = now
    return total_new_trades


def loop_sleep_seconds(config: dict[str, Any]) -> int:
    intervals = [int(config["scheduler"]["poll_interval_minutes"]) * 60]
    if config.get("polymarket_price_snapshots", {}).get("enabled", False):
        intervals.append(int(config["polymarket_price_snapshots"].get("run_every_minutes", 1)) * 60)
    if config.get("twc_raw_collection", {}).get("enabled", False):
        intervals.append(int(config["twc_raw_collection"].get("run_every_minutes", 15)) * 60)
    for strategy in config.get("strategies") or []:
        if strategy.get("enabled", True):
            intervals.append(int(strategy.get("run_every_minutes", config["scheduler"]["poll_interval_minutes"])) * 60)
    return max(1, min(intervals))


def write_state(config: dict[str, Any], cycle_num: int) -> None:
    state = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "cycle_num": cycle_num,
        "config_target_dates": config["events"]["target_dates"],
        "trades_csv": config["outputs"]["trades_csv"],
        "snapshots_csv": config["outputs"]["snapshots_csv"],
        "settled_trades_csv": config["outputs"]["settled_trades_csv"],
        "performance_by_cycle_csv": config["outputs"]["performance_by_cycle_csv"],
        "performance_by_event_csv": config["outputs"]["performance_by_event_csv"],
        "twc_raw_wide_csv": config["outputs"].get("twc_raw_wide_csv", "polymarket_weather_twc_raw_wide.csv"),
        "twc_raw_collection_enabled": bool(config.get("twc_raw_collection", {}).get("enabled", False)),
        "twc_raw_collection_run_every_minutes": int(config.get("twc_raw_collection", {}).get("run_every_minutes", 15)),
        "polymarket_price_snapshots_csv": config["outputs"].get("polymarket_price_snapshots_csv", "polymarket_weather_price_snapshots.csv"),
        "log_file": config["outputs"]["log_file"],
    }
    with open(config["outputs"]["state_json"], "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def summarize_settled(config: dict[str, Any]) -> None:
    trades = read_trades(config["outputs"]["settled_trades_csv"])
    if not trades:
        return
    settled = [t for t in trades if t.status in {"SETTLED", "SOLD"}]
    open_trades = [t for t in trades if t.status not in {"SETTLED", "SOLD"}]
    settled_cost = sum(t.total_cost_usdc for t in settled)
    open_cost = sum(t.total_cost_usdc for t in open_trades)
    total_cost = settled_cost + open_cost
    total_payout = sum(t.payout_usdc for t in settled)
    total_fee = sum(t.buy_fee_usdc for t in trades)
    realized_pnl = total_payout - settled_cost
    log_info(
        f"trades={len(trades)} settled={len(settled)} open={len(open_trades)} "
        f"total_cost=${total_cost:.2f} open_cost=${open_cost:.2f} fees=${total_fee:.2f} "
        f"settled_payout=${total_payout:.2f} realized_pnl=${realized_pnl:.2f}"
    )


def run(config: dict[str, Any]) -> None:
    cycle_num = 0
    last_run_at: dict[str, datetime] = {}
    max_cycles = int(config["scheduler"]["max_cycles"])
    LOGGER.info("bot started config=%s", json.dumps(redacted_config(config), ensure_ascii=False, sort_keys=True))
    start_strategy4_websocket_thread(config)
    while True:
        cycle_num += 1
        run_cycle(config, cycle_num, last_run_at)
        process_strategy4_reprice(config)
        if config["scheduler"]["settle_after_each_cycle"]:
            settle_open_trades(config)
            summarize_settled(config)
        write_state(config, cycle_num)

        if config["scheduler"]["stop_when_all_target_events_settled"]:
            settled_trades = read_trades(config["outputs"]["settled_trades_csv"])
            if settled_trades and all(t.status == "SETTLED" for t in settled_trades):
                log_info("all known paper trades are settled; stopping")
                break

        if config["scheduler"]["run_once"] or (max_cycles and cycle_num >= max_cycles):
            break
        if config["scheduler"].get("align_to_top_of_hour", False):
            now = datetime.now()
            next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            sleep_seconds = max(1, int((next_hour - now).total_seconds()))
        else:
            sleep_seconds = loop_sleep_seconds(config)
        log_info(f"sleeping {sleep_seconds} seconds")
        time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="Run continuously using the config file.")
    run_parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    once_parser = sub.add_parser("once", help="Run one polling cycle using the config file.")
    once_parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    settle_parser = sub.add_parser("settle", help="Settle existing paper trades using Polymarket closed market data.")
    settle_parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    setup_logging(config)
    if args.command == "once":
        config["scheduler"]["run_once"] = True
        run(config)
    elif args.command == "settle":
        settle_open_trades(config)
        summarize_settled(config)
    else:
        run(config)


if __name__ == "__main__":
    main()
