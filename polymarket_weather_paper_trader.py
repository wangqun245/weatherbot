#!/usr/bin/env python3
"""
Deterministic Polymarket weather paper trader.

Only keeps the current strategy:
- Watch one frontier temperature market per event over Polymarket websocket.
- Use AviationWeather METAR as the cheap change detector.
- Verify changed observed extremes with TWC historical observations.
- Buy NO when an option is already impossible, buy YES when the extreme bucket is reached.
- Sell held NO only if corrected observations make that NO possible again.

This is a simulator. It never submits real orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import threading
import time
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


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def default_config() -> dict[str, Any]:
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
        "deterministic_no_max_price": 0.99,
        "deterministic_yes_max_price": 0.99,
        "monitor_price_change_pct": 0.03,
        "aviation_poll_after_observation_minutes": 15,
        "aviation_poll_interval_seconds": 60,
        "twc_verify_interval_seconds": 3,
        "twc_verify_window_seconds": 180,
        "allowed_cities": allowed,
        "websocket_enabled": True,
        "websocket_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "websocket_persistent": True,
        "websocket_reconnect_seconds": 5,
        "websocket_ping_seconds": 10,
        "websocket_timeout_seconds": 10,
        "websocket_asset_refresh_seconds": 300,
    }
    return {
        "api": {
            "polymarket_gamma_base": "https://gamma-api.polymarket.com",
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
            "settled_trades_csv": "polymarket_weather_trades_settled.csv",
            "performance_by_cycle_csv": "polymarket_weather_performance_by_cycle.csv",
            "performance_by_event_csv": "polymarket_weather_performance_by_event.csv",
            "state_json": "polymarket_weather_state.json",
            "log_file": "bot.log",
            "log_level": "INFO",
            "console_log_enabled": False,
        },
        "strategies": [{"name": "deterministic_harvest", "enabled": True, "events": {"target_dates": ["today"]}, "trading": trading}],
    }


def active_config(config: dict[str, Any]) -> dict[str, Any]:
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
    with open(path, "r", encoding="utf-8") as f:
        user_config = json.load(f)
    return active_config(deep_merge(default_config(), user_config))


def redacted_config(config: dict[str, Any]) -> dict[str, Any]:
    def redact(value: Any, key: str = "") -> Any:
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
    outputs = config["outputs"]
    level = getattr(logging, str(outputs.get("log_level", "INFO")).upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.FileHandler(outputs["log_file"], encoding="utf-8")]
    if outputs.get("console_log_enabled", False):
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers, force=True)


def resolve_date(value: str) -> date:
    today = datetime.now().date()
    text = value.strip().lower()
    if text == "today":
        return today
    if text == "tomorrow":
        return today + timedelta(days=1)
    return date.fromisoformat(value)


def infer_year(month_day_text: str, today: Optional[date] = None) -> date:
    base = today or datetime.now().date()
    parsed = datetime.strptime(f"{month_day_text} {base.year}", "%B %d %Y").date()
    if parsed < base - timedelta(days=180):
        parsed = datetime.strptime(f"{month_day_text} {base.year + 1}", "%B %d %Y").date()
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
    return http_get_json(f"{config['api']['polymarket_gamma_base']}{path}", params, int(config["api"]["request_timeout_seconds"]))


def twc_get(config: dict[str, Any], path: str, params: dict[str, Any]) -> Any:
    env_name = str(config["api"].get("twc_api_key_env", "TWC_API_KEY")).strip()
    api_key = str(config["api"].get("twc_api_key", "")).strip() or (os.environ.get(env_name, "").strip() if env_name else "")
    if not api_key:
        raise RuntimeError(f"Missing Weather Company API key. Set config api.twc_api_key or {env_name}.")
    return http_get_json(f"{config['api']['weather_company_base']}{path}", {"apiKey": api_key, **params}, int(config["api"]["request_timeout_seconds"]))


def parse_jsonish(value: Any, default: Any) -> Any:
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
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def parse_event_title(title: str) -> Optional[tuple[str, str, str]]:
    m = TEMP_TITLE_RE.match(title.strip())
    if not m:
        return None
    return m.group(1).title(), m.group(2).strip(), m.group(3).strip()


def poly_url_from_event(event: dict[str, Any]) -> str:
    slug = event.get("slug") or event.get("ticker") or event.get("id", "")
    return urljoin(BASE_POLY, f"/event/{slug}") if slug else BASE_POLY


def discover_temperature_events(config: dict[str, Any], target: date) -> list[dict[str, Any]]:
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
                event_date = infer_year(md)
                if event_date != target or (city_filter and city_filter not in city.lower()) or (allowed and city not in allowed):
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
            event["_parsed_event_date"] = infer_year(md).isoformat()
        if event.get("_parsed_city") == city and event.get("_parsed_kind") == kind and event.get("_parsed_event_date") == event_date:
            return event
    return None


def extract_wunderground_source(config: dict[str, Any], event_url: str) -> str:
    html = http_get_text(event_url, int(config["api"]["request_timeout_seconds"]))
    urls = [u.rstrip(".,") for u in WU_URL_RE.findall(html)]
    return urls[0] if urls else ""


def station_from_wu_url(url: str) -> str:
    parts = [p for p in url.split("?")[0].rstrip("/").split("/") if p]
    if "date" in parts:
        parts = parts[: parts.index("date")]
    station = parts[-1].upper() if parts else ""
    return station if re.fullmatch(r"[A-Z0-9]{4}", station) else ""


def infer_temperature_unit(text: str, default_unit: str = "F") -> str:
    normalized = text.lower()
    if "celsius" in normalized or "°c" in normalized:
        return "C"
    if "fahrenheit" in normalized or "°f" in normalized:
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
    if len(values) >= 2 and (re.search(r"\bbetween\b|\bfrom\b|\bto\b|\bthrough\b|\brange\b", low_text) or re.search(r"\d\s*-\s*\d", low_text)):
        return min(values[0], values[1]), max(values[0], values[1]), unit
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
    for market in markets:
        if market.unit:
            return market.unit.upper()
    return "F"


def convert_temperature(value: Optional[float], from_unit: str, to_unit: str) -> Optional[float]:
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
    return convert_temperature(market.rule_min, market.unit, target_unit), convert_temperature(market.rule_max, market.unit, target_unit), target_unit.upper()


def market_contains_temperature(market: TemperatureMarket, target_temp: float, target_unit: str) -> bool:
    lo, hi, _ = comparable_rule_bounds(market, target_unit)
    return (lo is None or target_temp >= lo) and (hi is None or target_temp <= hi)


def sorted_markets_for_unit(markets: list[TemperatureMarket], target_unit: str) -> list[TemperatureMarket]:
    def sort_key(market: TemperatureMarket) -> tuple[float, float, str]:
        lo, hi, _ = comparable_rule_bounds(market, target_unit)
        return lo if lo is not None else -9999.0, hi if hi is not None else 9999.0, market.market_id
    return sorted([m for m in markets if not m.closed], key=sort_key)


def market_no_price(market: TemperatureMarket) -> Optional[float]:
    raw = parse_jsonish(market.raw_market_json, {})
    no_price = outcome_price(raw, "No") if isinstance(raw, dict) else None
    if no_price is not None:
        return no_price
    if market.yes_price is not None:
        return max(0.0, min(1.0, 1.0 - float(market.yes_price)))
    return None


def deterministic_ordered_markets(markets: list[TemperatureMarket], event_unit: str, kind: str) -> list[TemperatureMarket]:
    ordered = sorted_markets_for_unit(markets, event_unit)
    return ordered if kind == "Highest" else list(reversed(ordered))


def deterministic_market_impossible(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        return observed_high is not None and hi is not None and float(observed_high) > hi
    return observed_low is not None and lo is not None and float(observed_low) < lo


def deterministic_no_possible_again(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        return hi is None or observed_high is None or float(observed_high) <= hi
    return lo is None or observed_low is None or float(observed_low) >= lo


def deterministic_extreme_market_reached(market: TemperatureMarket, kind: str, observed_high: Optional[float], observed_low: Optional[float], unit: str) -> bool:
    lo, hi, _ = comparable_rule_bounds(market, unit)
    if kind == "Highest":
        if observed_high is None:
            return False
        return market_contains_temperature(market, float(observed_high), unit) if hi is not None else lo is None or float(observed_high) >= lo
    if observed_low is None:
        return False
    return market_contains_temperature(market, float(observed_low), unit) if lo is not None else hi is None or float(observed_low) <= hi


def twc_units_for_temperature_unit(unit: str) -> str:
    return "m" if unit.upper() == "C" else "e"


def twc_historical_observations_by_icao(config: dict[str, Any], icao_code: str, units: str, event_date: str) -> dict[str, Any]:
    ymd = event_date.replace("-", "")
    return twc_get(config, f"/v1/location/{icao_code}:9:US/observations/historical.json", {"units": units, "startDate": ymd, "endDate": ymd})


def parse_twc_obs_time(row: dict[str, Any]) -> Optional[datetime]:
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
    if not points:
        return None, None, "", "", [], []
    sorted_points = sorted(points, key=lambda item: item[0])
    temps = [temp for _, temp in sorted_points]
    times = [dt.isoformat() for dt, _ in sorted_points]
    return max(temps), min(temps), times[0], times[-1], times, temps


def deterministic_observed_extremes_from_twc(config: dict[str, Any], station: str, event_date: str, city_local_dt: Optional[datetime], unit: str) -> tuple[Optional[float], Optional[float], list[tuple[datetime, float]], dict[str, Any]]:
    payload = twc_historical_observations_by_icao(config, station, twc_units_for_temperature_unit(unit), event_date)
    points = observed_twc_observation_points(payload, event_date, city_local_dt)
    high, low, _, _, _, _ = summarize_points(points)
    return high, low, points, payload


def aviation_metar_observations(station: str, hours: int = 24) -> list[dict[str, Any]]:
    r = requests.get("https://aviationweather.gov/api/data/metar", headers=HEADERS, params={"ids": station, "hours": hours, "format": "json"}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, list) else []


def parse_aviation_obs_time(value: Any) -> Optional[datetime]:
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
    tz_name = config["events"].get("city_timezones", {}).get(city, "UTC")
    value = datetime.now(ZoneInfo(tz_name))
    return value, tz_name, value.isoformat(timespec="seconds")


def aviation_observed_extremes(config: dict[str, Any], station: str, city: str, event_date: str, unit: str) -> tuple[Optional[float], Optional[float], Optional[datetime], list[tuple[datetime, float]]]:
    city_tz = ZoneInfo(config["events"].get("city_timezones", {}).get(city, "UTC"))
    points: list[tuple[datetime, float]] = []
    for row in aviation_metar_observations(station, 24):
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


def deterministic_twc_confirms_aviation(config: dict[str, Any], station: str, event_date: str, city_local_dt: Optional[datetime], unit: str, kind: str, aviation_high: Optional[float], aviation_low: Optional[float]) -> bool:
    twc_high, twc_low, _, _ = deterministic_observed_extremes_from_twc(config, station, event_date, city_local_dt, unit)
    if kind == "Highest":
        return aviation_high is not None and twc_high is not None and float(twc_high) >= float(aviation_high)
    return aviation_low is not None and twc_low is not None and float(twc_low) <= float(aviation_low)


def wait_for_twc_confirmation(config: dict[str, Any], event: dict[str, Any], station: str, event_unit: str, aviation_high: Optional[float], aviation_low: Optional[float]) -> bool:
    interval = max(1, int(config["trading"].get("twc_verify_interval_seconds", 3)))
    deadline = time.monotonic() + max(interval, int(config["trading"].get("twc_verify_window_seconds", 180)))
    city = event["_parsed_city"]
    kind = event["_parsed_kind"]
    event_date = event["_parsed_event_date"]
    while time.monotonic() <= deadline:
        city_local_dt, _, _ = city_local_now(config, city)
        if deterministic_twc_confirms_aviation(config, station, event_date, city_local_dt, event_unit, kind, aviation_high, aviation_low):
            return True
        time.sleep(interval)
    return False


def taker_fee_usdc(shares: float, price: float, fee_rate: float, fee_enabled: bool) -> float:
    return 0.0 if not fee_enabled else round(fee_rate * shares * price * (1.0 - price), 8)


def make_trade(config: dict[str, Any], cycle_id: str, market: TemperatureMarket, wu_source: str, station: str, side: str, observed_high: Optional[float], observed_low: Optional[float], reason: str) -> PaperTrade:
    now = datetime.now().isoformat(timespec="seconds")
    side = side.upper()
    price = float(market.yes_price if side == "YES" else market_no_price(market) or 0.0)
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
    )


def close_trade(config: dict[str, Any], trade: PaperTrade, market: TemperatureMarket, reason: str) -> None:
    side = (trade.position_side or "YES").upper()
    if side == "NO":
        exit_price = float(market_no_price(market) or 0.0)
        trade.exit_action = "sell_no"
        trade.exit_no_price = exit_price
    else:
        exit_price = float(market.yes_price or 0.0)
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


def read_csv_dicts(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with IO_LOCK, open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: str, rows: Iterable[Any]) -> None:
    data = [asdict(r) if hasattr(r, "__dataclass_fields__") else dict(r) for r in rows]
    if not data:
        return
    with IO_LOCK, open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


def read_trades(path: str) -> list[PaperTrade]:
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
    return round((numerator / denominator * 100.0), 4) if denominator else 0.0


def performance_row(group_name: str, group_value: str, rows: list[PaperTrade]) -> dict[str, Any]:
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
    outcome = market.get("outcome") or market.get("winningOutcome") or market.get("resolution") or ""
    return outcome.title() if isinstance(outcome, str) and outcome.lower() in {"yes", "no"} else ""


def settle_open_trades(config: dict[str, Any]) -> list[PaperTrade]:
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
    return any(t.status == "OPEN" and t.strategy == strategy_name and t.market_id == market_id and (t.position_side or "YES").upper() == side.upper() for t in trades)


def process_deterministic_harvest(config: dict[str, Any], trigger_context: dict[str, Any]) -> list[PaperTrade]:
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
    city_local_dt, _, _ = city_local_now(config, city)
    observed_high, observed_low, _, _ = deterministic_observed_extremes_from_twc(config, station, event_date, city_local_dt, event_unit)
    EXTREMES_BY_EVENT[(event_date, city, kind)] = {"observed_high": observed_high, "observed_low": observed_low, "checked_at": datetime.now().isoformat(timespec="seconds")}

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
            close_trade(config, trade, market, "deterministic_twc_correction_no_possible_again")
            changed = True
            LOGGER.info("deterministic correction sell_no trade=%s city=%s kind=%s market=%s", trade.trade_id, city, kind, market.market_id)

    ordered = deterministic_ordered_markets(markets, event_unit, kind)
    extreme_market = ordered[-1] if ordered else None
    extreme_reached = bool(extreme_market and deterministic_extreme_market_reached(extreme_market, kind, observed_high, observed_low, event_unit))
    no_max = float(config["trading"].get("deterministic_no_max_price", 0.99))
    yes_max = float(config["trading"].get("deterministic_yes_max_price", 0.99))
    cycle_id = datetime.now().strftime("%Y%m%dT%H%M%S") + ":deterministic_harvest"

    if extreme_market and extreme_reached and not open_trade_exists(trades + new_trades, strategy_name, extreme_market.market_id, "YES"):
        price = float(extreme_market.yes_price or 0.0)
        if 0 < price <= yes_max:
            new_trades.append(make_trade(config, cycle_id, extreme_market, wu_source, station, "YES", observed_high, observed_low, "extreme_yes_reached"))
            changed = True
            LOGGER.info("deterministic buy_yes city=%s kind=%s market=%s price=%s", city, kind, extreme_market.market_id, price)

    if not extreme_reached:
        for market in ordered:
            if not deterministic_market_impossible(market, kind, observed_high, observed_low, event_unit):
                continue
            if open_trade_exists(trades + new_trades, strategy_name, market.market_id, "NO"):
                continue
            no_price = market_no_price(market)
            if no_price is None or no_price <= 0 or no_price > no_max:
                continue
            new_trades.append(make_trade(config, cycle_id, market, wu_source, station, "NO", observed_high, observed_low, "impossible_no"))
            changed = True
            LOGGER.info("deterministic buy_no city=%s kind=%s market=%s no_price=%s observed_high=%s observed_low=%s", city, kind, market.market_id, no_price, observed_high, observed_low)
            break

    if new_trades:
        trades.extend(new_trades)
    if changed:
        write_csv(config["outputs"]["trades_csv"], trades)
        write_csv(config["outputs"]["settled_trades_csv"], trades)
        write_performance_reports(config, trades)
    return trades


def market_clob_tokens(market_json: dict[str, Any]) -> list[tuple[str, str]]:
    outcomes = parse_jsonish(market_json.get("outcomes"), [])
    token_ids = parse_jsonish(market_json.get("clobTokenIds") or market_json.get("clobTokenIDs"), [])
    rows: list[tuple[str, str]] = []
    for idx, token in enumerate(token_ids):
        outcome = str(outcomes[idx]) if idx < len(outcomes) else ""
        if token:
            rows.append((str(token), outcome))
    return rows


def websocket_message_prices(message: Any) -> list[tuple[str, float, str]]:
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            return []
    rows: list[tuple[str, float, str]] = []
    if isinstance(message, list):
        for item in message:
            rows.extend(websocket_message_prices(item))
        return rows
    if not isinstance(message, dict):
        return rows
    for nested_key in ("data", "payload", "event"):
        nested = message.get(nested_key)
        if isinstance(nested, (dict, list)):
            rows.extend(websocket_message_prices(nested))
    asset_id = str(message.get("asset_id") or message.get("assetId") or message.get("token_id") or message.get("tokenId") or "")
    for field_name in ("price", "best_bid", "best_ask", "last_price"):
        if asset_id and message.get(field_name) not in (None, ""):
            try:
                rows.append((asset_id, float(message[field_name]), field_name))
            except (TypeError, ValueError):
                pass
    return rows


def frontier_markets(config: dict[str, Any], markets: list[TemperatureMarket], event_unit: str, kind: str, trades: list[PaperTrade], observed_high: Optional[float], observed_low: Optional[float]) -> list[TemperatureMarket]:
    min_yes = float(config["trading"].get("deterministic_min_yes_price", 0.01))
    strategy_name = str(config["trading"]["strategy_name"])
    open_no_ids = {t.market_id for t in trades if t.status == "OPEN" and t.strategy == strategy_name and (t.position_side or "YES").upper() == "NO"}
    ordered = deterministic_ordered_markets(markets, event_unit, kind)
    if not ordered:
        return []
    extreme = ordered[-1]
    if deterministic_extreme_market_reached(extreme, kind, observed_high, observed_low, event_unit):
        return [extreme]
    for market in ordered:
        if market.market_id in open_no_ids:
            continue
        if market.yes_price is not None and float(market.yes_price) > min_yes:
            return [market]
    return []


def websocket_assets(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    trades = read_trades(config["outputs"]["trades_csv"])
    assets: dict[str, dict[str, Any]] = {}
    for target in [resolve_date(str(v)) for v in config["events"]["target_dates"]]:
        for event in discover_temperature_events(config, target):
            city = event["_parsed_city"]
            kind = event["_parsed_kind"]
            event_date = event["_parsed_event_date"]
            try:
                markets = markets_for_event(config, event)
                obs = EXTREMES_BY_EVENT.get((event_date, city, kind), {})
                selected = frontier_markets(config, markets, event_market_unit(markets), kind, trades, obs.get("observed_high"), obs.get("observed_low"))
                for market in selected:
                    raw = parse_jsonish(market.raw_market_json, {})
                    for asset_id, outcome in market_clob_tokens(raw):
                        if outcome.lower() == "yes":
                            assets[asset_id] = {"asset_id": asset_id, "market_id": market.market_id, "city": city, "kind": kind, "event_date": event_date, "polymarket_url": poly_url_from_event(event), "last_price": market.yes_price}
            except Exception:
                LOGGER.exception("websocket asset discovery failed city=%s kind=%s", city, kind)
    return assets


def monitor_websocket(config: dict[str, Any], duration_seconds: int = 0) -> bool:
    if not config["trading"].get("websocket_enabled", True):
        return False
    try:
        import websocket  # type: ignore
    except ImportError:
        LOGGER.warning("websocket disabled: install websocket-client from requirements.txt")
        return False
    ws_url = str(config["trading"].get("websocket_url"))
    timeout_seconds = int(config["trading"].get("websocket_timeout_seconds", 10))
    refresh_seconds = int(config["trading"].get("websocket_asset_refresh_seconds", 300))
    trigger_pct = float(config["trading"].get("monitor_price_change_pct", 0.03))
    assets = websocket_assets(config)
    if not assets:
        LOGGER.info("websocket no frontier assets")
        return False
    asset_ids = sorted(assets)
    ws = websocket.create_connection(ws_url, timeout=timeout_seconds)
    try:
        ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
        started = time.monotonic()
        last_refresh = started
        while duration_seconds <= 0 or time.monotonic() - started < duration_seconds:
            if time.monotonic() - last_refresh >= refresh_seconds:
                assets = websocket_assets(config)
                new_ids = sorted(assets)
                if new_ids and new_ids != asset_ids:
                    asset_ids = new_ids
                    ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
                last_refresh = time.monotonic()
            try:
                message = ws.recv()
            except Exception:
                break
            for asset_id, price, price_field in websocket_message_prices(message):
                asset = assets.get(asset_id)
                if not asset:
                    continue
                previous = asset.get("last_price")
                asset["last_price"] = price
                if previous is None:
                    continue
                previous_price = float(previous)
                if previous_price <= 0:
                    continue
                price_change_pct = abs(float(price) - previous_price) / previous_price
                if price_change_pct < trigger_pct:
                    continue
                context = {
                    "source": "polymarket_websocket_frontier",
                    "asset_id": asset_id,
                    "price": price,
                    "previous_price": previous_price,
                    "price_change_pct": price_change_pct,
                    "price_field": price_field,
                    **asset,
                }
                LOGGER.info("websocket trigger %s", json.dumps(context, ensure_ascii=False, sort_keys=True))
                process_deterministic_harvest(config, context)
                assets = websocket_assets(config)
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return True


def websocket_supervisor(config: dict[str, Any]) -> None:
    reconnect = int(config["trading"].get("websocket_reconnect_seconds", 5))
    LOGGER.info("websocket supervisor started")
    while True:
        try:
            monitor_websocket(config, 0)
        except Exception:
            LOGGER.exception("websocket monitor failed")
        time.sleep(reconnect)


def start_websocket_thread(config: dict[str, Any]) -> threading.Thread:
    thread = threading.Thread(target=websocket_supervisor, args=(config,), name="deterministic-websocket", daemon=True)
    thread.start()
    return thread


def aviation_supervisor(config: dict[str, Any]) -> None:
    poll_seconds = max(10, int(config["trading"].get("aviation_poll_interval_seconds", 60)))
    lag_minutes = max(0, int(config["trading"].get("aviation_poll_after_observation_minutes", 15)))
    event_cache: list[dict[str, Any]] = []
    event_refresh_at = 0.0
    state: dict[tuple[str, str, str], dict[str, Any]] = {}
    LOGGER.info("aviation supervisor started")
    while True:
        try:
            if time.monotonic() >= event_refresh_at:
                event_cache = []
                for target in [resolve_date(str(v)) for v in config["events"]["target_dates"]]:
                    event_cache.extend(discover_temperature_events(config, target))
                event_refresh_at = time.monotonic() + 300
            now_utc = datetime.now(timezone.utc)
            for event in event_cache:
                city = event["_parsed_city"]
                kind = event["_parsed_kind"]
                event_date = event["_parsed_event_date"]
                event_url = poly_url_from_event(event)
                try:
                    markets = markets_for_event(config, event)
                    event_unit = event_market_unit(markets)
                    station = station_from_wu_url(extract_wunderground_source(config, event_url))
                    if not station:
                        continue
                    aviation_high, aviation_low, latest_dt, _ = aviation_observed_extremes(config, station, city, event_date, event_unit)
                    if latest_dt is None or latest_dt.astimezone(timezone.utc) + timedelta(minutes=lag_minutes) > now_utc:
                        continue
                    key = (event_date, city, kind)
                    previous = state.get(key, {})
                    changed = (kind == "Highest" and aviation_high is not None and aviation_high != previous.get("aviation_high")) or (kind == "Lowest" and aviation_low is not None and aviation_low != previous.get("aviation_low"))
                    state[key] = {"aviation_high": aviation_high, "aviation_low": aviation_low, "latest_dt": latest_dt.isoformat()}
                    if not changed:
                        continue
                    LOGGER.info("aviation extreme changed city=%s kind=%s high=%s low=%s latest=%s", city, kind, aviation_high, aviation_low, latest_dt.isoformat())
                    if wait_for_twc_confirmation(config, event, station, event_unit, aviation_high, aviation_low):
                        process_deterministic_harvest(config, {"source": "aviation_metar_twc_confirmed", "city": city, "kind": kind, "event_date": event_date, "polymarket_url": event_url})
                    else:
                        LOGGER.info("twc not confirmed city=%s kind=%s station=%s high=%s low=%s", city, kind, station, aviation_high, aviation_low)
                except Exception:
                    LOGGER.exception("aviation event failed city=%s kind=%s", city, kind)
        except Exception:
            LOGGER.exception("aviation supervisor loop failed")
        time.sleep(poll_seconds)


def start_aviation_thread(config: dict[str, Any]) -> threading.Thread:
    thread = threading.Thread(target=aviation_supervisor, args=(config,), name="deterministic-aviation", daemon=True)
    thread.start()
    return thread


def write_state(config: dict[str, Any], cycle_num: int) -> None:
    state = {"cycle_num": cycle_num, "updated_at": datetime.now().isoformat(timespec="seconds"), "strategy_mode": config["trading"].get("strategy_mode")}
    with IO_LOCK, open(config["outputs"]["state_json"], "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=True)


def summarize_settled(config: dict[str, Any]) -> None:
    trades = read_trades(config["outputs"]["trades_csv"])
    if trades:
        write_performance_reports(config, trades)


def run(config: dict[str, Any]) -> None:
    cycle_num = 0
    max_cycles = int(config["scheduler"].get("max_cycles", 0))
    LOGGER.info("bot started config=%s", json.dumps(redacted_config(config), ensure_ascii=False, sort_keys=True))
    start_websocket_thread(config)
    start_aviation_thread(config)
    while True:
        cycle_num += 1
        if config["scheduler"].get("settle_after_each_cycle", True):
            settle_open_trades(config)
            summarize_settled(config)
        write_state(config, cycle_num)
        if max_cycles and cycle_num >= max_cycles:
            break
        time.sleep(max(10, int(config["scheduler"].get("poll_interval_minutes", 15)) * 60))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic Polymarket weather paper trader")
    parser.add_argument("command", choices=["run", "once", "settle"], help="Command to run")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config JSON")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    setup_logging(config)
    if args.command == "settle":
        settle_open_trades(config)
        summarize_settled(config)
        return
    if args.command == "once":
        settle_open_trades(config)
        summarize_settled(config)
        write_state(config, 1)
        return
    run(config)


if __name__ == "__main__":
    main()
