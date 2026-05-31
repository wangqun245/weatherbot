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
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional
from urllib.parse import urljoin

import requests

BASE_POLY = "https://polymarket.com"
DEFAULT_CONFIG_PATH = "polymarket_weather_config.json"

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
    yes_price: Optional[float]
    notional_usdc: float
    shares: float
    taker_fee_rate: float
    buy_fee_usdc: float
    total_cost_usdc: float
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
            "twc_duration": "2day",
            "twc_units": "e",
            "twc_language": "en-US",
            "request_timeout_seconds": 30,
            "per_request_delay_seconds": 0.25,
        },
        "events": {
            "target_dates": ["tomorrow"],
            "city_filter": "",
            "include_closed": False,
            "max_offsets": 1200,
        },
        "trading": {
            "strategy_name": "twc_every_15m_most_likely",
            "buy_notional_usdc": 5.0,
            "fee_rate": 0.05,
            "fee_enabled": True,
            "one_trade_per_event_per_cycle": True,
        },
        "scheduler": {
            "poll_interval_minutes": 15,
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
            "state_json": "polymarket_weather_state.json",
        },
    }


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        user_config = json.load(f)
    return deep_merge(default_config(), user_config)


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
    api_key = os.environ.get(config["api"]["twc_api_key_env"], "").strip()
    if not api_key:
        raise RuntimeError(f"Missing {config['api']['twc_api_key_env']} environment variable.")
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


def parse_temperature_rule(text: str, default_unit: str = "F") -> tuple[Optional[float], Optional[float], str]:
    normalized = text.replace("\u2013", "-").replace("\u2014", "-")
    low_text = normalized.lower()
    nums = [(float(n), (u or default_unit).upper()) for n, u in TEMP_NUMBER_RE.findall(normalized)]
    unit = nums[0][1] if nums else default_unit
    values = [n for n, _ in nums]
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
        rule_min, rule_max, unit = parse_temperature_rule(question)
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


def twc_hourly_forecast_by_icao(config: dict[str, Any], icao_code: str) -> dict[str, Any]:
    return twc_get(
        config,
        f"/v3/wx/forecast/hourly/{config['api']['twc_duration']}",
        {
            "icaoCode": icao_code,
            "units": config["api"]["twc_units"],
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


def market_distance(forecast: float, market: TemperatureMarket) -> tuple[float, float, float]:
    lo, hi = market.rule_min, market.rule_max
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


def choose_most_likely_market(markets: list[TemperatureMarket], forecast_temp: Optional[float]) -> Optional[TemperatureMarket]:
    if forecast_temp is None:
        return None
    usable = [
        m
        for m in markets
        if m.yes_price is not None and m.yes_price > 0 and (m.rule_min is not None or m.rule_max is not None)
    ]
    return sorted(usable, key=lambda m: market_distance(forecast_temp, m))[0] if usable else None


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
    notional = float(config["trading"]["buy_notional_usdc"])
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
        forecast_unit="F" if config["api"]["twc_units"] == "e" else "C",
        rule_min=market.rule_min,
        rule_max=market.rule_max,
        market_unit=market.unit,
        yes_price=market.yes_price,
        notional_usdc=round(notional, 6),
        shares=round(shares, 8),
        taker_fee_rate=float(config["trading"]["fee_rate"]),
        buy_fee_usdc=round(fee, 8),
        total_cost_usdc=round(notional + fee, 8),
    )


def append_csv(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def read_csv_dicts(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
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
                "yes_price",
                "notional_usdc",
                "shares",
                "taker_fee_rate",
                "buy_fee_usdc",
                "total_cost_usdc",
                "payout_usdc",
                "pnl_usdc",
            }:
                cleaned[field] = float(value) if value not in {"", "None", None} else None
            else:
                cleaned[field] = value
        cleaned.setdefault("forecast_first_valid_time_local", "")
        cleaned.setdefault("forecast_last_valid_time_local", "")
        for field in {"notional_usdc", "shares", "taker_fee_rate", "buy_fee_usdc", "total_cost_usdc", "payout_usdc", "pnl_usdc"}:
            cleaned[field] = float(cleaned[field] or 0.0)
        trades.append(PaperTrade(**cleaned))
    return trades


def write_csv(path: str, rows: Iterable[Any]) -> None:
    materialized = [asdict(r) if hasattr(r, "__dataclass_fields__") else dict(r) for r in rows]
    fieldnames = sorted({k for row in materialized for k in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized)


def pct(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 8) if denominator else 0.0


def performance_row(group_name: str, group_value: str, rows: list[PaperTrade]) -> dict[str, Any]:
    settled = [t for t in rows if t.status == "SETTLED"]
    total_notional = sum(t.notional_usdc for t in rows)
    total_fees = sum(t.buy_fee_usdc for t in rows)
    total_cost = sum(t.total_cost_usdc for t in rows)
    total_payout = sum(t.payout_usdc for t in settled)
    wins = [t for t in settled if t.pnl_usdc > 0]
    losses = [t for t in settled if t.pnl_usdc < 0]
    pnl = total_payout - total_cost
    return {
        group_name: group_value,
        "trade_count": len(rows),
        "settled_count": len(settled),
        "win_count": len(wins),
        "loss_count": len(losses),
        "open_count": len(rows) - len(settled),
        "total_notional_usdc": round(total_notional, 8),
        "total_fees_usdc": round(total_fees, 8),
        "total_cost_usdc": round(total_cost, 8),
        "total_payout_usdc": round(total_payout, 8),
        "total_pnl_usdc": round(pnl, 8),
        "roi_on_total_cost": pct(pnl, total_cost),
        "win_rate_settled": pct(len(wins), len(settled)),
    }


def write_performance_reports(config: dict[str, Any], trades: list[PaperTrade]) -> None:
    by_cycle: dict[str, list[PaperTrade]] = {}
    by_event: dict[str, list[PaperTrade]] = {}
    for trade in trades:
        by_cycle.setdefault(trade.cycle_id, []).append(trade)
        event_key = f"{trade.event_date}|{trade.city}|{trade.kind}"
        by_event.setdefault(event_key, []).append(trade)

    cycle_rows = [performance_row("cycle_id", key, rows) for key, rows in sorted(by_cycle.items())]
    event_rows = []
    for key, rows in sorted(by_event.items()):
        row = performance_row("event_key", key, rows)
        first = rows[0]
        row.update(
            {
                "event_date": first.event_date,
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
        return []

    market_cache: dict[str, Optional[dict[str, Any]]] = {}
    for trade in trades:
        if trade.status == "SETTLED":
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

    write_csv(config["outputs"]["settled_trades_csv"], trades)
    write_performance_reports(config, trades)
    return trades


def all_events_settled(events: list[dict[str, Any]]) -> bool:
    if not events:
        return False
    return all(parse_bool(e.get("closed")) for e in events)


def run_cycle(config: dict[str, Any], cycle_num: int) -> int:
    cycle_id = datetime.now().strftime("%Y%m%dT%H%M%S") + f"-{cycle_num}"
    target_dates = [resolve_date(str(v)) for v in config["events"]["target_dates"]]
    all_new_trades: list[PaperTrade] = []
    snapshot_rows: list[dict[str, Any]] = []

    for target in target_dates:
        events = discover_temperature_events(config, target)
        print(f"[{cycle_id}] {target.isoformat()} events={len(events)}", file=sys.stderr)

        for event in events:
            markets = markets_for_event(config, event)
            event_url = poly_url_from_event(event)
            try:
                wu_source = extract_wunderground_source(config, event_url)
                station = station_from_wu_url(wu_source)
                if not station:
                    raise RuntimeError("No ICAO station code found in Wunderground source URL.")

                payload = twc_hourly_forecast_by_icao(config, station)
                high, low, first_local, last_local = summarize_twc_daily_forecast(payload, event["_parsed_event_date"])
                daily_times, daily_temps = twc_daily_series(payload, event["_parsed_event_date"])
                forecast_temp = high if event["_parsed_kind"] == "Highest" else low
                chosen = choose_most_likely_market(markets, forecast_temp)
                observed_at = datetime.now().isoformat(timespec="seconds")

                snapshot_rows.append(
                    {
                        "cycle_id": cycle_id,
                        "observed_at": observed_at,
                        "target_date": target.isoformat(),
                        "city": event["_parsed_city"],
                        "kind": event["_parsed_kind"],
                        "station": station,
                        "forecast_temp": forecast_temp,
                        "forecast_high": high,
                        "forecast_low": low,
                        "forecast_unit": "F" if config["api"]["twc_units"] == "e" else "C",
                        "first_valid_time_local": first_local,
                        "last_valid_time_local": last_local,
                        "chosen_market_id": chosen.market_id if chosen else "",
                        "chosen_condition_id": chosen.condition_id if chosen else "",
                        "chosen_question": chosen.market_question if chosen else "",
                        "chosen_yes_price": chosen.yes_price if chosen else "",
                        "chosen_rule_min": chosen.rule_min if chosen else "",
                        "chosen_rule_max": chosen.rule_max if chosen else "",
                        "trade_notional_usdc": config["trading"]["buy_notional_usdc"] if chosen else "",
                        "polymarket_url": event_url,
                        "wunderground_source_url": wu_source,
                        "twc_valid_time_local_json": json.dumps(daily_times, ensure_ascii=False),
                        "twc_temperature_json": json.dumps(daily_temps, ensure_ascii=False),
                        "twc_raw_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        "error": "",
                    }
                )

                if chosen:
                    all_new_trades.append(
                        build_trade(
                            config,
                            cycle_id,
                            chosen,
                            wu_source,
                            station,
                            forecast_temp,
                            high,
                            low,
                            first_local,
                            last_local,
                        )
                    )
            except Exception as exc:
                snapshot_rows.append(
                    {
                        "cycle_id": cycle_id,
                        "observed_at": datetime.now().isoformat(timespec="seconds"),
                        "target_date": target.isoformat(),
                        "city": event.get("_parsed_city", ""),
                        "kind": event.get("_parsed_kind", ""),
                        "station": "",
                        "forecast_temp": "",
                        "forecast_high": "",
                        "forecast_low": "",
                        "forecast_unit": "",
                        "first_valid_time_local": "",
                        "last_valid_time_local": "",
                        "chosen_market_id": "",
                        "chosen_condition_id": "",
                        "chosen_question": "",
                        "chosen_yes_price": "",
                        "chosen_rule_min": "",
                        "chosen_rule_max": "",
                        "trade_notional_usdc": "",
                        "polymarket_url": event_url,
                        "wunderground_source_url": "",
                        "twc_valid_time_local_json": "",
                        "twc_temperature_json": "",
                        "twc_raw_payload_json": "",
                        "error": repr(exc),
                    }
                )
            time.sleep(float(config["api"]["per_request_delay_seconds"]))

    append_csv(config["outputs"]["snapshots_csv"], snapshot_rows)
    append_csv(config["outputs"]["trades_csv"], [asdict(t) for t in all_new_trades])
    print(f"[{cycle_id}] snapshots={len(snapshot_rows)} new_trades={len(all_new_trades)}", file=sys.stderr)
    return len(all_new_trades)


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
    }
    with open(config["outputs"]["state_json"], "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def summarize_settled(config: dict[str, Any]) -> None:
    trades = read_trades(config["outputs"]["settled_trades_csv"])
    if not trades:
        return
    settled = [t for t in trades if t.status == "SETTLED"]
    total_cost = sum(t.total_cost_usdc for t in trades)
    total_payout = sum(t.payout_usdc for t in settled)
    total_fee = sum(t.buy_fee_usdc for t in trades)
    print(
        f"trades={len(trades)} settled={len(settled)} total_cost=${total_cost:.2f} "
        f"fees=${total_fee:.2f} payout=${total_payout:.2f} pnl=${total_payout - total_cost:.2f}",
        file=sys.stderr,
    )


def run(config: dict[str, Any]) -> None:
    cycle_num = 0
    max_cycles = int(config["scheduler"]["max_cycles"])
    while True:
        cycle_num += 1
        run_cycle(config, cycle_num)
        if config["scheduler"]["settle_after_each_cycle"]:
            settle_open_trades(config)
            summarize_settled(config)
        write_state(config, cycle_num)

        if config["scheduler"]["stop_when_all_target_events_settled"]:
            settled_trades = read_trades(config["outputs"]["settled_trades_csv"])
            if settled_trades and all(t.status == "SETTLED" for t in settled_trades):
                print("all known paper trades are settled; stopping", file=sys.stderr)
                break

        if config["scheduler"]["run_once"] or (max_cycles and cycle_num >= max_cycles):
            break
        sleep_seconds = int(config["scheduler"]["poll_interval_minutes"]) * 60
        print(f"sleeping {sleep_seconds} seconds", file=sys.stderr)
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
