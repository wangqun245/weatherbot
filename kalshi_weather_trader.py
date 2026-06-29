from __future__ import annotations

import argparse
import atexit
import concurrent.futures
import copy
import json
import logging
import math
import os
import re
import threading
import time
import warnings
from collections import deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import joblib
import requests
from dotenv import load_dotenv

from featurize_metar_history import (
    MetarRow,
    add_observation_context_features,
    decode_metar,
    is_extra_metar_report,
    nearest_lag_value,
    regular_observation_minutes_by_year,
)
from kalshi.featurize_katt import parse_six_hour_extrema
from kalshi.featurize_all_stations import STATION_IDS
from kalshi_client import KalshiClient
from kalshi_execution import (
    KalshiHourlyExecutionManager,
    ManagedLeg,
    depth_price,
)
from kalshi_ws import KalshiWebSocketFeed
from weather_telegram_notifier import TelegramNotifier


DEFAULT_CONFIG = "kalshi_weather_config.json"
NWS_LST = timezone(timedelta(hours=-6), name="CST")
LOGGER = logging.getLogger("kalshi_weather_trader")
ORDER_EVENT_LOCK = threading.RLock()
EVENT_DATE_RE = re.compile(r"-(\d{2}[A-Z]{3}\d{2})(?:-|$)")


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = ("kalshi", "market", "observations", "model", "trading", "outputs")
    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError(f"Missing config sections: {', '.join(missing)}")
    return config


def setup_logging(config: dict[str, Any]) -> None:
    log_path = Path(config["outputs"]["log_file"])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def parse_obs_time(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        return datetime.fromtimestamp(float(text), tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def fetch_metars(
    config: dict[str, Any], hours: int | None = None
) -> list[dict[str, Any]]:
    observations = config["observations"]
    response = requests.get(
        observations["aviation_weather_url"],
        params={
            "ids": observations["station"],
            "hours": int(
                observations["lookback_hours"] if hours is None else hours
            ),
            "format": "json",
        },
        headers={"User-Agent": "weatherbot-kalshi/1.0"},
        timeout=float(config["kalshi"]["request_timeout_seconds"]),
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, list) else []


def fetch_metars_with_retry(
    config: dict[str, Any], hours: int, reason: str
) -> list[dict[str, Any]]:
    """Fetch AWC history at most three times using the configured retry delay."""
    observations = config["observations"]
    attempts = max(1, int(observations.get("awc_max_attempts", 3)))
    interval = max(
        0.0, float(observations.get("awc_retry_interval_seconds", 3))
    )
    for attempt in range(1, attempts + 1):
        try:
            rows = fetch_metars(config, hours)
            if rows:
                LOGGER.info(
                    "AWC METAR history ready station=%s hours=%s reason=%s rows=%s attempt=%s/%s",
                    observations["station"],
                    hours,
                    reason,
                    len(rows),
                    attempt,
                    attempts,
                )
                return rows
            LOGGER.warning(
                "AWC METAR history empty station=%s hours=%s reason=%s attempt=%s/%s",
                observations["station"],
                hours,
                reason,
                attempt,
                attempts,
            )
        except Exception as exc:
            LOGGER.warning(
                "AWC METAR history failed station=%s hours=%s reason=%s attempt=%s/%s error=%s",
                observations["station"],
                hours,
                reason,
                attempt,
                attempts,
                exc,
            )
        if attempt < attempts and interval:
            time.sleep(interval)
    return []


def parse_tgftp_metar(text: str) -> dict[str, Any] | None:
    """Parse the timestamp and raw METAR from a TGFTP station TXT response."""
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    try:
        obs_dt = datetime.strptime(lines[0], "%Y/%m/%d %H:%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return {"obs_dt": obs_dt, "raw_ob": lines[1]}


def fetch_tgftp_metar(config: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch the latest station METAR while bypassing intermediary caches."""
    observations = config["observations"]
    station = str(observations["station"]).upper()
    base_url = str(
        observations.get(
            "tgftp_station_url",
            "https://tgftp.nws.noaa.gov/data/observations/metar/stations/{station}.TXT",
        )
    ).format(station=station)
    separator = "&" if "?" in base_url else "?"
    response = requests.get(
        f"{base_url}{separator}nocache={int(time.time() * 1000)}",
        headers={
            "User-Agent": "weatherbot-kalshi/1.0",
            "Cache-Control": "no-cache, no-store, max-age=0",
            "Pragma": "no-cache",
        },
        timeout=max(
            0.5,
            float(observations.get("tgftp_request_timeout_seconds", 2)),
        ),
    )
    response.raise_for_status()
    return parse_tgftp_metar(response.text)


def merge_tgftp_metar(
    awc_rows: list[dict[str, Any]], observation: dict[str, Any]
) -> list[dict[str, Any]]:
    """Merge TGFTP's newest report into AWC history without duplicate timestamps."""
    obs_dt = observation.get("obs_dt")
    raw_ob = str(observation.get("raw_ob") or "").strip()
    merged = []
    for row in awc_rows:
        row_dt = parse_obs_time(
            row.get("obsTime")
            or row.get("reportTime")
            or row.get("receiptTime")
        )
        row_raw = str(
            row.get("rawOb") or row.get("raw") or row.get("metar") or ""
        ).strip()
        if row_dt == obs_dt or (raw_ob and row_raw == raw_ob):
            continue
        merged.append(row)
    merged.append(
        {
            "obsTime": (
                obs_dt.isoformat() if isinstance(obs_dt, datetime) else obs_dt
            ),
            "rawOb": raw_ob,
            "_source": "tgftp",
        }
    )
    return merged


class KalshiMetarCoordinator:
    """Prefetch AWC context, then poll only TGFTP until the hourly report arrives."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.windows: dict[str, dict[str, Any]] = {}

    def poll(self) -> list[dict[str, Any]] | None:
        observations = self.config["observations"]
        model = self.config["model"]
        local_timezone = configured_timezone(self.config)
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(local_timezone)
        hour = now_local.hour
        if not (
            int(model["buy_start_hour"])
            <= hour
            <= int(model["buy_end_hour"])
        ):
            return None

        minute = int(observations["regular_observation_minute"])
        expected_local = now_local.replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        expected_utc = expected_local.astimezone(timezone.utc)
        if now_utc < expected_utc:
            return None

        start_delay = max(
            0.0, float(observations.get("tgftp_start_delay_seconds", 60))
        )
        tgftp_start = expected_utc + timedelta(seconds=start_delay)
        timeout = max(
            1.0, float(observations.get("tgftp_poll_timeout_seconds", 300))
        )
        deadline = tgftp_start + timedelta(seconds=timeout)
        window_key = f"{now_local.date()}:hour_{hour:02d}"
        state = self.windows.get(window_key)
        if state is None:
            if now_utc > deadline:
                return None
            history_hours = max(
                1, int(observations.get("awc_prefetch_hours", 11))
            )
            state = {
                "awc_rows": fetch_metars_with_retry(
                    self.config,
                    history_hours,
                    "tgftp_prefetch",
                ),
                "next_tgftp_at": 0.0,
                "ready_rows": None,
                "next_delivery_at": 0.0,
                "fallback_done": False,
                "tgftp_attempts": 0,
            }
            self.windows[window_key] = state
            LOGGER.info(
                "METAR window initialized station=%s window=%s expected=%s tgftp_start=%s awc_rows=%s",
                observations["station"],
                window_key,
                expected_utc.isoformat(),
                tgftp_start.isoformat(),
                len(state["awc_rows"]),
            )

        now_mono = time.monotonic()
        ready_rows = state.get("ready_rows")
        if ready_rows is not None:
            if now_mono >= float(state.get("next_delivery_at", 0.0)):
                state["next_delivery_at"] = now_mono + max(
                    10.0,
                    float(
                        self.config["scheduler"].get("poll_seconds", 60)
                    ),
                )
                return list(ready_rows)
            return None

        if now_utc < tgftp_start:
            return None
        if now_utc <= deadline:
            if now_mono < float(state.get("next_tgftp_at", 0.0)):
                return None
            interval = max(
                0.5,
                float(observations.get("tgftp_poll_interval_seconds", 2)),
            )
            state["next_tgftp_at"] = now_mono + interval
            state["tgftp_attempts"] = int(state.get("tgftp_attempts", 0)) + 1
            try:
                observation = fetch_tgftp_metar(self.config)
            except Exception as exc:
                if state["tgftp_attempts"] == 1 or state["tgftp_attempts"] % 15 == 0:
                    LOGGER.warning(
                        "TGFTP METAR poll failed station=%s window=%s attempt=%s error=%s",
                        observations["station"],
                        window_key,
                        state["tgftp_attempts"],
                        exc,
                    )
                return None
            obs_dt = observation.get("obs_dt") if observation else None
            if not isinstance(obs_dt, datetime):
                return None
            obs_local = obs_dt.astimezone(local_timezone)
            if (
                obs_dt < expected_utc - timedelta(minutes=10)
                or obs_local.date() != now_local.date()
                or obs_local.hour != hour
                or obs_dt.minute != minute
            ):
                return None
            combined = merge_tgftp_metar(
                list(state.get("awc_rows") or []), observation
            )
            state["ready_rows"] = combined
            state["next_delivery_at"] = now_mono + max(
                10.0,
                float(self.config["scheduler"].get("poll_seconds", 60)),
            )
            LOGGER.info(
                "TGFTP METAR accepted station=%s window=%s observation=%s combined_rows=%s raw=%r",
                observations["station"],
                window_key,
                obs_dt.isoformat(),
                len(combined),
                observation.get("raw_ob"),
            )
            return combined

        if not state.get("fallback_done"):
            state["fallback_done"] = True
            fallback_hours = max(
                1, int(observations.get("awc_fallback_hours", 11))
            )
            rows = fetch_metars_with_retry(
                self.config,
                fallback_hours,
                "tgftp_timeout_fallback",
            )
            if rows:
                state["ready_rows"] = rows
                state["next_delivery_at"] = now_mono + max(
                    10.0,
                    float(self.config["scheduler"].get("poll_seconds", 60)),
                )
                return rows
        return None


def parse_metar_rows(
    source_rows: list[dict[str, Any]],
    station: str,
    target_date: date,
    local_timezone: Any = NWS_LST,
) -> list[MetarRow]:
    parsed: list[MetarRow] = []
    for source in source_rows:
        metar = str(
            source.get("rawOb") or source.get("raw") or source.get("metar") or ""
        ).strip()
        valid_utc = parse_obs_time(
            source.get("obsTime")
            or source.get("reportTime")
            or source.get("receiptTime")
        )
        if not metar or valid_utc is None:
            continue
        if valid_utc.astimezone(local_timezone).date() != target_date:
            continue
        parsed.append(
            MetarRow(
                daily_high_f="",
                station=station,
                valid_utc=valid_utc,
                valid_text=valid_utc.strftime("%Y-%m-%d %H:%M"),
                metar=metar,
            )
        )
    parsed.sort(key=lambda row: row.valid_utc)
    return parsed


def required_lag_hours(model: Any) -> int:
    values = []
    for name in getattr(model, "feature_name_", []):
        match = re.fullmatch(r"temp_f_lag_(\d+)h", str(name))
        if match:
            values.append(int(match.group(1)))
    return max(values, default=0)


def add_asos_extrema_context(
    features: dict[str, Any], rows: list[MetarRow], latest: MetarRow
) -> None:
    max_age_minutes = 390
    history: deque[tuple[datetime, float, float]] = deque(maxlen=2)
    for row in rows:
        if row.valid_utc > latest.valid_utc:
            break
        max_f, min_f = parse_six_hour_extrema(row.metar)
        if max_f is not None and min_f is not None:
            history.append((row.valid_utc, max_f, min_f))

    specs = [
        (
            history[-1] if history else None,
            "asos_6h",
            "current_temp_minus_asos_6h_min_f",
            "has_asos_6h_extrema_context",
            max_age_minutes,
        ),
        (
            history[-2] if len(history) >= 2 else None,
            "asos_previous_6h",
            "current_temp_minus_asos_previous_6h_min_f",
            "has_asos_previous_6h_extrema_context",
            max_age_minutes * 2,
        ),
    ]
    temp_f = features.get("temp_f")
    for context, prefix, current_minus_name, flag_name, allowed_age in specs:
        if context is None:
            features[flag_name] = 0
            continue
        extrema_time, max_f, min_f = context
        age_minutes = (latest.valid_utc - extrema_time).total_seconds() / 60.0
        if age_minutes < 0 or age_minutes > allowed_age:
            features[flag_name] = 0
            continue
        features.update(
            {
                f"{prefix}_max_temp_f": max_f,
                f"{prefix}_min_temp_f": min_f,
                f"{prefix}_temp_range_f": max_f - min_f,
                f"{prefix}_extrema_age_minutes": age_minutes,
                f"{prefix}_max_minus_current_temp_f": (
                    None if temp_f is None else max_f - float(temp_f)
                ),
                current_minus_name: (
                    None if temp_f is None else float(temp_f) - min_f
                ),
                flag_name: 1,
            }
        )


def build_feature_row(
    config: dict[str, Any],
    model: Any,
    source_rows: list[dict[str, Any]],
    target_date: date,
) -> tuple[dict[str, Any], MetarRow, datetime] | None:
    observations = config["observations"]
    station = str(observations["station"]).upper()
    local_timezone = configured_timezone(config)
    rows = parse_metar_rows(
        source_rows, station, target_date, local_timezone
    )
    if not rows:
        return None
    regular_minute = int(observations["regular_observation_minute"])
    regular = [
        row
        for row in rows
        if row.valid_utc.minute == regular_minute
        and " COR " not in f" {row.metar} "
    ]
    if not regular:
        return None
    latest = regular[-1]
    latest_local = latest.valid_utc.astimezone(local_timezone)
    if not (
        int(config["model"]["buy_start_hour"])
        <= latest_local.hour
        <= int(config["model"]["buy_end_hour"])
    ):
        return None

    decoded_rows = [
        decode_metar(row, station, local_timezone) for row in rows
    ]
    features = decode_metar(latest, station, local_timezone)
    if station not in STATION_IDS:
        raise ValueError(f"Station {station} is not present in the 16-station model")
    features["station_id"] = STATION_IDS[station]
    features["local_week_of_year"] = float(latest_local.isocalendar().week)
    valid_times = [row.valid_utc for row in rows]
    temp_values = [decoded.get("temp_f") for decoded in decoded_rows]
    tolerance = timedelta(minutes=int(observations["lag_tolerance_minutes"]))
    current_temp = features.get("temp_f")
    for hours in range(1, required_lag_hours(model) + 1):
        lag_value = nearest_lag_value(
            latest.valid_utc - timedelta(hours=hours),
            valid_times,
            temp_values,
            tolerance,
        )
        features[f"temp_f_lag_{hours}h"] = lag_value
        features[f"temp_f_change_{hours}h"] = (
            None
            if current_temp is None or lag_value is None
            else float(current_temp) - float(lag_value)
        )
    regular_minutes_by_year = regular_observation_minutes_by_year(rows)
    extra_flags = [
        is_extra_metar_report(row, regular_minutes_by_year) for row in rows
    ]
    latest_index = rows.index(latest)
    add_observation_context_features(
        features=features,
        row=latest,
        row_idx=latest_index,
        valid_times=valid_times,
        temp_values=temp_values,
        extra_flags=extra_flags,
        window=timedelta(hours=6),
    )
    add_asos_extrema_context(features, rows, latest)
    return features, latest, latest_local


def predict(model: Any, features: dict[str, Any]) -> float:
    row = []
    for name in model.feature_name_:
        value = features.get(name)
        try:
            row.append(float(value) if value not in (None, "") else float("nan"))
        except (TypeError, ValueError):
            row.append(float("nan"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return float(model.predict([row], num_iteration=model.best_iteration_)[0])


def event_date_from_ticker(event_ticker: str) -> date | None:
    match = EVENT_DATE_RE.search(event_ticker.upper())
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%y%b%d").date()
    except ValueError:
        return None


def market_contains_temperature(market: dict[str, Any], temperature_f: float) -> bool:
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")
    if floor is None and cap is not None:
        return temperature_f <= float(cap) - 1.0
    if floor is not None and cap is None:
        return temperature_f >= float(floor) + 1.0
    if floor is not None and cap is not None:
        return float(floor) <= temperature_f <= float(cap)
    return False


def outcome_ask(market: dict[str, Any], side: str) -> float:
    side = side.upper()
    try:
        depth_value = market.get(f"_{side.lower()}_depth_price")
        if depth_value not in (None, ""):
            return float(depth_value)
        direct = market.get(f"{side.lower()}_ask_dollars")
        if direct not in (None, ""):
            return float(direct)
        opposite_bid = market.get(
            "no_bid_dollars" if side == "YES" else "yes_bid_dollars"
        )
        return 1.0 - float(opposite_bid) if opposite_bid not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def hydrate_orderbook_prices(
    client: KalshiClient,
    markets: list[dict[str, Any]],
    trading: dict[str, Any],
) -> None:
    """Attach 1:1 depth prices for both outcomes to each market."""
    for market in markets:
        ticker = str(market.get("ticker") or "")
        if not ticker:
            continue
        try:
            book = client.get_orderbook(ticker, depth=100)
        except Exception:
            LOGGER.exception("Unable to load Kalshi orderbook ticker=%s", ticker)
            continue
        yes_bids = [
            (float(price), float(quantity))
            for price, quantity in (
                book.get("yes_dollars") or book.get("yes_dollars_fp") or []
            )
        ]
        no_bids = [
            (float(price), float(quantity))
            for price, quantity in (
                book.get("no_dollars") or book.get("no_dollars_fp") or []
            )
        ]
        yes_asks = sorted(
            [(round(1.0 - price, 4), quantity) for price, quantity in no_bids]
        )
        no_asks = sorted(
            [(round(1.0 - price, 4), quantity) for price, quantity in yes_bids]
        )
        for side, levels in (("yes", yes_asks), ("no", no_asks)):
            best = levels[0][0] if levels else None
            target_shares = (
                contract_count_for_order(best, trading)
                if best is not None
                else max(1, int(trading.get("default_contracts", 10)))
            )
            full_depth = depth_price(levels, target_shares, 1.0)
            market[f"_{side}_depth_price"] = (
                full_depth if full_depth is not None else best
            )
            market[f"_{side}_has_target_depth"] = full_depth is not None
            market[f"_{side}_target_shares"] = target_shares
            market[f"_{side}_buy_levels"] = levels


def market_bounds(market: dict[str, Any]) -> tuple[float | None, float | None]:
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")
    if floor is None and cap is not None:
        return None, float(cap) - 1.0
    if floor is not None and cap is None:
        return float(floor) + 1.0, None
    return (
        None if floor is None else float(floor),
        None if cap is None else float(cap),
    )


def prediction_matches(
    markets: list[dict[str, Any]], prediction_f: float
) -> list[dict[str, Any]]:
    return [
        market
        for market in markets
        if market_contains_temperature(market, prediction_f)
    ]


def boundary_snap_market(
    markets: list[dict[str, Any]], prediction_f: float, tolerance_f: float
) -> tuple[dict[str, Any], float] | None:
    candidates: list[tuple[float, dict[str, Any]]] = []
    for market in markets:
        lower, upper = market_bounds(market)
        if lower is not None and prediction_f < lower:
            candidates.append((lower - prediction_f, market))
        if upper is not None and prediction_f > upper:
            candidates.append((prediction_f - upper, market))
    eligible = [
        candidate for candidate in candidates if candidate[0] <= tolerance_f + 1e-9
    ]
    if not eligible:
        return None
    distance, market = min(
        eligible, key=lambda item: (item[0], str(item[1].get("ticker")))
    )
    return market, distance


def adjacent_prediction_markets(
    markets: list[dict[str, Any]], prediction_f: float
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    lower_candidates: list[tuple[float, dict[str, Any]]] = []
    upper_candidates: list[tuple[float, dict[str, Any]]] = []
    for market in markets:
        lower, upper = market_bounds(market)
        if upper is not None and upper < prediction_f:
            lower_candidates.append((upper, market))
        if lower is not None and lower > prediction_f:
            upper_candidates.append((lower, market))
    if not lower_candidates or not upper_candidates:
        return None
    lower_market = max(lower_candidates, key=lambda item: item[0])[1]
    upper_market = min(upper_candidates, key=lambda item: item[0])[1]
    return lower_market, upper_market


def select_order_plan(
    config: dict[str, Any],
    markets: list[dict[str, Any]],
    prediction_f: float,
) -> tuple[list[dict[str, Any]], str]:
    """Mirror the prior Polymarket interval/YES/NO selection policy."""
    trading = config["trading"]
    matches = prediction_matches(markets, prediction_f)
    if len(matches) == 1:
        predicted_market = matches[0]
        candidates = []
        for market in markets:
            side = "YES" if market["ticker"] == predicted_market["ticker"] else "NO"
            price = outcome_ask(market, side)
            if price > 0:
                candidates.append({"market": market, "side": side, "price": price})
        if not candidates:
            return [], "exact_no_priced_candidates"
        selected = min(
            candidates,
            key=lambda item: (
                float(item["price"]),
                str(item["market"]["ticker"]),
                str(item["side"]),
            ),
        )
        gross_profit = 1.0 - float(selected["price"])
        return [selected], (
            f"exact_interval_{predicted_market['ticker']}_selected_"
            f"{selected['side']}_{selected['market']['ticker']}_"
            f"gross_profit_{gross_profit:.4f}"
        )

    tolerance = float(trading.get("interval_snap_tolerance_f", 0.15))
    snapped = boundary_snap_market(markets, prediction_f, tolerance)
    if snapped is not None:
        predicted_market, distance = snapped
        candidates = []
        for market in markets:
            side = (
                "YES"
                if market["ticker"] == predicted_market["ticker"]
                else "NO"
            )
            price = outcome_ask(market, side)
            if price > 0:
                candidates.append(
                    {"market": market, "side": side, "price": price}
                )
        if not candidates:
            return [], "boundary_snap_no_priced_candidates"
        selected = min(
            candidates,
            key=lambda item: (
                float(item["price"]),
                str(item["market"]["ticker"]),
                str(item["side"]),
            ),
        )
        return [selected], (
            f"boundary_snap_interval_{predicted_market['ticker']}_distance_"
            f"{distance:.4f}F_tolerance_{tolerance:.4f}F_selected_"
            f"{selected['side']}_{selected['market']['ticker']}"
        )

    adjacent = adjacent_prediction_markets(markets, prediction_f)
    if adjacent is None:
        return [], "no_exact_snap_or_adjacent_intervals"
    adjacent_orders = [
        {"market": market, "side": "YES", "price": outcome_ask(market, "YES")}
        for market in adjacent
    ]
    if any(float(order["price"]) <= 0 for order in adjacent_orders):
        return [], "adjacent_missing_yes_price"
    total_yes_price = sum(float(order["price"]) for order in adjacent_orders)

    adjacent_tickers = {market["ticker"] for market in adjacent}
    no_candidates = [
        {
            "market": market,
            "side": "NO",
            "price": outcome_ask(market, "NO"),
        }
        for market in markets
        if market["ticker"] not in adjacent_tickers
    ]
    no_candidates = [
        candidate for candidate in no_candidates if float(candidate["price"]) > 0
    ]
    cheapest_no = (
        min(
            no_candidates,
            key=lambda item: (float(item["price"]), str(item["market"]["ticker"])),
        )
        if no_candidates
        else None
    )
    max_total = float(trading.get("adjacent_yes_max_total_price", 0.90))
    if (
        total_yes_price < max_total
        and (
            cheapest_no is None
            or total_yes_price <= float(cheapest_no["price"])
        )
    ):
        gross_profit = 1.0 - total_yes_price
        return adjacent_orders, (
            f"adjacent_yes_pair_{adjacent[0]['ticker']}_{adjacent[1]['ticker']}_"
            f"total_{total_yes_price:.4f}_gross_profit_{gross_profit:.4f}"
        )
    if cheapest_no is not None:
        gross_profit = 1.0 - float(cheapest_no["price"])
        return [cheapest_no], (
            f"non_adjacent_no_{cheapest_no['market']['ticker']}_"
            f"cheaper_than_adjacent_yes_{total_yes_price:.4f}_"
            f"gross_profit_{gross_profit:.4f}"
        )
    return [], f"adjacent_yes_total_{total_yes_price:.4f}_without_no_alternative"


def contract_count_for_order(price: float, trading: dict[str, Any]) -> int:
    """Request the configured share count without exceeding per-order notional."""
    requested = max(1, int(trading.get("default_contracts", 10)))
    max_cost = max(0.01, float(trading.get("max_order_cost_dollars", 5.0)))
    affordable = int(math.floor((max_cost + 1e-9) / price)) if price > 0 else 0
    return max(0, min(requested, affordable))


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"completed_windows": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {"completed_windows": {}}
    except (OSError, json.JSONDecodeError):
        return {"completed_windows": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def append_trade(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def configured_timezone(config: dict[str, Any]) -> Any:
    value = str(
        config.get("observations", {}).get(
            "timezone", "America/Chicago"
        )
    )
    if value.lower() == "fixed_cst":
        return NWS_LST
    return ZoneInfo(value)


def configured_city_configs(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand the shared configuration into one isolated config per station."""
    cities = config.get("cities")
    if not isinstance(cities, list) or not cities:
        return [config]
    live_stations = {
        str(value).upper()
        for value in config.get("trading", {}).get("live_stations", [])
    }
    expanded = []
    for city in cities:
        if not isinstance(city, dict) or not city.get("station"):
            continue
        item = copy.deepcopy(config)
        item["city"] = dict(city)
        item["observations"].update(
            {
                "station": str(city["station"]).upper(),
                "timezone": city["timezone"],
                "regular_observation_minute": int(
                    city["regular_observation_minute"]
                ),
            }
        )
        item["kalshi"]["series_ticker"] = city["series_ticker"]
        item["market"]["expected_rules_text"] = str(
            city.get("expected_rules_text") or ""
        )
        station = item["observations"]["station"]
        is_live = station in live_stations
        item["trading"]["live_enabled"] = is_live
        item["trading"]["dry_run"] = not is_live
        expanded.append(item)
    return expanded


def notify_kalshi_trade(
    notifier: TelegramNotifier | None, record: dict[str, Any]
) -> None:
    """Send a platform-labelled Telegram notification for a Kalshi decision."""
    if notifier is None:
        return
    mode = "PAPER" if record.get("dry_run") else "LIVE"
    orders = record.get("orders") or []
    order_text = ", ".join(
        f"{order.get('outcome_side') or order.get('side')} "
        f"{order.get('contracts') or record.get('target_shares_each', '')}x "
        f"{order.get('market_ticker') or order.get('ticker', '')}"
        for order in orders
    )
    notifier.send(
        f"*Kalshi {mode} TRADE*\n"
        f"Station: *{record.get('station', '')}*\n"
        f"Prediction: *{float(record.get('prediction_f', 0.0)):.3f}F*\n"
        f"Orders: {order_text or record.get('selection_reason', '')}"
    )


def target_date(config: dict[str, Any]) -> date:
    configured = str(config["market"].get("target_date", "today"))
    if configured.lower() == "today":
        return datetime.now(configured_timezone(config)).date()
    return date.fromisoformat(configured)


def make_client(config: dict[str, Any]) -> KalshiClient:
    section = config["kalshi"]
    return KalshiClient(
        base_url=section["base_url"],
        api_key_id=os.getenv(section["api_key_id_env"], ""),
        private_key_path=os.getenv(section["private_key_path_env"], ""),
        timeout_seconds=float(section["request_timeout_seconds"]),
    )


def verify_series(config: dict[str, Any], client: KalshiClient) -> None:
    series = client.get_series(config["kalshi"]["series_ticker"])
    sources = series.get("settlement_sources") or []
    LOGGER.info(
        "Kalshi series=%s title=%s settlement_sources=%s",
        series.get("ticker"),
        series.get("title"),
        sources,
    )


def run_cycle(
    config: dict[str, Any],
    client: KalshiClient,
    model: Any,
    execution_manager: KalshiHourlyExecutionManager | None = None,
    source_rows: list[dict[str, Any]] | None = None,
    notifier: TelegramNotifier | None = None,
) -> None:
    day = target_date(config)
    markets = [
        market
        for market in client.get_open_markets(config["kalshi"]["series_ticker"])
        if event_date_from_ticker(str(market.get("event_ticker") or "")) == day
    ]
    if not markets:
        LOGGER.info("No open Kalshi markets for %s", day)
        return
    hydrate_orderbook_prices(
        client,
        markets,
        config["trading"],
    )

    expected_rules = str(config["market"].get("expected_rules_text") or "")
    mismatch = expected_rules and not any(
        expected_rules.lower() in str(market.get("rules_primary") or "").lower()
        for market in markets
    )
    if mismatch:
        message = f"Kalshi rules do not contain expected text {expected_rules!r}"
        if not config["market"].get("allow_source_station_mismatch", False):
            raise RuntimeError(message)
        LOGGER.warning("%s; continuing with configured station=%s", message, config["observations"]["station"])

    built = build_feature_row(
        config,
        model,
        fetch_metars(config) if source_rows is None else source_rows,
        day,
    )
    if built is None:
        LOGGER.info("No eligible KATT observation window for %s", day)
        return
    features, latest, latest_local = built
    prediction = predict(model, features)
    predicted_integer = int(math.floor(prediction + 0.5))
    trading = config["trading"]
    order_plan, selection_reason = select_order_plan(config, markets, prediction)
    LOGGER.info(
        "signal date=%s observation=%s prediction=%.3f rounded=%d plan=%s reason=%s",
        day,
        latest_local.isoformat(),
        prediction,
        predicted_integer,
        [
            {
                "ticker": order["market"]["ticker"],
                "side": order["side"],
                "price": order["price"],
            }
            for order in order_plan
        ],
        selection_reason,
    )
    if not order_plan:
        LOGGER.info("Skip order: no valid order plan")
        return
    state_path = Path(config["outputs"]["state_json"])
    state = load_state(state_path)
    station = str(config["observations"]["station"]).upper()
    window_key = f"{station}:{day}:hour_{latest_local.hour:02d}"
    if trading.get("one_order_per_hour", True) and (
        window_key in state.get("completed_windows", {})
        or (
            execution_manager is not None
            and execution_manager.has_window(window_key)
        )
    ):
        LOGGER.info("Skip duplicate window %s", window_key)
        return

    dry_run = bool(trading.get("dry_run", True)) or not bool(
        trading.get("live_enabled", False)
    )
    if execution_manager is not None and not dry_run:
        if len(order_plan) == 2:
            requested = min(
                contract_count_for_order(float(order["price"]), trading)
                for order in order_plan
            )
            mode = "adjacent"
        else:
            requested = contract_count_for_order(
                float(order_plan[0]["price"]), trading
            )
            mode = "single"
        if requested <= 0:
            LOGGER.info(
                "Skip managed batch: no affordable contracts plan=%s",
                order_plan,
            )
            return
        batch = execution_manager.start_batch(
            window_key=window_key,
            mode=mode,
            legs=tuple(
                ManagedLeg(
                    ticker=str(order["market"]["ticker"]),
                    outcome_side=str(order["side"]).upper(),
                )
                for order in order_plan
            ),
            target_shares=requested,
            predicted_high_f=prediction,
        )
        record = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "window_key": window_key,
            "target_date": day.isoformat(),
            "station": config["observations"]["station"],
            "observation_utc": latest.valid_utc.isoformat(),
            "observation_local": latest_local.isoformat(),
            "prediction_f": prediction,
            "predicted_integer_f": predicted_integer,
            "selection_reason": selection_reason,
            "execution_mode": "MANAGED_WEBSOCKET",
            "batch_id": batch.batch_id,
            "target_shares_each": requested,
            "orders": [
                {
                    "ticker": leg.ticker,
                    "outcome_side": leg.outcome_side,
                }
                for leg in batch.legs
            ],
        }
        append_trade(Path(config["outputs"]["trades_jsonl"]), record)
        state.setdefault("completed_windows", {})[window_key] = record
        save_state(state_path, state)
        notify_kalshi_trade(notifier, record)
        return

    max_buy_price = float(trading.get("max_buy_price", 0.85))
    min_buy_price = float(trading.get("min_buy_price", 0.01))
    invalid = [
        order
        for order in order_plan
        if not min_buy_price <= float(order["price"]) <= max_buy_price
    ]
    if invalid:
        LOGGER.info(
            "Skip order plan: price outside %.4f..%.4f invalid=%s",
            min_buy_price,
            max_buy_price,
            [
                {
                    "ticker": order["market"]["ticker"],
                    "side": order["side"],
                    "price": order["price"],
                }
                for order in invalid
            ],
        )
        return

    order_results: list[dict[str, Any]] = []
    for planned in order_plan:
        market = planned["market"]
        side = str(planned["side"]).upper()
        price = float(planned["price"])
        contracts = contract_count_for_order(price, trading)
        if contracts <= 0:
            LOGGER.info(
                "Skip order: one contract exceeds max_order_cost_dollars ticker=%s side=%s price=%.4f cap=%.2f",
                market["ticker"],
                side,
                price,
                float(trading["max_order_cost_dollars"]),
            )
            return
        order_cost = round(contracts * price, 4)
        if dry_run:
            order_result: dict[str, Any] = {
                "dry_run": True,
                "order": {
                    "ticker": market["ticker"],
                    "outcome_side": side.lower(),
                    "book_side": "bid" if side == "YES" else "ask",
                    "count": contracts,
                    "outcome_price": price,
                    "yes_scale_price": price if side == "YES" else 1.0 - price,
                    "order_cost_dollars": order_cost,
                },
            }
        else:
            order_result = client.create_order(
                market["ticker"],
                side.lower(),
                contracts,
                price,
                time_in_force=str(trading["time_in_force"]),
                subaccount=int(config["kalshi"].get("subaccount", 0)),
            )
        order_results.append(
            {
                "market_ticker": market["ticker"],
                "outcome_side": side,
                "outcome_price": price,
                "contracts": contracts,
                "order_cost_dollars": order_cost,
                "result": order_result,
            }
        )

    record = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "window_key": window_key,
        "target_date": day.isoformat(),
        "station": config["observations"]["station"],
        "observation_utc": latest.valid_utc.isoformat(),
        "observation_local": latest_local.isoformat(),
        "prediction_f": prediction,
        "predicted_integer_f": predicted_integer,
        "selection_reason": selection_reason,
        "default_contracts": int(trading["default_contracts"]),
        "max_order_cost_dollars": float(trading["max_order_cost_dollars"]),
        "dry_run": dry_run,
        "orders": order_results,
    }
    append_trade(Path(config["outputs"]["trades_jsonl"]), record)
    state.setdefault("completed_windows", {})[window_key] = record
    save_state(state_path, state)
    notify_kalshi_trade(notifier, record)
    LOGGER.info("Order recorded window=%s dry_run=%s", window_key, dry_run)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Kalshi Austin weather model trader")
    parser.add_argument("command", choices=("run", "once", "status"))
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()
    config = load_config(args.config)
    setup_logging(config)
    client = make_client(config)
    model = joblib.load(config["model"]["path"])
    city_configs = configured_city_configs(config)
    notifier = TelegramNotifier()
    LOGGER.info(
        "Loaded model=%s features=%d stations=%s live_stations=%s",
        config["model"]["path"],
        len(model.feature_name_),
        [item["observations"]["station"] for item in city_configs],
        config["trading"].get("live_stations", []),
    )
    live = any(
        bool(item["trading"].get("live_enabled", False))
        and not bool(item["trading"].get("dry_run", True))
        for item in city_configs
    )
    if live:
        balance = client.validate_credentials()
        LOGGER.info("Kalshi production credentials validated balance=%s", balance)
    for item in city_configs:
        verify_series(item, client)
    if args.command == "status":
        LOGGER.info(
            "Open markets=%d target_date=%s",
            sum(
                len(client.get_open_markets(item["kalshi"]["series_ticker"]))
                for item in city_configs
            ),
            target_date(city_configs[0]),
        )
        if os.getenv(config["kalshi"]["api_key_id_env"]):
            LOGGER.info("Balance=%s", client.get_balance())
        return
    execution_manager: KalshiHourlyExecutionManager | None = None
    if live:
        holder: dict[str, KalshiHourlyExecutionManager] = {}

        def websocket_callback(message: dict[str, Any]) -> None:
            manager = holder.get("manager")
            if manager is not None:
                manager.on_websocket_message(message)

        def execution_event(event: dict[str, Any]) -> None:
            event = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                **event,
            }
            with ORDER_EVENT_LOCK:
                append_trade(
                    Path(
                        config["outputs"].get(
                            "order_events_jsonl",
                            "kalshi/runtime/order_events.jsonl",
                        )
                    ),
                    event,
                )

        feed = KalshiWebSocketFeed(
            client=client,
            url=str(
                config["kalshi"].get(
                    "websocket_url",
                    "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
                )
            ),
            on_message=websocket_callback,
            reconnect_seconds=float(
                config["kalshi"].get(
                    "websocket_reconnect_seconds", 2
                )
            ),
        )
        execution_manager = KalshiHourlyExecutionManager(
            client=client,
            feed=feed,
            trading=config["trading"],
            subaccount=int(config["kalshi"].get("subaccount", 0)),
            event_callback=execution_event,
            state_path=Path(
                config["outputs"].get(
                    "managed_batches_json",
                    "kalshi/runtime/managed_batches.json",
                )
            ),
        )
        holder["manager"] = execution_manager
        execution_manager.start()
        atexit.register(execution_manager.stop)
    if args.command == "once":
        for item in city_configs:
            run_cycle(
                item,
                client,
                model,
                execution_manager,
                notifier=notifier,
            )
        while (
            execution_manager is not None
            and execution_manager.active_batch_count() > 0
        ):
            time.sleep(1)
        return
    coordinators = {
        item["observations"]["station"]: KalshiMetarCoordinator(item)
        for item in city_configs
    }
    configs_by_station = {
        item["observations"]["station"]: item for item in city_configs
    }
    metar_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=len(coordinators),
        thread_name_prefix="kalshi-metar",
    )
    metar_futures: dict[str, concurrent.futures.Future[Any]] = {}
    while True:
        try:
            for station, coordinator in coordinators.items():
                future = metar_futures.get(station)
                if future is None:
                    metar_futures[station] = metar_executor.submit(
                        coordinator.poll
                    )
                    continue
                if not future.done():
                    continue
                metar_futures.pop(station, None)
                source_rows = future.result()
                if source_rows is not None:
                    run_cycle(
                        configs_by_station[station],
                        client,
                        model,
                        execution_manager,
                        source_rows=source_rows,
                        notifier=notifier,
                    )
        except Exception:
            LOGGER.exception("Kalshi trading cycle failed")
        time.sleep(
            max(
                0.1,
                float(
                    config["scheduler"].get(
                        "observation_loop_sleep_seconds", 0.5
                    )
                ),
            )
        )


if __name__ == "__main__":
    main()
