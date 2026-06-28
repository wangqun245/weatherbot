from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from featurize_metar_history import (  # noqa: E402
    BASE_FEATURE_COLUMNS,
    MetarRow,
    add_observation_context_features,
    blank,
    decode_metar,
    is_extra_metar_report,
    nearest_lag_value,
    read_rows,
    regular_observation_minutes_by_year,
)


DEFAULT_INPUT_DIR = (
    Path(__file__).resolve().parent
    / "data"
    / "all_airports_daily_high"
    / "processed"
)
DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parent
    / "data"
    / "all_airports_daily_high"
    / "features_lag10_speci_asos"
)
DEFAULT_COMBINED_NAME = (
    "all_stations_daily_high_features_lag10_speci_asos.csv"
)

STATION_TIMEZONES = {
    "KATL": "America/New_York",
    "KAUS": "America/Chicago",
    "KBOS": "America/New_York",
    "KDCA": "America/New_York",
    "KDFW": "America/Chicago",
    "KLAS": "America/Los_Angeles",
    "KMDW": "America/Chicago",
    "KMIA": "America/New_York",
    "KMSP": "America/Chicago",
    "KNYC": "America/New_York",
    "KOKC": "America/Chicago",
    "KPHL": "America/New_York",
    "KPHX": "America/Phoenix",
    "KSAT": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
}
STATION_IDS = {
    station: index
    for index, station in enumerate(sorted(STATION_TIMEZONES), start=1)
}

MAX_6H_RE = re.compile(r"(?:^|\s)1([01])(\d{3})(?=\s|$)")
MIN_6H_RE = re.compile(r"(?:^|\s)2([01])(\d{3})(?=\s|$)")

LATEST_ASOS_COLUMNS = [
    "asos_6h_max_temp_f",
    "asos_6h_min_temp_f",
    "asos_6h_temp_range_f",
    "asos_6h_extrema_age_minutes",
    "asos_6h_max_minus_current_temp_f",
    "current_temp_minus_asos_6h_min_f",
    "has_asos_6h_extrema_context",
]
PREVIOUS_ASOS_COLUMNS = [
    "asos_previous_6h_max_temp_f",
    "asos_previous_6h_min_temp_f",
    "asos_previous_6h_temp_range_f",
    "asos_previous_6h_extrema_age_minutes",
    "asos_previous_6h_max_minus_current_temp_f",
    "current_temp_minus_asos_previous_6h_min_f",
    "has_asos_previous_6h_extrema_context",
]
ASOS_COLUMNS = [*LATEST_ASOS_COLUMNS, *PREVIOUS_ASOS_COLUMNS]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Kalshi features from cleaned tagged METARs: standard "
            "observations as model rows, SPECI/COR as context, lag1-lag10 "
            "temperature changes, and two ASOS six-hour extrema contexts."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-lag-hours", type=int, default=10)
    parser.add_argument("--lag-tolerance-minutes", type=int, default=30)
    parser.add_argument("--context-hours", type=int, default=6)
    parser.add_argument("--asos-context-max-age-minutes", type=int, default=390)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def signed_tenths_f(sign: str, digits: str) -> float:
    value_c = int(digits) / 10.0
    if sign == "1":
        value_c *= -1
    return value_c * 9.0 / 5.0 + 32.0


def parse_six_hour_extrema(
    metar: str,
) -> tuple[float | None, float | None]:
    maximum = MAX_6H_RE.search(metar)
    minimum = MIN_6H_RE.search(metar)
    return (
        signed_tenths_f(maximum.group(1), maximum.group(2))
        if maximum
        else None,
        signed_tenths_f(minimum.group(1), minimum.group(2))
        if minimum
        else None,
    )


def add_asos_context(
    features: dict[str, object | None],
    row: MetarRow,
    history: deque[tuple[datetime, float, float]],
    max_age_minutes: int,
) -> None:
    maximum, minimum = parse_six_hour_extrema(row.metar)
    if maximum is not None and minimum is not None:
        history.append((row.valid_utc, maximum, minimum))

    current_temp = features.get("temp_f")
    contexts = [
        (
            history[-1] if history else None,
            "",
            max_age_minutes,
        ),
        (
            history[-2] if len(history) >= 2 else None,
            "previous_",
            max_age_minutes * 2,
        ),
    ]
    for context, prefix, allowed_age in contexts:
        has_column = f"has_asos_{prefix}6h_extrema_context"
        if context is None:
            features[has_column] = 0
            continue
        observed, high_f, low_f = context
        age = (row.valid_utc - observed).total_seconds() / 60.0
        if age < 0 or age > allowed_age:
            features[has_column] = 0
            continue
        features.update(
            {
                f"asos_{prefix}6h_max_temp_f": high_f,
                f"asos_{prefix}6h_min_temp_f": low_f,
                f"asos_{prefix}6h_temp_range_f": high_f - low_f,
                f"asos_{prefix}6h_extrema_age_minutes": age,
                f"asos_{prefix}6h_max_minus_current_temp_f": (
                    None
                    if current_temp is None
                    else high_f - float(current_temp)
                ),
                f"current_temp_minus_asos_{prefix}6h_min_f": (
                    None
                    if current_temp is None
                    else float(current_temp) - low_f
                ),
                has_column: 1,
            }
        )


def write_station(
    source: Path,
    output: Path,
    combined_writer: csv.DictWriter,
    feature_columns: list[str],
    fieldnames: list[str],
    args: argparse.Namespace,
) -> Counter:
    station = source.name.split("_", 1)[0].upper()
    if station not in STATION_TIMEZONES:
        raise ValueError(f"Unexpected or excluded station file: {source}")
    rows = read_rows(source)
    rows.sort(key=lambda row: row.valid_utc)
    timezone_local = ZoneInfo(STATION_TIMEZONES[station])
    decoded_rows = [
        decode_metar(row, station, timezone_local) for row in rows
    ]
    for decoded in decoded_rows:
        decoded["station_id"] = STATION_IDS[station]

    valid_times = [row.valid_utc for row in rows]
    temp_values = [decoded.get("temp_f") for decoded in decoded_rows]
    regular_minutes = regular_observation_minutes_by_year(rows)
    extra_flags = [
        is_extra_metar_report(row, regular_minutes) for row in rows
    ]
    lag_tolerance = timedelta(minutes=args.lag_tolerance_minutes)
    context_window = timedelta(hours=args.context_hours)
    extrema_history: deque[tuple[datetime, float, float]] = deque(maxlen=2)
    stats: Counter = Counter(
        input_rows=len(rows),
        extra_metar_context_rows=sum(extra_flags),
    )

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, lineterminator="\n"
        )
        writer.writeheader()
        for index, (row, features) in enumerate(
            zip(rows, decoded_rows)
        ):
            # Context must be updated before potentially skipping a SPECI/COR
            # row, because those reports remain observable history.
            add_asos_context(
                features,
                row,
                extrema_history,
                args.asos_context_max_age_minutes,
            )
            if extra_flags[index]:
                stats["extra_metar_input_rows_skipped"] += 1
                continue

            current_temp = features.get("temp_f")
            for hours in range(1, args.max_lag_hours + 1):
                lag = nearest_lag_value(
                    row.valid_utc - timedelta(hours=hours),
                    valid_times,
                    temp_values,
                    lag_tolerance,
                )
                features[f"temp_f_lag_{hours}h"] = lag
                features[f"temp_f_change_{hours}h"] = (
                    None
                    if current_temp is None or lag is None
                    else float(current_temp) - float(lag)
                )
                if lag is None:
                    stats[f"missing_temp_lag_{hours}h"] += 1

            add_observation_context_features(
                features=features,
                row=row,
                row_idx=index,
                valid_times=valid_times,
                temp_values=temp_values,
                extra_flags=extra_flags,
                window=context_window,
            )
            if features.get("has_extra_metar_past_6h"):
                stats["rows_with_speci_context"] += 1
            if features.get("has_asos_6h_extrema_context"):
                stats["rows_with_asos_context"] += 1
            if features.get("has_asos_previous_6h_extrema_context"):
                stats["rows_with_two_asos_contexts"] += 1

            output_row = {
                "daily_high_f": row.daily_high_f,
                "station": row.station,
                "valid": row.valid_text,
                "metar": row.metar,
            }
            output_row.update(
                {
                    column: blank(features.get(column))
                    for column in feature_columns
                }
            )
            writer.writerow(output_row)
            combined_writer.writerow(output_row)
            stats["output_rows"] += 1
    return stats


def main() -> int:
    args = parse_args()
    if args.max_lag_hours != 10:
        raise SystemExit("This Kalshi model requires --max-lag-hours 10")
    sources = sorted(
        path
        for path in args.input_dir.glob(
            "K*_local_0000_2359_daily_high.csv"
        )
        if path.name.split("_", 1)[0] in STATION_TIMEZONES
    )
    if len(sources) != len(STATION_TIMEZONES):
        found = {path.name.split("_", 1)[0] for path in sources}
        missing = sorted(set(STATION_TIMEZONES) - found)
        raise SystemExit(
            f"Expected 16 clean station files; missing: {', '.join(missing)}"
        )

    lag_columns = [
        column
        for hours in range(1, args.max_lag_hours + 1)
        for column in (
            f"temp_f_lag_{hours}h",
            f"temp_f_change_{hours}h",
        )
    ]
    feature_columns = [*BASE_FEATURE_COLUMNS, *lag_columns, *ASOS_COLUMNS]
    fieldnames = [
        "daily_high_f",
        "station",
        "valid",
        "metar",
        *feature_columns,
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    combined = args.output_dir / DEFAULT_COMBINED_NAME
    outputs = [
        args.output_dir / f"{source.stem}_features.csv"
        for source in sources
    ]
    existing = [path for path in [combined, *outputs] if path.exists()]
    if existing and not args.overwrite:
        raise SystemExit(
            "Feature outputs already exist; use --overwrite: "
            + ", ".join(map(str, existing[:3]))
        )

    summary: dict[str, object] = {}
    totals: Counter = Counter()
    with combined.open("w", encoding="utf-8", newline="") as combined_handle:
        combined_writer = csv.DictWriter(
            combined_handle, fieldnames=fieldnames, lineterminator="\n"
        )
        combined_writer.writeheader()
        for source, output in zip(sources, outputs):
            stats = write_station(
                source,
                output,
                combined_writer,
                feature_columns,
                fieldnames,
                args,
            )
            station = source.name.split("_", 1)[0]
            stats["output_file"] = str(output)
            summary[station] = dict(stats)
            totals.update(
                {
                    key: value
                    for key, value in stats.items()
                    if isinstance(value, int)
                }
            )
            print(
                f"{station}: wrote {stats['output_rows']:,} regular "
                f"observation feature rows"
            )

    summary["_totals"] = dict(totals)
    summary["_config"] = {
        "input_dir": str(args.input_dir),
        "combined_file": str(combined),
        "stations": sorted(STATION_TIMEZONES),
        "feature_count": len(feature_columns),
        "max_lag_hours": args.max_lag_hours,
        "lag_tolerance_minutes": args.lag_tolerance_minutes,
        "speci_context_hours": args.context_hours,
        "asos_contexts": 2,
        "asos_context_max_age_minutes": args.asos_context_max_age_minutes,
        "model_input_rows": "regular scheduled observations only",
        "context_rows": "regular plus SPECI/COR observations",
    }
    (args.output_dir / "feature_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Combined feature file: {combined}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
