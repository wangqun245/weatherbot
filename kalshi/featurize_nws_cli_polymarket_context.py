from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from featurize_metar_history import (  # noqa: E402
    BASE_FEATURE_COLUMNS,
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
    / "features_nws_cli_polymarket_context"
)
DEFAULT_COMBINED_NAME = (
    "all_stations_nws_cli_daily_high_polymarket_context.csv"
)

STATION_STANDARD_UTC_OFFSETS = {
    "KATL": -5,
    "KAUS": -6,
    "KBOS": -5,
    "KDCA": -5,
    "KDEN": -7,
    "KDFW": -6,
    "KHOU": -6,
    "KLAS": -8,
    "KLAX": -8,
    "KMDW": -6,
    "KMIA": -5,
    "KMSP": -6,
    "KOKC": -6,
    "KPHL": -5,
    "KPHX": -7,
    "KSAT": -6,
    "KSEA": -8,
    "KSFO": -8,
}
STATION_IDS = {
    station: index
    for index, station in enumerate(
        sorted(STATION_STANDARD_UTC_OFFSETS), start=1
    )
}

CONTEXT_HOURS = 10
MAX_LAG_HOURS = 10
CONTEXT_SUFFIX_SOURCE = "_past_6h"
CONTEXT_SUFFIX_TARGET = "_past_10h"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Kalshi NWS-CLI-label features with the same 10-hour "
            "METAR/SPECI context and feature dimensions as the online "
            "Polymarket model."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--lag-tolerance-minutes", type=int, default=30)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def polymarket_base_columns() -> list[str]:
    return [
        column.replace(CONTEXT_SUFFIX_SOURCE, CONTEXT_SUFFIX_TARGET)
        for column in BASE_FEATURE_COLUMNS
    ]


def feature_columns() -> list[str]:
    lag_values = [
        f"temp_f_lag_{hours}h"
        for hours in range(1, MAX_LAG_HOURS + 1)
    ]
    lag_changes = [
        f"temp_f_change_{hours}h"
        for hours in range(1, MAX_LAG_HOURS + 1)
    ]
    return [*polymarket_base_columns(), *lag_values, *lag_changes]


def rename_context_features(features: dict[str, object | None]) -> None:
    for name in list(features):
        if CONTEXT_SUFFIX_SOURCE not in name:
            continue
        renamed = name.replace(
            CONTEXT_SUFFIX_SOURCE, CONTEXT_SUFFIX_TARGET
        )
        features[renamed] = features.pop(name)


def write_station(
    source: Path,
    output: Path,
    combined_writer: csv.DictWriter,
    columns: list[str],
    fieldnames: list[str],
    lag_tolerance: timedelta,
) -> Counter:
    station = source.name.split("_", 1)[0].upper()
    rows = read_rows(source)
    rows.sort(key=lambda row: row.valid_utc)
    fixed_lst = timezone(
        timedelta(hours=STATION_STANDARD_UTC_OFFSETS[station])
    )
    decoded_rows = [
        decode_metar(row, station, fixed_lst) for row in rows
    ]
    for features in decoded_rows:
        features["station_id"] = STATION_IDS[station]

    valid_times = [row.valid_utc for row in rows]
    temp_values = [features.get("temp_f") for features in decoded_rows]
    regular_minutes = regular_observation_minutes_by_year(rows)
    extra_flags = [
        is_extra_metar_report(row, regular_minutes) for row in rows
    ]
    context_window = timedelta(hours=CONTEXT_HOURS)
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
            add_observation_context_features(
                features=features,
                row=row,
                row_idx=index,
                valid_times=valid_times,
                temp_values=temp_values,
                extra_flags=extra_flags,
                window=context_window,
            )
            rename_context_features(features)
            if extra_flags[index]:
                stats["extra_metar_input_rows_skipped"] += 1
                continue

            current_temp = features.get("temp_f")
            for hours in range(1, MAX_LAG_HOURS + 1):
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
                    else float(current_temp) - lag
                )

            output_row = {
                "daily_high_f": row.daily_high_f,
                "station": row.station,
                "valid": row.valid_text,
                "metar": row.metar,
            }
            output_row.update(
                {
                    column: blank(features.get(column))
                    for column in columns
                }
            )
            writer.writerow(output_row)
            combined_writer.writerow(output_row)
            stats["output_rows"] += 1
    return stats


def main() -> int:
    args = parse_args()
    sources = sorted(
        args.input_dir.glob("K*_local_0000_2359_daily_high.csv")
    )
    found = {source.name.split("_", 1)[0] for source in sources}
    expected = set(STATION_STANDARD_UTC_OFFSETS)
    if found != expected:
        raise SystemExit(
            f"Processed station mismatch; missing={sorted(expected - found)}, "
            f"unexpected={sorted(found - expected)}"
        )

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

    columns = feature_columns()
    fieldnames = [
        "daily_high_f",
        "station",
        "valid",
        "metar",
        *columns,
    ]
    summary: dict[str, object] = {}
    totals: Counter = Counter()
    lag_tolerance = timedelta(minutes=args.lag_tolerance_minutes)
    with combined.open(
        "w", encoding="utf-8", newline=""
    ) as combined_handle:
        combined_writer = csv.DictWriter(
            combined_handle, fieldnames=fieldnames, lineterminator="\n"
        )
        combined_writer.writeheader()
        for source, output in zip(sources, outputs):
            station = source.name.split("_", 1)[0]
            stats = write_station(
                source,
                output,
                combined_writer,
                columns,
                fieldnames,
                lag_tolerance,
            )
            stats["output_file"] = str(output)
            summary[station] = dict(stats)
            totals.update(
                {
                    key: value
                    for key, value in stats.items()
                    if isinstance(value, int)
                }
            )
            print(f"{station}: wrote {stats['output_rows']:,} rows")

    summary["_totals"] = dict(totals)
    summary["_config"] = {
        "input_dir": str(args.input_dir),
        "combined_file": str(combined),
        "stations": sorted(expected),
        "station_ids": STATION_IDS,
        "raw_feature_count": len(columns),
        "expected_trained_feature_count": 93,
        "max_lag_hours": MAX_LAG_HOURS,
        "context_hours": CONTEXT_HOURS,
        "lag_tolerance_minutes": args.lag_tolerance_minutes,
        "model_input_rows": "regular scheduled observations only",
        "context_rows": "regular plus SPECI/COR observations",
        "time_basis": "fixed Local Standard Time",
        "label": "NWS CLI YESTERDAY MAXIMUM",
    }
    (args.output_dir / "feature_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Combined feature file: {combined}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
