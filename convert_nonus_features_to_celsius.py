from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
STAGING_DIR = ROOT / "NONUS" / "newC" / "_staging_features_f_schema"
OUTPUT_DIR = ROOT / "NONUS" / "newC" / "features_halfhour"
MAX_LAG_HOURS = 10

ABSOLUTE_F_EXACT = {
    "temp_f",
    "dewpoint_f",
    "heat_index_f",
    "wind_chill_f",
    "previous_local_day_high_f",
    "previous_local_day_low_f",
    "current_local_day_min_temp_f_so_far",
}


def is_temperature_difference(name: str) -> bool:
    return (
        name.startswith("temp_f_change_")
        or "temp_range_f" in name
        or name.startswith("temp_f_change_from_")
    )


def is_absolute_f_temperature(name: str) -> bool:
    return (
        name in ABSOLUTE_F_EXACT
        or name.startswith("temp_f_lag_")
        or "_temp_f_" in name
    )


def celsius_name(name: str) -> str:
    if name == "daily_high_f":
        return "daily_high_c"
    if name == "temp_f":
        return "temp_c_equivalent"
    if name == "dewpoint_f":
        return "dewpoint_c_equivalent"
    return (
        name.replace("temp_f", "temp_c")
        .replace("heat_index_f", "heat_index_c")
        .replace("wind_chill_f", "wind_chill_c")
        .replace("high_f", "high_c")
        .replace("low_f", "low_c")
        .replace("range_f", "range_c")
    )


def parse_number(text: str) -> float | None:
    if text in ("", "NaN", "nan", "NAN", None):
        return None
    try:
        value = float(text)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(value) else value


def format_celsius(value: float | None) -> str:
    return "NaN" if value is None else f"{value:.1f}"


def transform_value(name: str, text: str) -> str:
    value = parse_number(text)
    if name == "daily_high_f":
        return format_celsius(value)
    if name in {"temp_c", "dewpoint_c", "temp_dewpoint_spread_c"}:
        return format_celsius(value)
    if is_temperature_difference(name):
        return format_celsius(None if value is None else value * 5.0 / 9.0)
    if is_absolute_f_temperature(name):
        return format_celsius(
            None if value is None else (value - 32.0) * 5.0 / 9.0
        )
    return text


def convert_file(source: Path, destination: Path) -> int:
    rows = 0
    with (
        source.open("r", encoding="utf-8", newline="") as input_handle,
        destination.open("w", encoding="utf-8", newline="") as output_handle,
    ):
        reader = csv.reader(input_handle)
        writer = csv.writer(output_handle, lineterminator="\n")
        source_fields = next(reader)
        output_fields = [celsius_name(name) for name in source_fields]
        if len(output_fields) != len(set(output_fields)):
            raise RuntimeError(f"Duplicate output columns in {source.name}")
        writer.writerow(output_fields)
        for source_row in reader:
            writer.writerow(
                [
                    transform_value(name, value)
                    for name, value in zip(source_fields, source_row)
                ]
            )
            rows += 1
    return rows


def main() -> int:
    if OUTPUT_DIR.exists():
        raise SystemExit(f"Output directory already exists: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True)
    outputs = {}
    for source in sorted(STAGING_DIR.glob("*.csv")):
        if source.name.startswith("all_nonus_"):
            destination_name = "all_nonus_halfhour_numeric_features_c.csv"
        else:
            destination_name = source.name.replace(
                "_daily_high_c_features.csv",
                "_daily_high_c_features.csv",
            )
        destination = OUTPUT_DIR / destination_name
        row_count = convert_file(source, destination)
        outputs[source.name] = {
            "output_file": str(destination),
            "rows": row_count,
        }
        print(f"{source.name}: {row_count:,} rows")

    staging_summary = json.loads(
        (STAGING_DIR / "feature_summary.json").read_text(encoding="utf-8")
    )
    summary = {
        "target": "daily_high_c",
        "temperature_unit": "degrees Celsius",
        "temperature_precision_decimals": 1,
        "same_column_count_as_fahrenheit_schema": True,
        "absolute_temperature_conversion": "(F - 32) * 5 / 9",
        "temperature_difference_conversion": "F difference * 5 / 9",
        "max_lag_hours": MAX_LAG_HOURS,
        "half_hour_change_features": [
            f"temp_c_change_{hour}_5h" for hour in range(MAX_LAG_HOURS)
        ],
        "source_feature_summary": staging_summary,
        "outputs": outputs,
    }
    (OUTPUT_DIR / "feature_summary_celsius.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
