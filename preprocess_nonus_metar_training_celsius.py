from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_metar_twc_all_stations import STATION_LOCATIONS, parse_metar_temp_c
from preprocess_nonus_metar_training import load_station_rows


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "NONUS" / "newC"
COVERAGE_FILE = (
    ROOT
    / "outputs"
    / "metar_hourly_coverage"
    / "station_year_hourly_coverage.csv"
)
EXCLUDED_STATIONS = {
    "RCSS", "LIMC", "VILK", "OPKC", "EHAM",
    "NZWN", "EGLC", "LFPB",
}
STATIONS = {
    "RJTT", "RKSI", "ZSPD", "LEMD", "WSSS", "LTAC", "RKPK", "EDDM",
    "WMKK", "EFHK", "EPWA",
}
MIN_COMPLETE_DAY_RATIO = 0.95


def load_qualified_years() -> dict[str, set[int]]:
    qualified: dict[str, set[int]] = defaultdict(set)
    with COVERAGE_FILE.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station = row["station"].upper()
            year = int(row["year"])
            ratio = float(row["complete_day_ratio"])
            if (
                station in STATIONS
                and station not in EXCLUDED_STATIONS
                and ratio >= MIN_COMPLETE_DAY_RATIO
            ):
                qualified[station].add(year)
    return qualified


def main() -> int:
    if not COVERAGE_FILE.exists():
        raise SystemExit(f"Coverage file does not exist: {COVERAGE_FILE}")
    if OUTPUT_DIR.exists():
        raise SystemExit(
            f"Output directory already exists; refusing to overwrite: {OUTPUT_DIR}"
        )
    OUTPUT_DIR.mkdir(parents=True)
    qualified_years = load_qualified_years()
    summary = {}
    total_rows = 0
    fieldnames = [
        "daily_high_c",
        "station",
        "valid",
        "valid_local",
        "metar",
    ]
    combined_path = OUTPUT_DIR / "all_nonus_local_full_day_daily_high_c.csv"

    with combined_path.open("w", encoding="utf-8", newline="") as combined:
        combined_writer = csv.DictWriter(
            combined, fieldnames=fieldnames, lineterminator="\n"
        )
        combined_writer.writeheader()

        for station in sorted(STATIONS):
            timezone_name, _country = STATION_LOCATIONS[station]
            tz = ZoneInfo(timezone_name)
            years = qualified_years.get(station, set())
            stats = Counter()
            selected = [
                row
                for row in load_station_rows(station, tz)
                if row["local_year"] in years
            ]
            non_auto = []
            daily_highs_c: dict[str, float] = {}

            for row in selected:
                stats["selected_input_rows"] += 1
                if row["is_auto"]:
                    stats["auto_rows_excluded"] += 1
                    continue
                non_auto.append(row)
                temp_c = parse_metar_temp_c(row["metar"], station)
                if temp_c is None:
                    stats["rows_without_decodable_temperature"] += 1
                    continue
                if row["is_speci"]:
                    stats["decoded_explicit_speci_temperatures"] += 1
                else:
                    stats["decoded_regular_or_unmarked_speci_temperatures"] += 1
                previous = daily_highs_c.get(row["local_date"])
                if previous is None or temp_c > previous:
                    daily_highs_c[row["local_date"]] = temp_c

            station_path = (
                OUTPUT_DIR / f"{station}_local_full_day_daily_high_c.csv"
            )
            with station_path.open(
                "w", encoding="utf-8", newline=""
            ) as station_file:
                writer = csv.DictWriter(
                    station_file, fieldnames=fieldnames, lineterminator="\n"
                )
                writer.writeheader()
                for row in sorted(non_auto, key=lambda item: item["valid_utc"]):
                    high_c = daily_highs_c.get(row["local_date"])
                    if high_c is None:
                        stats["rows_without_daily_high_excluded"] += 1
                        continue
                    output = {
                        "daily_high_c": f"{high_c:.1f}",
                        "station": station,
                        "valid": row["valid_utc"].strftime("%Y-%m-%d %H:%M"),
                        "valid_local": row["valid_local"].isoformat(
                            timespec="minutes"
                        ),
                        "metar": row["metar"],
                    }
                    writer.writerow(output)
                    combined_writer.writerow(output)
                    stats["output_rows"] += 1
                    total_rows += 1

            stats["qualified_year_count"] = len(years)
            stats["local_days_with_daily_high"] = len(daily_highs_c)
            summary[station] = {
                **dict(stats),
                "timezone": timezone_name,
                "qualified_years": sorted(years),
                "output_file": str(station_path),
            }
            print(
                f"{station}: {len(years)} years, "
                f"{stats['local_days_with_daily_high']} days, "
                f"{stats['output_rows']} rows"
            )

    summary["_metadata"] = {
        "target_column": "daily_high_c",
        "target_unit": "degrees Celsius",
        "target_precision": "0.1 C",
        "minimum_complete_day_ratio": MIN_COMPLETE_DAY_RATIO,
        "excluded_stations": sorted(EXCLUDED_STATIONS),
        "auto_excluded_from_labels_and_rows": True,
        "speci_included_in_daily_high": True,
        "temperature_priority": "RMK T group, then main temperature group",
        "current_partial_year_included_when_qualified": True,
        "combined_output_file": str(combined_path),
        "combined_output_rows": total_rows,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    summary_path = OUTPUT_DIR / "preprocess_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Combined: {combined_path} ({total_rows} rows)")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
