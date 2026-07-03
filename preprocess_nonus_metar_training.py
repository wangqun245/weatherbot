from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_metar_twc_all_stations import STATION_LOCATIONS, parse_metar_temp_c


INPUT_DIR = Path(r"C:\weather\metar_history")
OUTPUT_DIR = Path(r"C:\Users\Jack\Documents\git\weatherbot\NONUS")
COVERAGE_FILE = (
    Path(__file__).resolve().parent
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
SPECI_RE = re.compile(r"(?:^|\s)SPECI(?:\s|$)")
AUTO_RE = re.compile(r"(?:^|\s)AUTO(?:\s|$)")


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


def load_station_rows(station: str, tz: ZoneInfo) -> list[dict]:
    rows = []
    for path in sorted((INPUT_DIR / station).glob(f"{station}_*_metar.csv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            for source in csv.DictReader(handle):
                valid_text = (source.get("valid") or "").strip()
                metar = (source.get("metar") or "").strip()
                if not valid_text or not metar:
                    continue
                try:
                    valid_utc = datetime.strptime(
                        valid_text, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                local_dt = valid_utc.astimezone(tz)
                rows.append(
                    {
                        "station": station,
                        "valid_utc": valid_utc,
                        "valid_local": local_dt,
                        "local_year": local_dt.year,
                        "local_date": local_dt.date().isoformat(),
                        "metar": metar,
                        "is_auto": bool(AUTO_RE.search(metar)),
                        "is_speci": bool(SPECI_RE.search(metar)),
                    }
                )
    return rows


def main() -> int:
    if not COVERAGE_FILE.exists():
        raise SystemExit(f"Coverage file does not exist: {COVERAGE_FILE}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    qualified_years = load_qualified_years()
    summary = {}
    combined_rows = 0

    combined_path = OUTPUT_DIR / "all_nonus_local_full_day_daily_high.csv"
    with combined_path.open("w", encoding="utf-8", newline="") as combined:
        fieldnames = [
            "daily_high_f",
            "station",
            "valid",
            "valid_local",
            "metar",
        ]
        combined_writer = csv.DictWriter(combined, fieldnames=fieldnames)
        combined_writer.writeheader()

        for station in sorted(STATIONS):
            timezone_name, _country = STATION_LOCATIONS[station]
            tz = ZoneInfo(timezone_name)
            years = qualified_years.get(station, set())
            stats = Counter()
            all_rows = load_station_rows(station, tz)
            selected = [
                row for row in all_rows
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
                stats["decoded_speci_temperatures" if row["is_speci"] else "decoded_regular_temperatures"] += 1
                previous = daily_highs_c.get(row["local_date"])
                if previous is None or temp_c > previous:
                    daily_highs_c[row["local_date"]] = temp_c

            station_path = OUTPUT_DIR / f"{station}_local_full_day_daily_high.csv"
            with station_path.open(
                "w", encoding="utf-8", newline=""
            ) as station_file:
                station_writer = csv.DictWriter(
                    station_file, fieldnames=fieldnames
                )
                station_writer.writeheader()
                for row in sorted(non_auto, key=lambda item: item["valid_utc"]):
                    high_c = daily_highs_c.get(row["local_date"])
                    if high_c is None:
                        stats["rows_without_daily_high_excluded"] += 1
                        continue
                    output = {
                        "daily_high_f": f"{high_c * 9.0 / 5.0 + 32.0:.1f}",
                        "station": station,
                        "valid": row["valid_utc"].strftime("%Y-%m-%d %H:%M"),
                        "valid_local": row["valid_local"].isoformat(
                            timespec="minutes"
                        ),
                        "metar": row["metar"],
                    }
                    station_writer.writerow(output)
                    combined_writer.writerow(output)
                    stats["output_rows"] += 1
                    combined_rows += 1

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
        "minimum_complete_day_ratio": MIN_COMPLETE_DAY_RATIO,
        "excluded_stations": sorted(EXCLUDED_STATIONS),
        "auto_excluded_from_labels_and_rows": True,
        "speci_included_in_daily_high": True,
        "daily_high_temperature_priority": "RMK T group, then main temperature group",
        "combined_output_file": str(combined_path),
        "combined_output_rows": combined_rows,
    }
    summary_path = OUTPUT_DIR / "preprocess_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Combined: {combined_path} ({combined_rows} rows)")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
