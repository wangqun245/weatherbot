from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_metar_twc_all_stations import STATION_LOCATIONS


INPUT_DIR = Path(r"C:\weather\metar_history")
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "metar_30min_coverage"
STATIONS = (
    "RJTT", "RKSI", "ZSPD", "RCSS", "NZWN", "LEMD", "LFPB", "WSSS",
    "EGLC", "LTAC", "RKPK", "EDDM", "WMKK", "EFHK", "LIMC", "EPWA",
    "VILK", "OPKC", "EHAM",
)
SPECI_RE = re.compile(r"(?:^|\s)SPECI(?:\s|$)")
MIN_COMPLETE_DAY_RATIO = 0.95


def utc_range_for_local_day(
    day: date, tz: ZoneInfo, phase: int
) -> list[datetime]:
    start = datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz).astimezone(
        timezone.utc
    )
    values = []
    current = start + timedelta(minutes=(phase - start.minute) % 30)
    while current < end:
        values.append(current)
        current += timedelta(minutes=30)
    return values


def load_year(station: str, year: int) -> list[tuple[datetime, str]]:
    path = INPUT_DIR / station / f"{station}_{year}_metar.csv"
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            valid = (row.get("valid") or "").strip()
            metar = (row.get("metar") or "").strip()
            if not valid or not metar or SPECI_RE.search(metar):
                continue
            try:
                observed = datetime.strptime(valid, "%Y-%m-%d %H:%M").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            rows.append((observed, metar))
    return rows


def choose_phase(rows: list[tuple[datetime, str]]) -> int | None:
    counts = Counter(observed.minute % 30 for observed, _ in rows)
    return counts.most_common(1)[0][0] if counts else None


def analyze_year(
    station: str,
    year: int,
    tz: ZoneInfo,
    today_local: date,
) -> dict:
    rows = load_year(station, year)
    phase = choose_phase(rows)
    if phase is None:
        return {
            "station": station,
            "year": year,
            "phase_minutes": "",
            "eligible_days": 0,
            "complete_days": 0,
            "complete_day_ratio": 0.0,
            "regular_observations": 0,
            "excluded_off_phase_observations": 0,
            "qualifies": False,
        }

    regular_times = {
        observed
        for observed, _ in rows
        if observed.minute % 30 == phase
    }
    off_phase = sum(observed.minute % 30 != phase for observed, _ in rows)

    first_day = date(year, 1, 1)
    last_day = date(year, 12, 31)
    if year == today_local.year:
        last_day = min(last_day, today_local - timedelta(days=1))

    eligible_days = 0
    complete_days = 0
    day = first_day
    while day <= last_day:
        expected = utc_range_for_local_day(day, tz, phase)
        eligible_days += 1
        if expected and all(observed in regular_times for observed in expected):
            complete_days += 1
        day += timedelta(days=1)

    ratio = complete_days / eligible_days if eligible_days else 0.0
    return {
        "station": station,
        "year": year,
        "phase_minutes": f"{phase:02d}/{(phase + 30) % 60:02d}",
        "eligible_days": eligible_days,
        "complete_days": complete_days,
        "complete_day_ratio": round(ratio, 6),
        "regular_observations": len(regular_times),
        "excluded_off_phase_observations": off_phase,
        "qualifies": ratio >= MIN_COMPLETE_DAY_RATIO,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now().year
    details = []
    summaries = []

    for station in STATIONS:
        timezone_name, _ = STATION_LOCATIONS[station]
        tz = ZoneInfo(timezone_name)
        today_local = datetime.now(timezone.utc).astimezone(tz).date()
        available_years = sorted(
            int(path.name.split("_")[1])
            for path in (INPUT_DIR / station).glob(f"{station}_*_metar.csv")
        )
        station_rows = [
            analyze_year(station, year, tz, today_local)
            for year in available_years
            if year <= current_year
        ]
        details.extend(station_rows)
        completed_rows = [
            row for row in station_rows if row["year"] < current_year
        ]
        qualified = [row["year"] for row in completed_rows if row["qualifies"]]
        current_row = next(
            (row for row in station_rows if row["year"] == current_year), None
        )

        continuous_years = []
        for year in range(
            current_year - 1, min(available_years or [current_year]) - 1, -1
        ):
            row = next(
                (item for item in completed_rows if item["year"] == year), None
            )
            if row is None or not row["qualifies"]:
                break
            continuous_years.append(year)

        summaries.append(
            {
                "station": station,
                "timezone": timezone_name,
                "first_qualified_year": min(qualified) if qualified else None,
                "latest_qualified_year": max(qualified) if qualified else None,
                "qualified_year_count": len(qualified),
                "continuous_start_year": (
                    min(continuous_years) if continuous_years else None
                ),
                "continuous_qualified_year_count": len(continuous_years),
                "qualified_years": qualified,
                "current_partial_year": current_year,
                "current_partial_year_qualifies_to_date": (
                    current_row["qualifies"] if current_row else False
                ),
                "current_partial_year_complete_day_ratio": (
                    current_row["complete_day_ratio"] if current_row else None
                ),
            }
        )

    detail_path = OUTPUT_DIR / "station_year_30min_coverage.csv"
    with detail_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(details[0]))
        writer.writeheader()
        writer.writerows(details)

    summary_path = OUTPUT_DIR / "station_30min_training_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "definition": {
                    "minimum_complete_day_ratio": MIN_COMPLETE_DAY_RATIO,
                    "explicit_speci_excluded": True,
                    "off_schedule_observations_excluded": True,
                    "current_year_through_yesterday": True,
                },
                "stations": summaries,
                "detail_file": str(detail_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(f"Detail: {detail_path}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
