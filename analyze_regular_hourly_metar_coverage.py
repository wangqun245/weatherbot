from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_metar_twc_all_stations import STATION_LOCATIONS


INPUT_DIR = Path(r"C:\weather\metar_history")
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs" / "metar_hourly_coverage"
STATIONS = (
    "RJTT", "RKSI", "ZSPD", "RCSS", "NZWN", "LEMD", "LFPB", "WSSS",
    "EGLC", "LTAC", "RKPK", "EDDM", "WMKK", "EFHK", "LIMC", "EPWA",
    "VILK", "OPKC", "EHAM",
)
SPECI_RE = re.compile(r"(?:^|\s)SPECI(?:\s|$)")
MIN_COMPLETE_DAY_RATIO = 0.98
MAX_MISSING_DAYS_PER_YEAR = 7
ALIGNMENT_TOLERANCE_MINUTES = 2


def circular_minute_distance(left: int, right: int) -> int:
    difference = abs(left - right)
    return min(difference, 60 - difference)


def load_year(station: str, year: int) -> list[datetime]:
    path = INPUT_DIR / station / f"{station}_{year}_metar.csv"
    if not path.exists():
        return []
    observations = []
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
            observations.append(observed)
    return observations


def choose_scheduled_minute(observations: list[datetime]) -> int | None:
    if not observations:
        return None
    counts = Counter(observed.minute for observed in observations)
    scores = {
        minute: sum(
            count
            for observed_minute, count in counts.items()
            if circular_minute_distance(observed_minute, minute)
            <= ALIGNMENT_TOLERANCE_MINUTES
        )
        for minute in range(60)
    }
    return max(scores, key=lambda minute: (scores[minute], counts[minute], -minute))


def expected_hourly_times(
    day: date, tz: ZoneInfo, scheduled_minute: int
) -> list[datetime]:
    start = datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz).astimezone(
        timezone.utc
    )
    current = start.replace(minute=scheduled_minute, second=0, microsecond=0)
    if current < start:
        current += timedelta(hours=1)
    values = []
    while current < end:
        values.append(current)
        current += timedelta(hours=1)
    return values


def matched_target(observed: datetime, scheduled_minute: int) -> datetime | None:
    candidates = [
        observed.replace(minute=scheduled_minute, second=0, microsecond=0)
        + timedelta(hours=offset)
        for offset in (-1, 0, 1)
    ]
    target = min(candidates, key=lambda value: abs(value - observed))
    if abs((target - observed).total_seconds()) > 150:
        return None
    return target


def analyze_year(
    station: str, year: int, tz: ZoneInfo, today_local: date
) -> dict:
    observations = load_year(station, year)
    scheduled_minute = choose_scheduled_minute(observations)
    if scheduled_minute is None:
        return {
            "station": station,
            "year": year,
            "scheduled_minute": "",
            "eligible_days": 0,
            "complete_days": 0,
            "complete_day_ratio": 0.0,
            "aligned_observations": 0,
            "excluded_misaligned_observations": 0,
            "qualifies": False,
        }

    targets = {
        target
        for observed in observations
        if (target := matched_target(observed, scheduled_minute)) is not None
    }
    aligned_count = sum(
        matched_target(observed, scheduled_minute) is not None
        for observed in observations
    )

    last_day = date(year, 12, 31)
    if year == today_local.year:
        last_day = min(last_day, today_local - timedelta(days=1))

    eligible_days = 0
    complete_days = 0
    day = date(year, 1, 1)
    while day <= last_day:
        expected = expected_hourly_times(day, tz, scheduled_minute)
        eligible_days += 1
        if expected and all(target in targets for target in expected):
            complete_days += 1
        day += timedelta(days=1)

    ratio = complete_days / eligible_days if eligible_days else 0.0
    return {
        "station": station,
        "year": year,
        "scheduled_minute": f"{scheduled_minute:02d}",
        "eligible_days": eligible_days,
        "complete_days": complete_days,
        "complete_day_ratio": round(ratio, 6),
        "aligned_observations": aligned_count,
        "excluded_misaligned_observations": len(observations) - aligned_count,
        "qualifies": (
            ratio >= MIN_COMPLETE_DAY_RATIO
            and eligible_days - complete_days <= MAX_MISSING_DAYS_PER_YEAR
        ),
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
        years = sorted(
            int(path.name.split("_")[1])
            for path in (INPUT_DIR / station).glob(f"{station}_*_metar.csv")
        )
        rows = [
            analyze_year(station, year, tz, today_local)
            for year in years
            if year <= current_year
        ]
        details.extend(rows)
        completed = [row for row in rows if row["year"] < current_year]
        qualified = [row["year"] for row in completed if row["qualifies"]]
        current = next(
            (row for row in rows if row["year"] == current_year), None
        )

        continuous = []
        for year in range(current_year - 1, min(years or [current_year]) - 1, -1):
            row = next((item for item in completed if item["year"] == year), None)
            if row is None or not row["qualifies"]:
                break
            continuous.append(year)

        summaries.append(
            {
                "station": station,
                "timezone": timezone_name,
                "first_qualified_year": min(qualified) if qualified else None,
                "latest_qualified_year": max(qualified) if qualified else None,
                "qualified_year_count": len(qualified),
                "continuous_start_year": min(continuous) if continuous else None,
                "continuous_qualified_year_count": len(continuous),
                "qualified_years": qualified,
                "current_partial_year": current_year,
                "current_partial_year_qualifies_to_date": (
                    current["qualifies"] if current else False
                ),
                "current_partial_year_complete_day_ratio": (
                    current["complete_day_ratio"] if current else None
                ),
            }
        )

    detail_path = OUTPUT_DIR / "station_year_hourly_coverage.csv"
    with detail_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(details[0]))
        writer.writeheader()
        writer.writerows(details)

    summary_path = OUTPUT_DIR / "station_hourly_training_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "definition": {
                    "minimum_complete_day_ratio": MIN_COMPLETE_DAY_RATIO,
                    "maximum_missing_days_per_year": MAX_MISSING_DAYS_PER_YEAR,
                    "alignment_tolerance_minutes": 2.5,
                    "explicit_speci_excluded": True,
                    "each_local_hour_requires_aligned_metar": True,
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
