from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_INPUT_DIR = Path(r"C:\weather2\metar_history\KAUS")
DEFAULT_OUTPUT_FILE = Path("outputs") / "KAUS_auto_daily_highs_local_2020_present.csv"
CIVIL_TZ = ZoneInfo("America/Chicago")
NWS_LST_TZ = timezone(timedelta(hours=-6), name="CST")
PRECISE_TEMP_RE = re.compile(r"(?:^|\s)T([01])(\d{3})([01])(\d{3})(?=\s|$)")
SIX_HOUR_MAX_RE = re.compile(r"(?:^|\s)1([01])(\d{3})(?=\s|$)")


@dataclass(frozen=True)
class Observation:
    valid_utc: datetime
    temp_f: Decimal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate KAUS local-day highs from five-minute AUTO METAR observations."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--station", help="ICAO code; defaults to input directory name.")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2020, 1, 1))
    parser.add_argument(
        "--max-gap-minutes",
        type=int,
        default=10,
        help="Largest observation gap allowed for complete-day status.",
    )
    parser.add_argument(
        "--nws-lst",
        action="store_true",
        help="Use the NWS climate-day timezone (fixed CST/UTC-6) instead of civil time.",
    )
    parser.add_argument(
        "--use-six-hour-max",
        action="store_true",
        help="Use ASOS six-hour maximum-temperature RMK groups when present.",
    )
    return parser.parse_args()


def parse_observation(line: str, use_six_hour_max: bool = False) -> Observation | None:
    if not line or line.startswith("#") or line.startswith("station,"):
        return None
    parts = line.rstrip("\r\n").split(",", 2)
    if len(parts) != 3:
        return None
    _, valid_text, metar = parts
    if " AUTO " not in f" {metar} ":
        return None

    match = PRECISE_TEMP_RE.search(metar)
    if match is None:
        return None
    temp_sign, temp_tenths, _, _ = match.groups()
    if use_six_hour_max:
        six_hour_match = SIX_HOUR_MAX_RE.search(metar)
        if six_hour_match is not None:
            temp_sign, temp_tenths = six_hour_match.groups()
    temp_c = Decimal(temp_tenths) / Decimal(10)
    if temp_sign == "1":
        temp_c = -temp_c
    temp_f = temp_c * Decimal(9) / Decimal(5) + Decimal(32)

    try:
        valid_utc = datetime.strptime(valid_text, "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return Observation(valid_utc=valid_utc, temp_f=temp_f)


def source_files(input_dir: Path, station: str) -> list[Path]:
    return sorted(input_dir.glob(f"{station}_*_metar.csv"))


def fmt_local(dt: datetime, local_tz: timezone | ZoneInfo) -> str:
    return dt.astimezone(local_tz).isoformat(timespec="minutes")


def main() -> int:
    args = parse_args()
    local_tz = NWS_LST_TZ if args.nws_lst else CIVIL_TZ
    station = (args.station or args.input_dir.name).upper()
    files = source_files(args.input_dir, station)
    if not files:
        raise SystemExit(f"No {station} raw METAR files found in {args.input_dir}")

    by_day: dict[date, list[Observation]] = defaultdict(list)
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for line in handle:
                obs = parse_observation(line, args.use_six_hour_max)
                if obs is None:
                    continue
                local_day = obs.valid_utc.astimezone(local_tz).date()
                if local_day >= args.start_date:
                    by_day[local_day].append(obs)

    if not by_day:
        raise SystemExit("No matching AUTO observations with precise RMK temperatures found")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    last_day = max(by_day)
    fieldnames = [
        "local_date",
        "max_temp_f",
        "max_temp_f_integer",
        "first_max_local_time",
        "last_max_local_time",
        "first_max_utc_time",
        "last_max_utc_time",
        "auto_observation_count",
        "first_observation_local_time",
        "last_observation_local_time",
        "largest_observation_gap_minutes",
        "coverage_status",
    ]
    with args.output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        day = args.start_date
        while day <= last_day:
            observations = sorted(by_day.get(day, []), key=lambda item: item.valid_utc)
            if not observations:
                writer.writerow({"local_date": day.isoformat(), "coverage_status": "missing"})
                day += timedelta(days=1)
                continue

            high_f = max(item.temp_f for item in observations)
            maxima = [item for item in observations if item.temp_f == high_f]
            first_local = observations[0].valid_utc.astimezone(local_tz)
            last_local = observations[-1].valid_utc.astimezone(local_tz)
            largest_gap_minutes = max(
                (
                    int(
                        (current.valid_utc - previous.valid_utc).total_seconds()
                        // 60
                    )
                    for previous, current in zip(observations, observations[1:])
                ),
                default=0,
            )
            complete = (
                first_local.hour == 0
                and first_local.minute <= args.max_gap_minutes
                and last_local.hour == 23
                and last_local.minute >= max(0, 60 - args.max_gap_minutes)
                and largest_gap_minutes <= args.max_gap_minutes
            )
            writer.writerow(
                {
                    "local_date": day.isoformat(),
                    "max_temp_f": f"{high_f:.2f}",
                    "max_temp_f_integer": str(
                        high_f.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
                    ),
                    "first_max_local_time": fmt_local(maxima[0].valid_utc, local_tz),
                    "last_max_local_time": fmt_local(maxima[-1].valid_utc, local_tz),
                    "first_max_utc_time": maxima[0].valid_utc.isoformat(timespec="minutes"),
                    "last_max_utc_time": maxima[-1].valid_utc.isoformat(timespec="minutes"),
                    "auto_observation_count": len(observations),
                    "first_observation_local_time": first_local.isoformat(timespec="minutes"),
                    "last_observation_local_time": last_local.isoformat(timespec="minutes"),
                    "largest_observation_gap_minutes": largest_gap_minutes,
                    "coverage_status": "complete" if complete else "partial",
                }
            )
            day += timedelta(days=1)

    print(f"Wrote {args.output} ({args.start_date} through {last_day})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
