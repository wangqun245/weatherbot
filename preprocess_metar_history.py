from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_INPUT_DIR = Path(r"C:\weather\metar_history")
DEFAULT_OUTPUT_DIR = Path(r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed")

STATION_TIMEZONES = {
    "KATL": "America/New_York",
    "KAUS": "America/Chicago",
    "KDAL": "America/Chicago",
    "KBKF": "America/Denver",
    "KDEN": "America/Denver",
    "KHOU": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KLGA": "America/New_York",
    "KMIA": "America/New_York",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
}

RMK_TEMP_RE = re.compile(r"(?:^|\s)T([01])(\d{3})([01])(\d{3})(?:\s|$)")


@dataclass(frozen=True)
class ParsedRow:
    raw_line: str
    valid_utc: datetime
    metar: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Drop AUTO METARs, compute city-local daily highs from RMK T groups, "
            "and export local-time training rows per station."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-hour", type=int, default=9)
    parser.add_argument("--end-hour", type=int, default=19)
    parser.add_argument(
        "--stations",
        nargs="*",
        help="Optional ICAO station list to process, for example: --stations KBKF",
    )
    parser.add_argument(
        "--full-day",
        action="store_true",
        help="Export all local-time rows for each local day instead of applying the hour window.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing station output files.",
    )
    return parser.parse_args()


def precise_temp_c_from_rmk(metar: str) -> float | None:
    match = RMK_TEMP_RE.search(metar)
    if not match:
        return None
    sign, tenths, _dew_sign, _dew_tenths = match.groups()
    temp_c = int(tenths) / 10.0
    if sign == "1":
        temp_c *= -1
    return temp_c


def c_to_f(temp_c: float) -> float:
    return (temp_c * 9.0 / 5.0) + 32.0


def parse_raw_line(raw_line: str) -> ParsedRow | None:
    line = raw_line.rstrip("\r\n")
    if not line or line.startswith("#") or line == "station,valid,metar":
        return None

    parts = line.split(",", 2)
    if len(parts) != 3:
        return None

    _station, valid_text, metar = parts
    try:
        valid_utc = datetime.strptime(valid_text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    return ParsedRow(raw_line=line, valid_utc=valid_utc, metar=metar)


def station_files(station_dir: Path) -> list[Path]:
    return sorted(station_dir.glob(f"{station_dir.name}_*_metar.csv"))


def compute_daily_highs(station_dir: Path, tz: ZoneInfo, stats: Counter) -> dict[str, float]:
    daily_highs_c: dict[str, float] = {}
    for source_file in station_files(station_dir):
        with source_file.open("r", encoding="utf-8", newline="") as handle:
            for raw_line in handle:
                stats["input_lines"] += 1
                row = parse_raw_line(raw_line)
                if row is None:
                    continue

                stats["data_rows"] += 1
                if " AUTO " in f" {row.metar} ":
                    stats["auto_rows_removed"] += 1
                    continue

                temp_c = precise_temp_c_from_rmk(row.metar)
                if temp_c is None:
                    stats["non_auto_rows_without_precise_rmk_temp"] += 1
                    continue

                local_date = row.valid_utc.astimezone(tz).date().isoformat()
                previous = daily_highs_c.get(local_date)
                if previous is None or temp_c > previous:
                    daily_highs_c[local_date] = temp_c

    stats["local_days_with_daily_high"] = len(daily_highs_c)
    return daily_highs_c


def write_training_rows(
    station_dir: Path,
    output_file: Path,
    tz: ZoneInfo,
    daily_highs_c: dict[str, float],
    start_hour: int,
    end_hour: int,
    full_day: bool,
    stats: Counter,
) -> None:
    with output_file.open("w", encoding="utf-8", newline="") as out_handle:
        out_handle.write("daily_high_f,station,valid,metar\n")

        for source_file in station_files(station_dir):
            with source_file.open("r", encoding="utf-8", newline="") as in_handle:
                for raw_line in in_handle:
                    row = parse_raw_line(raw_line)
                    if row is None:
                        continue
                    if " AUTO " in f" {row.metar} ":
                        continue

                    local_dt = row.valid_utc.astimezone(tz)
                    if not full_day:
                        if local_dt.hour < start_hour or local_dt.hour > end_hour:
                            stats["non_auto_rows_outside_local_window"] += 1
                            continue
                        if local_dt.hour == end_hour and (local_dt.minute, local_dt.second, local_dt.microsecond) > (0, 0, 0):
                            stats["non_auto_rows_outside_local_window"] += 1
                            continue

                    local_date = local_dt.date().isoformat()
                    high_c = daily_highs_c.get(local_date)
                    if high_c is None:
                        stats["window_rows_without_daily_high_skipped"] += 1
                        continue

                    daily_high_f = c_to_f(high_c)
                    out_handle.write(f"{daily_high_f:.1f},{row.raw_line}\n")
                    stats["output_rows"] += 1


def output_window_label(start_hour: int, end_hour: int, full_day: bool) -> str:
    if full_day:
        return "0000_2359"
    return f"{start_hour:02d}00_{end_hour:02d}00"


def main() -> int:
    args = parse_args()
    if not args.input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")
    if not 0 <= args.start_hour <= args.end_hour <= 23:
        raise SystemExit("--start-hour and --end-hour must be between 0 and 23, with start <= end")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    requested_stations = {station.upper() for station in (args.stations or [])}
    station_dirs = [
        p
        for p in sorted(args.input_dir.iterdir())
        if p.is_dir() and (not requested_stations or p.name.upper() in requested_stations)
    ]
    if requested_stations and not station_dirs:
        raise SystemExit(f"No requested station directories found: {', '.join(sorted(requested_stations))}")
    window_label = output_window_label(args.start_hour, args.end_hour, args.full_day)

    summary: dict[str, dict[str, int | str]] = {}
    totals: Counter = Counter()
    missing_timezone: list[str] = []

    for station_dir in station_dirs:
        station = station_dir.name.upper()
        tz_name = STATION_TIMEZONES.get(station)
        if tz_name is None:
            missing_timezone.append(station)
            continue

        output_file = args.output_dir / f"{station}_local_{window_label}_daily_high.csv"
        if output_file.exists() and not args.overwrite:
            raise SystemExit(f"Output exists; rerun with --overwrite to replace it: {output_file}")

        stats: Counter = Counter()
        tz = ZoneInfo(tz_name)
        daily_highs_c = compute_daily_highs(station_dir, tz, stats)
        write_training_rows(
            station_dir=station_dir,
            output_file=output_file,
            tz=tz,
            daily_highs_c=daily_highs_c,
            start_hour=args.start_hour,
            end_hour=args.end_hour,
            full_day=args.full_day,
            stats=stats,
        )
        stats["source_files"] = len(station_files(station_dir))
        stats["timezone"] = tz_name
        stats["local_window"] = window_label
        stats["full_day"] = int(args.full_day)
        stats["output_file"] = str(output_file)
        summary[station] = dict(stats)
        totals.update({key: value for key, value in stats.items() if isinstance(value, int)})
        print(
            f"{station}: wrote {stats['output_rows']} rows, removed {stats['auto_rows_removed']} AUTO rows, "
            f"{stats['local_days_with_daily_high']} local days with highs"
        )

    if missing_timezone:
        raise SystemExit(f"Missing timezone mapping for station(s): {', '.join(missing_timezone)}")

    summary["_totals"] = dict(totals)
    summary_file = args.output_dir / "preprocess_summary.json"
    with summary_file.open("w", encoding="utf-8", newline="") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Summary written to {summary_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
