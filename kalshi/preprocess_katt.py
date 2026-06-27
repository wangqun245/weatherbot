from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


DEFAULT_INPUT_DIR = Path(r"C:\weather\metar_history\KATT")
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "data" / "KATT_nws_lst_daily_high.csv"
NWS_LST = timezone(timedelta(hours=-6), name="CST")

T_RE = re.compile(r"(?:^|\s)T([01])(\d{3})([01])(\d{3})(?=\s|$)")
MAX_6H_RE = re.compile(r"(?:^|\s)1([01])(\d{3})(?=\s|$)")
EXTREME_24H_RE = re.compile(r"(?:^|\s)4([01])(\d{3})([01])(\d{3})(?=\s|$)")


@dataclass(frozen=True)
class RawRow:
    station: str
    valid_text: str
    valid_utc: datetime
    metar: str

    @property
    def climate_date(self) -> date:
        return self.valid_utc.astimezone(NWS_LST).date()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tag every KATT METAR with its NWS-LST daily high."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(1997, 1, 1),
        help="First eligible climate date; years before 1997 lack complete ASOS extrema.",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date(2025, 12, 31),
        help="Last eligible climate date; defaults to the latest completed calendar year.",
    )
    parser.add_argument(
        "--allow-instantaneous-fallback",
        action="store_true",
        help="Keep days lacking ASOS 24-hour and 6-hour extrema by using instantaneous T values.",
    )
    return parser.parse_args()


def signed_tenths_f(sign: str, digits: str) -> Decimal:
    value_c = Decimal(digits) / Decimal(10)
    if sign == "1":
        value_c = -value_c
    return value_c * Decimal(9) / Decimal(5) + Decimal(32)


def nws_integer_f(value_f: Decimal) -> int:
    return int(value_f.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def parse_line(line: str) -> RawRow | None:
    if not line or line.startswith("#") or line.startswith("station,"):
        return None
    parts = line.rstrip("\r\n").split(",", 2)
    if len(parts) != 3:
        return None
    station, valid_text, metar = parts
    try:
        valid_utc = datetime.strptime(valid_text, "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return RawRow(station, valid_text, valid_utc, metar)


def extrema(row: RawRow) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    current = None
    max_6h = None
    max_24h = None
    match = T_RE.search(row.metar)
    if match:
        current = signed_tenths_f(match.group(1), match.group(2))
    match = MAX_6H_RE.search(row.metar)
    if match:
        max_6h = signed_tenths_f(match.group(1), match.group(2))
    match = EXTREME_24H_RE.search(row.metar)
    if match:
        max_24h = signed_tenths_f(match.group(1), match.group(2))
    return current, max_6h, max_24h


def main() -> int:
    args = parse_args()
    files = sorted(args.input_dir.glob("KATT_*_metar.csv"))
    if not files:
        raise SystemExit(f"No KATT raw files found in {args.input_dir}")

    rows_by_day: dict[date, list[RawRow]] = defaultdict(list)
    stats: Counter = Counter()
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for line in handle:
                row = parse_line(line)
                if row is None:
                    continue
                day = row.climate_date
                if args.start_date and day < args.start_date:
                    continue
                if args.end_date and day > args.end_date:
                    continue
                rows_by_day[day].append(row)
                stats["raw_rows"] += 1

    daily_highs: dict[date, int] = {}
    source_by_day: dict[date, str] = {}
    for day, rows in rows_by_day.items():
        current_values: list[Decimal] = []
        max_6h_values: list[Decimal] = []
        max_24h_values: list[Decimal] = []
        for row in rows:
            current, max_6h, max_24h = extrema(row)
            if current is not None:
                current_values.append(current)
            if max_6h is not None:
                max_6h_values.append(max_6h)
            if max_24h is not None:
                max_24h_values.append(max_24h)

        if max_24h_values:
            high = max(max_24h_values)
            source = "rmk_24h_extreme"
        elif max_6h_values:
            high = max(max_6h_values)
            source = "rmk_6h_max"
        elif current_values and args.allow_instantaneous_fallback:
            high = max(current_values)
            source = "rmk_instantaneous"
        else:
            stats["days_without_asos_extrema_label"] += 1
            continue
        daily_highs[day] = nws_integer_f(high)
        source_by_day[day] = source
        stats[f"days_source_{source}"] += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["daily_high_f", "station", "valid", "metar"])
        for day in sorted(rows_by_day):
            high = daily_highs.get(day)
            if high is None:
                continue
            for row in sorted(rows_by_day[day], key=lambda item: item.valid_utc):
                writer.writerow([high, row.station, row.valid_text, row.metar])
                stats["output_rows"] += 1

    stats["days_with_daily_high"] = len(daily_highs)
    stats["first_climate_date"] = min(daily_highs).isoformat() if daily_highs else ""
    stats["last_climate_date"] = max(daily_highs).isoformat() if daily_highs else ""
    stats["timezone"] = "fixed CST (UTC-06:00), matching NWS LST climate day"
    stats["output_file"] = str(args.output)
    summary_path = args.output.with_name("preprocess_summary.json")
    summary_path.write_text(
        json.dumps(dict(stats), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"Wrote {stats['output_rows']:,} rows across {stats['days_with_daily_high']:,} "
        f"days to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
