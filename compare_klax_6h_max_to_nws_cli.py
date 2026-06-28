from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_kaus_metar_to_nws_cli import download_year, load_cli_reports


MAX_6H_RE = re.compile(r"(?:^|\s)1([01])(\d{3})(?=\s|$)")
PRECISE_TEMP_RE = re.compile(
    r"(?:^|\s)T([01])(\d{3})[01]\d{3}(?=\s|$)"
)
BODY_TEMP_RE = re.compile(r"(?:^|\s)(M?\d{2})/(?:M?\d{2}|//)(?=\s|$)")
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decode ASOS six-hour maxima by station-local day and compare with NWS CLI."
    )
    parser.add_argument("--station", default="KLAX")
    parser.add_argument("--timezone", default="America/Los_Angeles")
    parser.add_argument("--pil", default="CLILAX")
    parser.add_argument(
        "--input-dir", type=Path, default=Path(r"C:\weather\metar_history\KLAX")
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument(
        "--include-instantaneous-max",
        action="store_true",
        help="Take the daily maximum across six-hour maxima and normal METAR temperatures.",
    )
    parser.add_argument(
        "--daily-output",
        type=Path,
        default=Path("outputs") / "KLAX_6h_max_daily_local_2000_present.csv",
    )
    parser.add_argument(
        "--comparison-output",
        type=Path,
        default=Path("outputs") / "KLAX_6h_max_vs_NWS_CLILAX_2000_present.csv",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("outputs") / "KLAX_6h_max_vs_NWS_CLILAX_2000_present_summary.json",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("outputs") / "nws_cli_lax_archive",
    )
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def signed_tenths_f(sign: str, tenths: str) -> Decimal:
    value_c = Decimal(tenths) / Decimal(10)
    if sign == "1":
        value_c = -value_c
    return value_c * Decimal(9) / Decimal(5) + Decimal(32)


def nws_integer_f(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def instantaneous_temp_f(metar: str) -> Decimal | None:
    precise = PRECISE_TEMP_RE.search(metar)
    if precise:
        return signed_tenths_f(*precise.groups())
    body = BODY_TEMP_RE.search(metar)
    if body is None:
        return None
    text = body.group(1)
    value_c = Decimal(text[1:] if text.startswith("M") else text)
    if text.startswith("M"):
        value_c = -value_c
    return value_c * Decimal(9) / Decimal(5) + Decimal(32)


def load_six_hour_maxima(
    input_dir: Path,
    station: str,
    local_tz: ZoneInfo,
    start_year: int,
    include_instantaneous_max: bool = False,
) -> tuple[dict[date, list[tuple[datetime, Decimal, str, str]]], Counter]:
    by_day: dict[
        date, list[tuple[datetime, Decimal, str, str]]
    ] = defaultdict(list)
    stats: Counter = Counter()
    files = sorted(input_dir.glob(f"{station}_*_metar.csv"))
    if not files:
        raise SystemExit(f"No {station} raw CSV files found in {input_dir}")
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                stats["raw_rows"] += 1
                valid_utc = datetime.strptime(row["valid"], "%Y-%m-%d %H:%M").replace(
                    tzinfo=timezone.utc
                )
                metar = row.get("metar", "")
                match = MAX_6H_RE.search(metar)
                if match is not None:
                    stats["rows_with_6h_max"] += 1
                    # The 1snTTT group covers the preceding six hours.
                    local_day = (valid_utc - timedelta(hours=3)).astimezone(
                        local_tz
                    ).date()
                    if local_day.year >= start_year:
                        by_day[local_day].append(
                            (
                                valid_utc,
                                signed_tenths_f(*match.groups()),
                                metar,
                                "rmk_6h_max",
                            )
                        )
                if include_instantaneous_max:
                    current_f = instantaneous_temp_f(metar)
                    if current_f is None:
                        continue
                    stats["rows_with_instantaneous_temp"] += 1
                    local_day = valid_utc.astimezone(local_tz).date()
                    if local_day.year >= start_year:
                        by_day[local_day].append(
                            (
                                valid_utc,
                                current_f,
                                metar,
                                "metar_instantaneous",
                            )
                        )
    return by_day, stats


def main() -> int:
    args = parse_args()
    station = args.station.upper()
    local_tz = ZoneInfo(args.timezone)
    by_day, stats = load_six_hour_maxima(
        args.input_dir,
        station,
        local_tz,
        args.start_year,
        args.include_instantaneous_max,
    )
    if not by_day:
        raise SystemExit(f"No {station} six-hour maximum groups found")

    args.daily_output.parent.mkdir(parents=True, exist_ok=True)
    daily_rows: list[dict[str, object]] = []
    daily_highs: dict[date, int] = {}
    for local_day, observations in sorted(by_day.items()):
        high_f = max(value for _valid, value, _metar, _source in observations)
        maxima = [item for item in observations if item[1] == high_f]
        maximum_sources = sorted({item[3] for item in maxima})
        for source in maximum_sources:
            stats[f"days_max_source_{source}"] += 1
        rounded = nws_integer_f(high_f)
        daily_highs[local_day] = rounded
        daily_rows.append(
            {
                "local_date": local_day.isoformat(),
                "daily_max_temp_f": f"{high_f:.2f}",
                "daily_max_temp_f_integer": rounded,
                "six_hour_group_count": sum(
                    item[3] == "rmk_6h_max" for item in observations
                ),
                "instantaneous_observation_count": sum(
                    item[3] == "metar_instantaneous"
                    for item in observations
                ),
                "maximum_source": "+".join(maximum_sources),
                "first_max_report_local": maxima[0][0]
                .astimezone(local_tz)
                .isoformat(timespec="minutes"),
                "last_max_report_local": maxima[-1][0]
                .astimezone(local_tz)
                .isoformat(timespec="minutes"),
                "first_max_report_utc": maxima[0][0].isoformat(timespec="minutes"),
                "source_metar": maxima[0][2],
            }
        )
    daily_fields = list(daily_rows[0])
    with args.daily_output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=daily_fields)
        writer.writeheader()
        writer.writerows(daily_rows)

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    last_day = max(daily_highs)
    archives = [
        download_year(year, args.cache_dir, args.refresh, args.pil)
        for year in range(args.start_year, last_day.year + 2)
    ]
    reports, parsed_versions = load_cli_reports(archives)

    comparison_rows: list[dict[str, object]] = []
    differences: Counter = Counter()
    counters: Counter = Counter()
    first_day = date(args.start_year, 1, 1)
    day = first_day
    while day <= last_day:
        metar_high = daily_highs.get(day)
        report = reports.get(day)
        row: dict[str, object] = {
            "local_date": day.isoformat(),
            "station_daily_maximum_f": "" if metar_high is None else metar_high,
        }
        if metar_high is None:
            counters["station_daily_max_missing"] += 1
            row["status"] = "station_daily_max_missing"
        elif report is None:
            counters["nws_report_missing"] += 1
            row["status"] = "nws_report_missing"
        elif report.maximum_f is None:
            counters["nws_maximum_missing"] += 1
            row["status"] = "nws_maximum_missing"
        else:
            difference = metar_high - report.maximum_f
            exact = difference == 0
            counters["comparable_days"] += 1
            counters["exact_matches" if exact else "mismatches"] += 1
            counters["within_1f_days"] += abs(difference) <= 1
            differences[difference] += 1
            row.update(
                {
                    "nws_cli_maximum_f": report.maximum_f,
                    "difference_f": difference,
                    "is_exact_match": exact,
                    "status": "match" if exact else "mismatch",
                    "nws_report_type": report.period_label,
                    "nws_product_id": report.product_id,
                }
            )
        comparison_rows.append(row)
        day = date.fromordinal(day.toordinal() + 1)

    comparison_fields = [
        "local_date",
        "station_daily_maximum_f",
        "nws_cli_maximum_f",
        "difference_f",
        "is_exact_match",
        "status",
        "nws_report_type",
        "nws_product_id",
    ]
    with args.comparison_output.open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=comparison_fields, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(comparison_rows)

    comparable = counters["comparable_days"]
    summary = {
        "station": station,
        "nws_product": args.pil,
        "timezone": args.timezone,
        "label_source": (
            "maximum of RMK 1snTTT six-hour maxima (assigned by interval "
            "midpoint) and normal METAR instantaneous temperatures"
            if args.include_instantaneous_max
            else "maximum of RMK 1snTTT six-hour maximum groups by the local "
            "date containing each six-hour interval midpoint"
        ),
        "comparison_start_date": first_day.isoformat(),
        "comparison_end_date": last_day.isoformat(),
        "raw_rows": stats["raw_rows"],
        "rows_with_6h_max": stats["rows_with_6h_max"],
        "rows_with_instantaneous_temp": stats[
            "rows_with_instantaneous_temp"
        ],
        "days_with_daily_max": len(daily_highs),
        "days_with_6h_max": sum(
            any(item[3] == "rmk_6h_max" for item in observations)
            for observations in by_day.values()
        ),
        "days_max_source_rmk_6h_max": stats["days_max_source_rmk_6h_max"],
        "days_max_source_metar_instantaneous": stats[
            "days_max_source_metar_instantaneous"
        ],
        **dict(counters),
        "exact_match_rate_percent": round(
            counters["exact_matches"] * 100 / comparable, 4
        )
        if comparable
        else None,
        "within_1f_rate_percent": round(
            counters["within_1f_days"] * 100 / comparable, 4
        )
        if comparable
        else None,
        "difference_f_distribution": {
            str(key): value for key, value in sorted(differences.items())
        },
        "parsed_cli_product_versions": parsed_versions,
        "daily_output": str(args.daily_output),
        "comparison_output": str(args.comparison_output),
    }
    args.summary.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
