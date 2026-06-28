from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_kaus_metar_to_nws_cli import download_year, load_cli_reports


LOS_ANGELES = ZoneInfo("America/Los_Angeles")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare processed KLAX local daily-high labels with NWS CLILAX."
    )
    parser.add_argument(
        "--metar-csv",
        type=Path,
        default=Path("metar_history_processed2")
        / "KLAX_local_0000_2359_daily_high.csv",
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs") / "KLAX_processed_vs_NWS_CLILAX_2000_present.csv",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("outputs") / "KLAX_processed_vs_NWS_CLILAX_2000_present_summary.json",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("outputs") / "nws_cli_lax_archive",
    )
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def round_fahrenheit(value: str) -> int:
    return int(Decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def load_processed_daily_highs(path: Path, start_year: int) -> dict[date, float]:
    daily: dict[date, float] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            valid_utc = datetime.strptime(row["valid"], "%Y-%m-%d %H:%M").replace(
                tzinfo=timezone.utc
            )
            local_day = valid_utc.astimezone(LOS_ANGELES).date()
            if local_day.year < start_year:
                continue
            value = float(row["daily_high_f"])
            previous = daily.get(local_day)
            if previous is not None and abs(previous - value) > 1e-9:
                raise ValueError(
                    f"Inconsistent daily_high_f labels for {local_day}: "
                    f"{previous} and {value}"
                )
            daily[local_day] = value
    return daily


def main() -> int:
    args = parse_args()
    daily = load_processed_daily_highs(args.metar_csv, args.start_year)
    if not daily:
        raise SystemExit(f"No KLAX daily labels found in {args.metar_csv}")

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    last_day = max(daily)
    archives = [
        download_year(year, args.cache_dir, args.refresh, "CLILAX")
        for year in range(args.start_year, last_day.year + 2)
    ]
    reports, parsed_versions = load_cli_reports(archives)

    output_rows: list[dict[str, object]] = []
    differences: dict[int, int] = {}
    exact = comparable = missing_nws = missing_maximum = 0
    for local_day, precise_high in sorted(daily.items()):
        report = reports.get(local_day)
        rounded_high = round_fahrenheit(str(precise_high))
        row: dict[str, object] = {
            "local_date": local_day.isoformat(),
            "processed_daily_high_f": precise_high,
            "processed_daily_high_f_rounded": rounded_high,
        }
        if report is None:
            missing_nws += 1
            row["status"] = "nws_report_missing"
        elif report.maximum_f is None:
            missing_maximum += 1
            row["status"] = "nws_maximum_missing"
            row["nws_product_id"] = report.product_id
        else:
            difference = rounded_high - report.maximum_f
            comparable += 1
            exact += difference == 0
            differences[difference] = differences.get(difference, 0) + 1
            row.update(
                {
                    "nws_cli_maximum_f": report.maximum_f,
                    "difference_f": difference,
                    "is_exact_match": difference == 0,
                    "status": "match" if difference == 0 else "mismatch",
                    "nws_report_type": report.period_label,
                    "nws_product_id": report.product_id,
                    "nws_issued_utc": (
                        report.issued_utc.isoformat(timespec="minutes")
                        if report.issued_utc
                        else ""
                    ),
                }
            )
        output_rows.append(row)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "local_date",
        "processed_daily_high_f",
        "processed_daily_high_f_rounded",
        "nws_cli_maximum_f",
        "difference_f",
        "is_exact_match",
        "status",
        "nws_report_type",
        "nws_product_id",
        "nws_issued_utc",
    ]
    with args.output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    summary = {
        "station": "KLAX",
        "nws_product": "CLILAX",
        "comparison_start_date": min(daily).isoformat(),
        "comparison_end_date": max(daily).isoformat(),
        "processed_daily_days": len(daily),
        "comparable_days": comparable,
        "exact_matches": exact,
        "mismatches": comparable - exact,
        "exact_match_rate_percent": round(exact * 100 / comparable, 4)
        if comparable
        else None,
        "nws_report_missing_days": missing_nws,
        "nws_maximum_missing_days": missing_maximum,
        "difference_f_distribution": {
            str(key): value for key, value in sorted(differences.items())
        },
        "parsed_cli_product_versions": parsed_versions,
        "processed_rounding": "ROUND_HALF_UP to integer Fahrenheit",
        "nws_selection": (
            "Latest completed YESTERDAY CLILAX report per climate date; "
            "otherwise latest TODAY report."
        ),
    }
    args.summary.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
