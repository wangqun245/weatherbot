from __future__ import annotations

import argparse
import csv
import io
import json
import re
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


IEM_RETRIEVE_URL = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
SUMMARY_DATE_RE = re.compile(
    r"CLIMATE SUMMARY FOR ([A-Z]+ \d{1,2} \d{4})", re.IGNORECASE
)
MAXIMUM_RE = re.compile(
    r"\b(TODAY|YESTERDAY)\s+MAXIMUM\s+(MM|-?\d+)\s*R?\b",
    re.IGNORECASE | re.DOTALL,
)
ISSUED_RE = re.compile(r"(\d{1,2}:\d{2}\s+[AP]M)\s+(?:CST|CDT)", re.IGNORECASE)


@dataclass(frozen=True)
class CliReport:
    climate_date: date
    maximum_f: int | None
    period_label: str
    product_id: str
    issued_utc: datetime | None
    issued_local_text: str
    is_correction: bool

    @property
    def selection_rank(self) -> tuple[int, datetime]:
        # The next-morning YESTERDAY report is the completed calendar-day product.
        # Corrections and later products supersede earlier versions.
        completed = int(self.period_label == "YESTERDAY")
        correction_bonus = int(self.is_correction)
        stamp = self.issued_utc or datetime.min.replace(tzinfo=timezone.utc)
        return completed * 10 + correction_bonus, stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare KAUS AUTO-METAR local daily highs with NWS CLIAUS reports."
    )
    parser.add_argument(
        "--metar-csv",
        type=Path,
        default=Path("outputs") / "KAUS_auto_daily_highs_local_2020_present.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs") / "KAUS_metar_vs_nws_cli_daily_comparison.csv",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("outputs") / "KAUS_metar_vs_nws_cli_summary.json",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("outputs") / "nws_cli_aus_archive",
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--pil", default="CLIAUS", help="NWS CLI product ID.")
    return parser.parse_args()


def download_year(year: int, cache_dir: Path, refresh: bool, pil: str) -> Path:
    target = cache_dir / f"{pil}_{year}.zip"
    if target.exists() and not refresh:
        return target
    params = urllib.parse.urlencode(
        {
            "pil": pil,
            "fmt": "zip",
            "sdate": f"{year}-01-01T00:00Z",
            "edate": f"{year + 1}-01-02T12:00Z",
            "limit": "9999",
            "order": "asc",
        }
    )
    request = urllib.request.Request(
        f"{IEM_RETRIEVE_URL}?{params}",
        headers={"User-Agent": "weatherbot-cli-comparison/1.0 (research use)"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            archive.testzip()
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"IEM did not return a valid ZIP for {year}") from exc
    target.write_bytes(payload)
    return target


def timestamp_from_product_id(product_id: str) -> datetime | None:
    match = re.search(r"(?<!\d)(\d{12})(?!\d)", product_id)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def parse_cli_product(product_id: str, text: str) -> CliReport | None:
    date_match = SUMMARY_DATE_RE.search(text)
    max_match = MAXIMUM_RE.search(text)
    if date_match is None or max_match is None:
        return None
    try:
        climate_date = datetime.strptime(
            date_match.group(1).upper(), "%B %d %Y"
        ).date()
    except ValueError:
        return None
    maximum_text = max_match.group(2).upper()
    maximum_f = None if maximum_text == "MM" else int(maximum_text)
    issued_match = ISSUED_RE.search(text)
    return CliReport(
        climate_date=climate_date,
        maximum_f=maximum_f,
        period_label=max_match.group(1).upper(),
        product_id=product_id,
        issued_utc=timestamp_from_product_id(product_id),
        issued_local_text=issued_match.group(1).upper() if issued_match else "",
        is_correction=bool(re.search(r"\bCORRECTED\b|(?:^|\s)COR\s*$", text, re.MULTILINE)),
    )


def load_cli_reports(zip_paths: list[Path]) -> tuple[dict[date, CliReport], int]:
    reports: dict[date, CliReport] = {}
    parsed_count = 0
    seen_products: set[str] = set()
    for path in zip_paths:
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                product_id = Path(member.filename).name
                if product_id in seen_products or member.is_dir():
                    continue
                seen_products.add(product_id)
                text = archive.read(member).decode("utf-8", errors="replace")
                report = parse_cli_product(product_id, text)
                if report is None:
                    continue
                parsed_count += 1
                previous = reports.get(report.climate_date)
                if previous is None or report.selection_rank > previous.selection_rank:
                    reports[report.climate_date] = report
    return reports, parsed_count


def load_metar_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    args = parse_args()
    metar_rows = load_metar_rows(args.metar_csv)
    available_dates = [
        date.fromisoformat(row["local_date"])
        for row in metar_rows
        if row.get("local_date")
    ]
    if not available_dates:
        raise SystemExit(f"No rows found in {args.metar_csv}")

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    # Include the following year because final YESTERDAY reports are issued next day.
    years = range(min(available_dates).year, max(available_dates).year + 2)
    archives = [
        download_year(year, args.cache_dir, args.refresh, args.pil) for year in years
    ]
    cli_by_date, parsed_product_count = load_cli_reports(archives)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "local_date",
        "metar_max_temp_f",
        "metar_max_temp_f_integer",
        "nws_cli_max_temp_f",
        "difference_f",
        "is_exact_match",
        "comparison_status",
        "metar_coverage_status",
        "nws_report_type",
        "nws_product_id",
        "nws_issued_utc",
        "first_max_local_time",
        "last_max_local_time",
        "auto_observation_count",
    ]
    counters = {
        "total_metar_calendar_days": len(metar_rows),
        "comparable_days": 0,
        "exact_matches": 0,
        "mismatches": 0,
        "nws_cli_missing": 0,
        "nws_cli_max_missing": 0,
        "metar_missing": 0,
        "complete_metar_comparable_days": 0,
        "complete_metar_exact_matches": 0,
        "complete_metar_mismatches": 0,
        "within_1f_days": 0,
        "complete_metar_within_1f_days": 0,
    }
    differences: dict[str, int] = {}
    complete_differences: dict[str, int] = {}
    output_rows: list[dict[str, object]] = []
    for metar in metar_rows:
        local_day = date.fromisoformat(metar["local_date"])
        cli = cli_by_date.get(local_day)
        metar_value = metar.get("max_temp_f_integer", "")
        coverage = metar.get("coverage_status", "")
        row: dict[str, object] = {
            "local_date": local_day.isoformat(),
            "metar_max_temp_f": metar.get("max_temp_f", ""),
            "metar_max_temp_f_integer": metar_value,
            "metar_coverage_status": coverage,
            "first_max_local_time": metar.get("first_max_local_time", ""),
            "last_max_local_time": metar.get("last_max_local_time", ""),
            "auto_observation_count": metar.get("auto_observation_count", ""),
        }
        if not metar_value:
            counters["metar_missing"] += 1
            row["comparison_status"] = "metar_missing"
        elif cli is None:
            counters["nws_cli_missing"] += 1
            row["comparison_status"] = "nws_cli_missing"
        else:
            row.update(
                {
                    "nws_cli_max_temp_f": "" if cli.maximum_f is None else cli.maximum_f,
                    "nws_report_type": cli.period_label,
                    "nws_product_id": cli.product_id,
                    "nws_issued_utc": (
                        cli.issued_utc.isoformat(timespec="minutes")
                        if cli.issued_utc
                        else ""
                    ),
                }
            )
            if cli.maximum_f is None:
                counters["nws_cli_max_missing"] += 1
                row["comparison_status"] = "nws_cli_max_missing"
            else:
                difference = int(metar_value) - cli.maximum_f
                exact = difference == 0
                counters["comparable_days"] += 1
                counters["exact_matches" if exact else "mismatches"] += 1
                differences[str(difference)] = differences.get(str(difference), 0) + 1
                if abs(difference) <= 1:
                    counters["within_1f_days"] += 1
                if coverage == "complete":
                    counters["complete_metar_comparable_days"] += 1
                    complete_differences[str(difference)] = (
                        complete_differences.get(str(difference), 0) + 1
                    )
                    if abs(difference) <= 1:
                        counters["complete_metar_within_1f_days"] += 1
                    counters[
                        "complete_metar_exact_matches"
                        if exact
                        else "complete_metar_mismatches"
                    ] += 1
                row.update(
                    {
                        "difference_f": difference,
                        "is_exact_match": str(exact).lower(),
                        "comparison_status": "match" if exact else "mismatch",
                    }
                )
        output_rows.append(row)

    with args.output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    comparable = counters["comparable_days"]
    complete_comparable = counters["complete_metar_comparable_days"]
    summary = {
        **counters,
        "exact_match_rate_percent": (
            round(counters["exact_matches"] * 100 / comparable, 4) if comparable else None
        ),
        "complete_metar_exact_match_rate_percent": (
            round(counters["complete_metar_exact_matches"] * 100 / complete_comparable, 4)
            if complete_comparable
            else None
        ),
        "within_1f_rate_percent": (
            round(counters["within_1f_days"] * 100 / comparable, 4)
            if comparable
            else None
        ),
        "complete_metar_within_1f_rate_percent": (
            round(counters["complete_metar_within_1f_days"] * 100 / complete_comparable, 4)
            if complete_comparable
            else None
        ),
        "difference_f_distribution": dict(
            sorted(differences.items(), key=lambda item: int(item[0]))
        ),
        "complete_metar_difference_f_distribution": dict(
            sorted(complete_differences.items(), key=lambda item: int(item[0]))
        ),
        "parsed_cli_product_versions": parsed_product_count,
        "selected_cli_daily_reports": len(cli_by_date),
        "comparison_start_date": min(available_dates).isoformat(),
        "comparison_end_date": max(available_dates).isoformat(),
        "nws_archive_source": IEM_RETRIEVE_URL,
        "selection_rule": (
            f"Prefer the latest YESTERDAY {args.pil} product for each climate date; "
            "otherwise use the latest TODAY product."
        ),
    }
    args.summary.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
