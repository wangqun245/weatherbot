from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from compare_kaus_metar_to_nws_cli import download_year, load_cli_reports
from compare_klax_6h_max_to_nws_cli import obvious_temperature_error


@dataclass(frozen=True)
class Airport:
    city: str
    station: str
    timezone: str
    issuedby: str

    @property
    def pil(self) -> str:
        return f"CLI{self.issuedby}"


@dataclass(frozen=True)
class MetarRow:
    station: str
    valid_text: str
    valid_utc: datetime
    metar: str
    climate_date: date


AIRPORTS = [
    Airport("Chicago", "KMDW", "America/Chicago", "MDW"),
    Airport("Miami", "KMIA", "America/New_York", "MIA"),
    Airport("Austin", "KAUS", "America/Chicago", "AUS"),
    Airport("Los Angeles", "KLAX", "America/Los_Angeles", "LAX"),
    Airport("Denver", "KDEN", "America/Denver", "DEN"),
    Airport("Phoenix", "KPHX", "America/Phoenix", "PHX"),
    Airport("Philadelphia", "KPHL", "America/New_York", "PHL"),
    Airport("Houston", "KHOU", "America/Chicago", "HOU"),
    Airport("Minneapolis", "KMSP", "America/Chicago", "MSP"),
    Airport("Oklahoma City", "KOKC", "America/Chicago", "OKC"),
    Airport("San Francisco", "KSFO", "America/Los_Angeles", "SFO"),
    Airport("Washington DC", "KDCA", "America/New_York", "DCA"),
    Airport("Boston", "KBOS", "America/New_York", "BOS"),
    Airport("Dallas", "KDFW", "America/Chicago", "DFW"),
    Airport("Seattle", "KSEA", "America/Los_Angeles", "SEA"),
    Airport("Las Vegas", "KLAS", "America/Los_Angeles", "LAS"),
    Airport("Atlanta", "KATL", "America/New_York", "ATL"),
    Airport("San Antonio", "KSAT", "America/Chicago", "SAT"),
]

STANDARD_UTC_OFFSETS = {
    "America/New_York": -5,
    "America/Chicago": -6,
    "America/Denver": -7,
    "America/Phoenix": -7,
    "America/Los_Angeles": -8,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Kalshi training rows using strict NWS CLI YESTERDAY "
            "maximum temperatures as labels and fixed-LST climate days."
        )
    )
    parser.add_argument(
        "--raw-root", type=Path, default=Path(r"C:\weather\metar_history")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("kalshi") / "data" / "all_airports_daily_high",
    )
    parser.add_argument(
        "--nws-cache-root",
        type=Path,
        default=Path("outputs")
        / "all_airports_daily_high_nws"
        / "nws_cache",
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--year-coverage-min-pct", type=float, default=95.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--refresh-nws", action="store_true")
    return parser.parse_args()


def expected_days(year: int, last_day: date) -> int:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), last_day)
    return max(0, (end - start).days + 1)


def station_files(station_dir: Path, station: str) -> list[Path]:
    return sorted(station_dir.glob(f"{station}_*_metar.csv"))


def read_station_rows(
    airport: Airport, station_dir: Path, start_year: int
) -> tuple[list[MetarRow], set[date], Counter]:
    offset = timezone(
        timedelta(hours=STANDARD_UTC_OFFSETS[airport.timezone])
    )
    rows: list[MetarRow] = []
    dirty_days: set[date] = set()
    stats: Counter = Counter()
    seen: set[tuple[str, str]] = set()

    for source_path in station_files(station_dir, airport.station):
        with source_path.open("r", encoding="utf-8", newline="") as handle:
            for source in csv.DictReader(handle):
                stats["input_rows"] += 1
                valid_text = source.get("valid", "")
                metar = source.get("metar", "")
                try:
                    valid_utc = datetime.strptime(
                        valid_text, "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    stats["invalid_timestamp_rows"] += 1
                    continue
                climate_date = valid_utc.astimezone(offset).date()
                if climate_date.year < start_year:
                    continue

                key = (valid_text, metar)
                if key in seen:
                    stats["duplicate_rows"] += 1
                    continue
                seen.add(key)

                if obvious_temperature_error(metar):
                    dirty_days.add(climate_date)
                    stats["obvious_dirty_rows"] += 1

                if " AUTO " in f" {metar} ":
                    stats["auto_rows_removed"] += 1
                    continue

                rows.append(
                    MetarRow(
                        station=source.get("station", airport.station),
                        valid_text=valid_text,
                        valid_utc=valid_utc,
                        metar=metar,
                        climate_date=climate_date,
                    )
                )

    rows.sort(key=lambda row: row.valid_utc)
    return rows, dirty_days, stats


def process_station(
    airport: Airport, args: argparse.Namespace
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    station_dir = args.raw_root / airport.station
    rows, dirty_days, stats = read_station_rows(
        airport, station_dir, args.start_year
    )
    if not rows:
        raise RuntimeError(f"No usable METAR rows for {airport.station}")

    last_day = max(row.climate_date for row in rows)
    observed_days_by_year: dict[int, set[date]] = defaultdict(set)
    for row in rows:
        observed_days_by_year[row.climate_date.year].add(row.climate_date)

    retained_years: set[int] = set()
    year_audit: list[dict[str, object]] = []
    for year in range(args.start_year, last_day.year + 1):
        expected = expected_days(year, last_day)
        observed = len(observed_days_by_year.get(year, set()))
        coverage = observed * 100.0 / expected if expected else 0.0
        retained = coverage >= args.year_coverage_min_pct
        if retained:
            retained_years.add(year)
        year_audit.append(
            {
                "city": airport.city,
                "station": airport.station,
                "year": year,
                "expected_calendar_days": expected,
                "days_with_usable_metar": observed,
                "coverage_pct": round(coverage, 4),
                "retained": retained,
                "exclusion_reason": (
                    ""
                    if retained
                    else "metar_calendar_day_coverage_below_95pct"
                ),
            }
        )

    cache_dir = args.nws_cache_root / airport.pil
    cache_dir.mkdir(parents=True, exist_ok=True)
    archives = [
        download_year(year, cache_dir, args.refresh_nws, airport.pil)
        for year in range(args.start_year, last_day.year + 2)
    ]
    reports, parsed_versions = load_cli_reports(archives)
    labels = {
        climate_date: report.maximum_f
        for climate_date, report in reports.items()
        if report.period_label == "YESTERDAY"
        and report.maximum_f is not None
    }

    processed_dir = args.output_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = (
        processed_dir
        / f"{airport.station}_local_0000_2359_daily_high.csv"
    )
    output_rows = 0
    skipped_dirty_rows = 0
    skipped_missing_label_rows = 0
    output_days: set[date] = set()
    missing_label_days: set[date] = set()
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["daily_high_f", "station", "valid", "metar"])
        for row in rows:
            if row.climate_date.year not in retained_years:
                continue
            if row.climate_date in dirty_days:
                skipped_dirty_rows += 1
                continue
            label = labels.get(row.climate_date)
            if label is None:
                skipped_missing_label_rows += 1
                missing_label_days.add(row.climate_date)
                continue
            writer.writerow(
                [f"{float(label):.1f}", row.station, row.valid_text, row.metar]
            )
            output_rows += 1
            output_days.add(row.climate_date)

    dirty_audit = [
        {
            "city": airport.city,
            "station": airport.station,
            "climate_date_lst": day.isoformat(),
            "removal_reason": "day_contains_obviously_dirty_metar",
        }
        for day in sorted(dirty_days)
    ]
    summary = {
        "city": airport.city,
        "station": airport.station,
        "timezone": airport.timezone,
        "nws_product": airport.pil,
        "lst_utc_offset_hours": STANDARD_UTC_OFFSETS[airport.timezone],
        "first_output_climate_date": (
            min(output_days).isoformat() if output_days else ""
        ),
        "last_output_climate_date": (
            max(output_days).isoformat() if output_days else ""
        ),
        "retained_year_count": len(retained_years),
        "retained_years": ",".join(map(str, sorted(retained_years))),
        "excluded_year_count": len(year_audit) - len(retained_years),
        "dirty_climate_days_removed": len(
            dirty_days & set().union(*observed_days_by_year.values())
        ),
        "metar_rows_removed_with_dirty_days": skipped_dirty_rows,
        "nws_label_missing_days": len(missing_label_days),
        "metar_rows_skipped_without_nws_label": skipped_missing_label_rows,
        "nws_cli_versions_parsed": parsed_versions,
        "output_climate_days": len(output_days),
        "output_rows": output_rows,
        "output_file": str(output_path),
        **dict(stats),
    }
    return summary, year_audit, dirty_audit


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    if not 0 < args.year_coverage_min_pct <= 100:
        raise SystemExit("--year-coverage-min-pct must be in (0, 100]")

    missing = [
        airport.station
        for airport in AIRPORTS
        if not (args.raw_root / airport.station).exists()
    ]
    if missing:
        raise SystemExit(f"Missing raw station data: {', '.join(missing)}")

    summaries: list[dict[str, object]] = []
    year_rows: list[dict[str, object]] = []
    dirty_rows: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(process_station, airport, args): airport
            for airport in AIRPORTS
        }
        for future in as_completed(futures):
            airport = futures[future]
            summary, years, dirty = future.result()
            summaries.append(summary)
            year_rows.extend(years)
            dirty_rows.extend(dirty)
            print(
                f"{airport.station}: {summary['output_rows']} rows, "
                f"{summary['output_climate_days']} labeled climate days"
            )

    order = {airport.station: index for index, airport in enumerate(AIRPORTS)}
    summaries.sort(key=lambda row: order[str(row["station"])])
    year_rows.sort(key=lambda row: (order[str(row["station"])], row["year"]))
    dirty_rows.sort(
        key=lambda row: (
            order[str(row["station"])],
            row["climate_date_lst"],
        )
    )
    write_csv(args.output_dir / "station_summary.csv", summaries)
    write_csv(args.output_dir / "year_coverage_audit.csv", year_rows)
    write_csv(args.output_dir / "removed_dirty_climate_days.csv", dirty_rows)

    config = {
        "raw_root": str(args.raw_root),
        "nws_cache_root": str(args.nws_cache_root),
        "start_year": args.start_year,
        "year_coverage_min_pct": args.year_coverage_min_pct,
        "year_coverage_definition": (
            "fixed-LST calendar days containing at least one valid non-AUTO "
            "METAR divided by expected calendar days through the last raw day"
        ),
        "climate_day_definition": (
            "00:00-23:59 fixed Local Standard Time using the station's "
            "standard UTC offset"
        ),
        "label_definition": (
            "strict next-morning NWS CLI YESTERDAY MAXIMUM; no METAR-derived "
            "daily high"
        ),
        "dirty_day_policy": (
            "remove the entire fixed-LST climate day when any METAR has a "
            "body/T-group difference greater than 1C or temperature/extrema "
            "outside -60C..52C"
        ),
        "output_format": "daily_high_f,station,valid_utc,metar",
        "stations": [airport.station for airport in AIRPORTS],
    }
    (args.output_dir / "processing_config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(config, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
