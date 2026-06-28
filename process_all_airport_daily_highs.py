from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


@dataclass(frozen=True)
class Airport:
    city: str
    station: str
    timezone: str
    issuedby: str

    @property
    def pil(self) -> str:
        return f"CLI{self.issuedby}"


AIRPORTS = [
    Airport("NYC", "KNYC", "America/New_York", "NYC"),
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
    Airport("New Orleans", "KMSY", "America/Chicago", "MSY"),
]

DIRTY_DATA_STATIONS = {"KDEN", "KLAX", "KHOU", "KMSY"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
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
    parser.add_argument("--daily-coverage-min-pct", type=float, default=90.0)
    parser.add_argument("--extrema-coverage-min-pct", type=float, default=80.0)
    parser.add_argument("--min-six-hour-groups", type=int, default=3)
    parser.add_argument("--weekly-high-outlier-f", type=float, default=20.0)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def run_station(
    airport: Airport, args: argparse.Namespace, raw_dir: Path
) -> dict[str, str]:
    raw_output = args.output_dir / "raw"
    cache_dir = args.nws_cache_root / airport.pil
    daily = raw_output / f"{airport.station}_daily_unfiltered.csv"
    comparison = raw_output / f"{airport.station}_vs_{airport.pil}_unfiltered.csv"
    summary = raw_output / f"{airport.station}_unfiltered_summary.json"
    command = [
        sys.executable,
        str(Path(__file__).with_name("compare_klax_6h_max_to_nws_cli.py")),
        "--station",
        airport.station,
        "--timezone",
        airport.timezone,
        "--pil",
        airport.pil,
        "--input-dir",
        str(raw_dir),
        "--start-year",
        str(args.start_year),
        "--include-instantaneous-max",
        "--daily-output",
        str(daily),
        "--comparison-output",
        str(comparison),
        "--summary",
        str(summary),
        "--cache-dir",
        str(cache_dir),
    ]
    completed = subprocess.run(
        command,
        cwd=Path(__file__).parent,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(
            f"{airport.station} failed: {completed.stderr or completed.stdout}"
        )
    return {
        "daily": str(daily),
        "comparison": str(comparison),
        "summary": str(summary),
    }


def expected_days(year: int, last_day: date) -> int:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), last_day)
    if end < start:
        return 0
    return (end - start).days + 1


def write_tagged_processed_rows(
    airport: Airport,
    raw_dir: Path,
    cleaned_daily: pd.DataFrame,
    output_path: Path,
    mismatch_dates: set[pd.Timestamp],
) -> tuple[int, int]:
    daily_high_by_date = {
        row.local_date.date(): float(row.daily_max_temp_f)
        for row in cleaned_daily.itertuples()
    }
    local_tz = ZoneInfo(airport.timezone)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    removed_mismatch_rows = 0
    with output_path.open("w", encoding="utf-8", newline="") as out_handle:
        writer = csv.writer(out_handle)
        writer.writerow(["daily_high_f", "station", "valid", "metar"])
        for source_path in sorted(
            raw_dir.glob(f"{airport.station}_*_metar.csv")
        ):
            with source_path.open(
                "r", encoding="utf-8", newline=""
            ) as in_handle:
                for row in csv.DictReader(in_handle):
                    metar = row.get("metar", "")
                    if " AUTO " in f" {metar} ":
                        continue
                    try:
                        valid_utc = datetime.strptime(
                            row["valid"], "%Y-%m-%d %H:%M"
                        ).replace(tzinfo=timezone.utc)
                    except (KeyError, ValueError):
                        continue
                    local_date = valid_utc.astimezone(local_tz).date()
                    daily_high_f = daily_high_by_date.get(local_date)
                    if daily_high_f is None:
                        continue
                    if pd.Timestamp(local_date) in mismatch_dates:
                        removed_mismatch_rows += 1
                        continue
                    writer.writerow(
                        [
                            f"{daily_high_f:.1f}",
                            row.get("station", ""),
                            row["valid"],
                            metar,
                        ]
                    )
                    written += 1
    return written, removed_mismatch_rows


def clean_station(
    airport: Airport,
    raw_dir: Path,
    paths: dict[str, str],
    args: argparse.Namespace,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    daily = pd.read_csv(paths["daily"])
    comparison = pd.read_csv(paths["comparison"])
    daily["local_date"] = pd.to_datetime(daily["local_date"])
    comparison["local_date"] = pd.to_datetime(comparison["local_date"])
    last_day = daily["local_date"].max().date()
    daily["year"] = daily["local_date"].dt.year

    year_audit: list[dict[str, object]] = []
    removed_years: set[int] = set()
    for year in range(args.start_year, last_day.year + 1):
        rows = daily[daily["year"] == year]
        expected = expected_days(year, last_day)
        observed = len(rows)
        daily_pct = observed * 100.0 / expected if expected else 0.0
        extrema_days = int(
            (rows["six_hour_group_count"] >= args.min_six_hour_groups).sum()
        )
        extrema_pct = extrema_days * 100.0 / observed if observed else 0.0
        reasons = []
        if daily_pct < args.daily_coverage_min_pct:
            reasons.append("daily_coverage_below_threshold")
        if extrema_pct < args.extrema_coverage_min_pct:
            reasons.append("six_hour_extrema_coverage_below_threshold")
        removed = bool(reasons)
        if removed:
            removed_years.add(year)
        year_audit.append(
            {
                "city": airport.city,
                "station": airport.station,
                "nws_product": airport.pil,
                "year": year,
                "expected_days": expected,
                "daily_label_days": observed,
                "daily_coverage_pct": round(daily_pct, 4),
                "days_with_min_six_hour_groups": extrema_days,
                "extrema_coverage_pct": round(extrema_pct, 4),
                "removed": removed,
                "removal_reason": "|".join(reasons),
            }
        )

    candidate = daily[~daily["year"].isin(removed_years)].copy()
    iso = candidate["local_date"].dt.isocalendar()
    candidate["iso_year"] = iso.year.astype(int)
    candidate["iso_week"] = iso.week.astype(int)
    group_keys = ["iso_year", "iso_week"]
    weekly_sum = candidate.groupby(group_keys)["daily_max_temp_f"].transform("sum")
    weekly_count = candidate.groupby(group_keys)["daily_max_temp_f"].transform(
        "count"
    )
    candidate["peer_week_mean_f"] = (
        weekly_sum - candidate["daily_max_temp_f"]
    ) / (weekly_count - 1)
    candidate["above_peer_week_mean_f"] = (
        candidate["daily_max_temp_f"] - candidate["peer_week_mean_f"]
    )
    anomaly_mask = (weekly_count >= 4) & (
        candidate["above_peer_week_mean_f"] > args.weekly_high_outlier_f
    )
    anomalies = candidate[anomaly_mask].copy()
    anomaly_audit = [
        {
            "city": airport.city,
            "station": airport.station,
            "local_date": row.local_date.date().isoformat(),
            "daily_max_temp_f": row.daily_max_temp_f,
            "peer_week_mean_f": round(row.peer_week_mean_f, 4),
            "above_peer_week_mean_f": round(row.above_peer_week_mean_f, 4),
            "removal_reason": "daily_high_gt_peer_week_mean_plus_threshold",
            "source_metar": row.source_metar,
        }
        for row in anomalies.itertuples()
    ]
    cleaned = candidate[~anomaly_mask].copy()
    keep_dates = set(cleaned["local_date"])
    cleaned_comparison = comparison[
        comparison["local_date"].isin(keep_dates)
    ].copy()
    mismatch_mask = cleaned_comparison["difference_f"].notna() & (
        cleaned_comparison["difference_f"] != 0
    )
    mismatch_comparison = cleaned_comparison[mismatch_mask].copy()
    mismatch_dates = set(mismatch_comparison["local_date"])
    processed_daily = cleaned[
        ~cleaned["local_date"].isin(mismatch_dates)
    ].copy()
    mismatch_audit = [
        {
            "city": airport.city,
            "station": airport.station,
            "dirty_data_station": airport.station in DIRTY_DATA_STATIONS,
            "local_date": row.local_date.date().isoformat(),
            "metar_daily_high_f": row.station_daily_maximum_f,
            "nws_daily_high_f": row.nws_cli_maximum_f,
            "difference_f": row.difference_f,
            "removal_reason": "metar_daily_high_not_equal_to_nws",
        }
        for row in mismatch_comparison.itertuples()
    ]

    processed_dir = args.output_dir / "processed"
    comparison_dir = args.output_dir / "comparisons"
    processed_dir.mkdir(parents=True, exist_ok=True)
    comparison_dir.mkdir(parents=True, exist_ok=True)
    processed_daily_path = (
        processed_dir / f"{airport.station}_daily_high_cleaned.csv"
    )
    processed_daily.drop(
        columns=[
            "year",
            "iso_year",
            "iso_week",
            "peer_week_mean_f",
            "above_peer_week_mean_f",
        ],
        errors="ignore",
    ).to_csv(
        processed_daily_path,
        index=False,
        encoding="utf-8-sig",
    )
    cleaned_comparison.to_csv(
        comparison_dir / f"{airport.station}_vs_{airport.pil}_cleaned.csv",
        index=False,
        encoding="utf-8-sig",
    )
    tagged_output = (
        processed_dir
        / f"{airport.station}_local_0000_2359_daily_high.csv"
    )
    tagged_rows, removed_mismatch_rows = write_tagged_processed_rows(
        airport=airport,
        raw_dir=raw_dir,
        cleaned_daily=cleaned,
        output_path=tagged_output,
        mismatch_dates=mismatch_dates,
    )
    excluded_from_training = airport.station in DIRTY_DATA_STATIONS
    if excluded_from_training:
        processed_daily_path.unlink(missing_ok=True)
        tagged_output.unlink(missing_ok=True)
        tagged_rows = 0

    comparable = cleaned_comparison[
        cleaned_comparison["difference_f"].notna()
    ].copy()
    exact = int((comparable["difference_f"] == 0).sum())
    within1 = int((comparable["difference_f"].abs() <= 1).sum())
    metrics = {
        "city": airport.city,
        "station": airport.station,
        "timezone": airport.timezone,
        "nws_product": airport.pil,
        "dirty_data": excluded_from_training,
        "excluded_from_training": excluded_from_training,
        "first_clean_date": processed_daily["local_date"].min().date().isoformat()
        if len(processed_daily)
        else "",
        "last_clean_date": processed_daily["local_date"].max().date().isoformat()
        if len(processed_daily)
        else "",
        "clean_daily_days": 0 if excluded_from_training else len(processed_daily),
        "removed_nws_mismatch_days": len(mismatch_dates),
        "removed_nws_mismatch_metar_rows": removed_mismatch_rows,
        "tagged_processed_rows": tagged_rows,
        "tagged_processed_file": str(tagged_output),
        "removed_year_count": len(removed_years),
        "removed_years": ",".join(map(str, sorted(removed_years))),
        "removed_anomaly_days": len(anomalies),
        "nws_comparable_days": len(comparable),
        "exact_matches": exact,
        "exact_match_rate_pct": round(exact * 100 / len(comparable), 4)
        if len(comparable)
        else None,
        "within_1f_days": within1,
        "within_1f_rate_pct": round(within1 * 100 / len(comparable), 4)
        if len(comparable)
        else None,
        "mae_f": round(comparable["difference_f"].abs().mean(), 6)
        if len(comparable)
        else None,
        "bias_f": round(comparable["difference_f"].mean(), 6)
        if len(comparable)
        else None,
        "nws_report_missing_days": int(
            (cleaned_comparison["status"] == "nws_report_missing").sum()
        ),
    }
    return metrics, year_audit, anomaly_audit, mismatch_audit


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
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "raw").mkdir(parents=True, exist_ok=True)
    available: list[tuple[Airport, Path]] = []
    missing: list[str] = []
    for airport in AIRPORTS:
        raw_dir = args.raw_root / airport.station
        if raw_dir.exists() and any(raw_dir.glob(f"{airport.station}_*_metar.csv")):
            available.append((airport, raw_dir))
        else:
            missing.append(airport.station)
    if missing:
        raise SystemExit(f"Missing raw station data: {', '.join(missing)}")

    generated: dict[str, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(run_station, airport, args, raw_dir): airport
            for airport, raw_dir in available
        }
        for future in as_completed(futures):
            airport = futures[future]
            generated[airport.station] = future.result()
            print(f"Generated unfiltered daily/NWS data: {airport.station}")

    metrics_rows: list[dict[str, object]] = []
    year_rows: list[dict[str, object]] = []
    anomaly_rows: list[dict[str, object]] = []
    mismatch_rows: list[dict[str, object]] = []
    for airport, raw_dir in available:
        metrics, years, anomalies, mismatches = clean_station(
            airport, raw_dir, generated[airport.station], args
        )
        metrics_rows.append(metrics)
        year_rows.extend(years)
        anomaly_rows.extend(anomalies)
        mismatch_rows.extend(mismatches)

    write_csv(args.output_dir / "station_summary.csv", metrics_rows)
    write_csv(args.output_dir / "year_coverage_audit.csv", year_rows)
    write_csv(args.output_dir / "removed_anomaly_days.csv", anomaly_rows)
    write_csv(
        args.output_dir / "removed_nws_mismatch_days.csv", mismatch_rows
    )
    config = {
        "raw_root": str(args.raw_root),
        "nws_cache_root": str(args.nws_cache_root),
        "start_year": args.start_year,
        "daily_coverage_min_pct": args.daily_coverage_min_pct,
        "extrema_coverage_min_pct": args.extrema_coverage_min_pct,
        "min_six_hour_groups": args.min_six_hour_groups,
        "weekly_high_outlier_f": args.weekly_high_outlier_f,
        "dirty_data_stations": sorted(DIRTY_DATA_STATIONS),
        "training_exclusion_policy": (
            "dirty_data_stations have no files in the processed directory"
        ),
        "nws_mismatch_policy": (
            "remove days with both values present and unequal; "
            "retain days where NWS report is missing"
        ),
        "stations": [airport.station for airport, _ in available],
    }
    (args.output_dir / "processing_config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(config, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
