from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path

from split_nonus_year_folds import build_folds


ROOT = Path(__file__).resolve().parent
INPUT_FILE = (
    ROOT / "NONUS" / "newC" / "features_halfhour"
    / "all_nonus_halfhour_numeric_features_c.csv"
)
PREPROCESS_SUMMARY = ROOT / "NONUS" / "newC" / "preprocess_summary.json"
OUTPUT_DIR = ROOT / "NONUS" / "newC" / "year_folds_6_to_1"
FIRST_VALIDATION_YEAR = 2026
MIN_HALF_HOUR_ROW_RATIO = 0.95


def main() -> int:
    if not INPUT_FILE.exists():
        raise SystemExit(f"Missing Celsius feature file: {INPUT_FILE}")
    if OUTPUT_DIR.exists():
        raise SystemExit(f"Output directory already exists: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True)

    summary = json.loads(PREPROCESS_SUMMARY.read_text(encoding="utf-8"))
    row_counts: Counter = Counter()
    latest_dates: dict[tuple[str, int], date] = {}
    with INPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station = row["station"].upper()
            year = int(row["local_year"])
            key = (station, year)
            row_counts[key] += 1
            local_date = date(
                year, int(row["local_month"]), int(row["local_day"])
            )
            if key not in latest_dates or local_date > latest_dates[key]:
                latest_dates[key] = local_date

    usable_years = {}
    rejected_years = {}
    for station, values in summary.items():
        if station.startswith("_"):
            continue
        usable, rejected = [], []
        for raw_year in values["qualified_years"]:
            year = int(raw_year)
            key = (station, year)
            rows = row_counts[key]
            if year == FIRST_VALIDATION_YEAR and key in latest_dates:
                eligible_days = (
                    latest_dates[key] - date(year, 1, 1)
                ).days + 1
            else:
                eligible_days = (date(year + 1, 1, 1) - date(year, 1, 1)).days
            expected = eligible_days * 48
            ratio = rows / expected if expected else 0.0
            if ratio >= MIN_HALF_HOUR_ROW_RATIO:
                usable.append(year)
            else:
                rejected.append(
                    {
                        "year": year,
                        "feature_rows": rows,
                        "expected_half_hour_rows": expected,
                        "row_ratio": round(ratio, 6),
                    }
                )
        usable_years[station] = usable
        rejected_years[station] = rejected

    station_folds = {
        station: build_folds(years) for station, years in usable_years.items()
    }
    assignments = {}
    for station, folds in station_folds.items():
        for index, fold in enumerate(folds, start=1):
            validation_year = int(fold["validation_year"])
            fold_id = f"{station}_val_{validation_year}"
            fold["fold_id"] = fold_id
            fold["fold_index"] = index
            assignments[(station, validation_year)] = (
                "validation", fold_id, validation_year
            )
            for year in fold["training_years"]:
                assignments[(station, int(year))] = (
                    "train", fold_id, validation_year
                )

    train_path = OUTPUT_DIR / "nonus_celsius_train_year_folds.csv"
    validation_path = OUTPUT_DIR / "nonus_celsius_validation_year_folds.csv"
    counts: Counter = Counter()
    with (
        INPUT_FILE.open("r", encoding="utf-8", newline="") as source_handle,
        train_path.open("w", encoding="utf-8", newline="") as train_handle,
        validation_path.open("w", encoding="utf-8", newline="") as valid_handle,
    ):
        reader = csv.DictReader(source_handle)
        fields = ["fold_id", "validation_year", "split", *reader.fieldnames]
        train_writer = csv.DictWriter(
            train_handle, fieldnames=fields, lineterminator="\n"
        )
        valid_writer = csv.DictWriter(
            valid_handle, fieldnames=fields, lineterminator="\n"
        )
        train_writer.writeheader()
        valid_writer.writeheader()
        for row in reader:
            key = (row["station"].upper(), int(row["local_year"]))
            assignment = assignments.get(key)
            if assignment is None:
                counts["unassigned_rows"] += 1
                continue
            split, fold_id, validation_year = assignment
            output = {
                "fold_id": fold_id,
                "validation_year": validation_year,
                "split": split,
                **row,
            }
            (train_writer if split == "train" else valid_writer).writerow(output)
            counts[f"{fold_id}_{split}_rows"] += 1
            counts[f"{split}_rows"] += 1

    for folds in station_folds.values():
        for fold in folds:
            fold["train_rows"] = counts[f"{fold['fold_id']}_train_rows"]
            fold["validation_rows"] = counts[
                f"{fold['fold_id']}_validation_rows"
            ]

    manifest = {
        "target": "daily_high_c",
        "temperature_unit": "Celsius",
        "rule": {
            "first_validation_year": FIRST_VALIDATION_YEAR,
            "training_window_years": 6,
            "missing_training_years": "use available years in window",
            "missing_validation_year": "move backward to nearest available",
            "minimum_half_hour_feature_row_ratio": MIN_HALF_HOUR_ROW_RATIO,
        },
        "totals": dict(counts),
        "usable_years": usable_years,
        "rejected_sparse_years": rejected_years,
        "stations": station_folds,
        "train_file": str(train_path),
        "validation_file": str(validation_path),
    }
    manifest_path = OUTPUT_DIR / "fold_manifest_celsius.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        f"Train rows: {counts['train_rows']}; "
        f"validation rows: {counts['validation_rows']}; "
        f"unassigned rows: {counts['unassigned_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
