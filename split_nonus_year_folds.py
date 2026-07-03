from __future__ import annotations

import csv
import json
from datetime import date
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT_FILE = (
    ROOT
    / "NONUS"
    / "features_halfhour"
    / "all_nonus_halfhour_numeric_features.csv"
)
PREPROCESS_SUMMARY = ROOT / "NONUS" / "preprocess_summary.json"
OUTPUT_DIR = ROOT / "NONUS" / "year_folds_6_to_1"
FIRST_VALIDATION_YEAR = 2026
TRAINING_WINDOW_YEARS = 6
MIN_HALF_HOUR_ROW_RATIO = 0.95


def build_folds(years: list[int]) -> list[dict]:
    available = set(years)
    folds = []
    anchor = FIRST_VALIDATION_YEAR
    while available:
        validation_candidates = [year for year in available if year <= anchor]
        if not validation_candidates:
            break
        validation_year = max(validation_candidates)
        training_years = [
            year
            for year in range(
                validation_year - TRAINING_WINDOW_YEARS, validation_year
            )
            if year in available
        ]
        if training_years:
            folds.append(
                {
                    "validation_year": validation_year,
                    "training_years": training_years,
                }
            )
        anchor = validation_year - (TRAINING_WINDOW_YEARS + 1)
    return folds


def main() -> int:
    if not INPUT_FILE.exists():
        raise SystemExit(f"Feature file does not exist: {INPUT_FILE}")
    summary = json.loads(PREPROCESS_SUMMARY.read_text(encoding="utf-8"))
    row_counts: Counter = Counter()
    latest_dates: dict[tuple[str, int], date] = {}
    with INPUT_FILE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            station = row["station"].upper()
            year = int(row["local_year"])
            key = (station, year)
            row_counts[key] += 1
            local_date = date(
                year, int(row["local_month"]), int(row["local_day"])
            )
            previous = latest_dates.get(key)
            if previous is None or local_date > previous:
                latest_dates[key] = local_date

    usable_years: dict[str, list[int]] = {}
    rejected_years: dict[str, list[dict]] = {}
    for station, values in summary.items():
        if station.startswith("_"):
            continue
        usable = []
        rejected = []
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
            expected_rows = eligible_days * 48
            ratio = rows / expected_rows if expected_rows else 0.0
            if ratio >= MIN_HALF_HOUR_ROW_RATIO:
                usable.append(year)
            else:
                rejected.append(
                    {
                        "year": year,
                        "feature_rows": rows,
                        "expected_half_hour_rows": expected_rows,
                        "row_ratio": round(ratio, 6),
                    }
                )
        usable_years[station] = usable
        rejected_years[station] = rejected

    station_folds = {
        station: build_folds(years)
        for station, years in usable_years.items()
    }

    assignments = {}
    for station, folds in station_folds.items():
        for fold_index, fold in enumerate(folds, start=1):
            fold_id = f"{station}_val_{fold['validation_year']}"
            assignments[(station, fold["validation_year"])] = (
                "validation",
                fold_id,
                fold["validation_year"],
            )
            for year in fold["training_years"]:
                assignments[(station, year)] = (
                    "train",
                    fold_id,
                    fold["validation_year"],
                )
            fold["fold_id"] = fold_id
            fold["fold_index"] = fold_index

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    train_path = OUTPUT_DIR / "nonus_train_year_folds.csv"
    validation_path = OUTPUT_DIR / "nonus_validation_year_folds.csv"
    counts: Counter = Counter()

    with (
        INPUT_FILE.open("r", encoding="utf-8", newline="") as source_handle,
        train_path.open("w", encoding="utf-8", newline="") as train_handle,
        validation_path.open(
            "w", encoding="utf-8", newline=""
        ) as validation_handle,
    ):
        reader = csv.DictReader(source_handle)
        if not reader.fieldnames or "local_year" not in reader.fieldnames:
            raise SystemExit("Input feature file is missing local_year")
        output_fields = [
            "fold_id",
            "validation_year",
            "split",
            *reader.fieldnames,
        ]
        train_writer = csv.DictWriter(
            train_handle, fieldnames=output_fields, lineterminator="\n"
        )
        validation_writer = csv.DictWriter(
            validation_handle, fieldnames=output_fields, lineterminator="\n"
        )
        train_writer.writeheader()
        validation_writer.writeheader()

        for row in reader:
            station = row["station"].upper()
            year = int(row["local_year"])
            assignment = assignments.get((station, year))
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
            if split == "train":
                train_writer.writerow(output)
            else:
                validation_writer.writerow(output)
            counts[f"{fold_id}_{split}_rows"] += 1
            counts[f"{split}_rows"] += 1

    for station, folds in station_folds.items():
        for fold in folds:
            fold_id = fold["fold_id"]
            fold["train_rows"] = counts[f"{fold_id}_train_rows"]
            fold["validation_rows"] = counts[f"{fold_id}_validation_rows"]

    manifest = {
        "rule": {
            "first_validation_year": FIRST_VALIDATION_YEAR,
            "training_window_years": TRAINING_WINDOW_YEARS,
            "missing_training_years": "use available years inside the six-calendar-year window",
            "missing_validation_year": "move backward to the nearest available year",
            "next_anchor": "selected validation year minus seven",
            "random_split": False,
            "model_training_performed": False,
            "minimum_half_hour_feature_row_ratio": MIN_HALF_HOUR_ROW_RATIO,
        },
        "totals": dict(counts),
        "stations": station_folds,
        "usable_years": usable_years,
        "rejected_sparse_years": rejected_years,
        "train_file": str(train_path),
        "validation_file": str(validation_path),
    }
    manifest_path = OUTPUT_DIR / "fold_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        f"Train rows: {counts['train_rows']}; "
        f"validation rows: {counts['validation_rows']}; "
        f"unassigned rows: {counts['unassigned_rows']}"
    )
    print(f"Train: {train_path}")
    print(f"Validation: {validation_path}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
