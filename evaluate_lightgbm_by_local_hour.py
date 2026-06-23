from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from train_lightgbm_metar_high import add_calendar_variants


DEFAULT_FEATURE_FILE = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed"
    r"\all_stations_local_0900_1900_daily_high_features.csv"
)
DEFAULT_MODEL_FILE = Path(r"C:\Users\Jack\Documents\git\weatherbot\models\lightgbm_metar_high_best.pkl")
DEFAULT_METRICS_FILE = Path(r"C:\Users\Jack\Documents\git\weatherbot\models\lightgbm_metar_high_metrics.json")
DEFAULT_OUTPUT_DIR = Path(r"C:\Users\Jack\Documents\git\weatherbot\models\hourly_validation")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate saved LightGBM daily-high model by airport local observation hour."
    )
    parser.add_argument("--feature-file", type=Path, default=DEFAULT_FEATURE_FILE)
    parser.add_argument("--model-file", type=Path, default=DEFAULT_MODEL_FILE)
    parser.add_argument("--metrics-file", type=Path, default=DEFAULT_METRICS_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--prediction-sample-rows", type=int, default=200000)
    return parser.parse_args()


def rmse(values: pd.Series) -> float:
    return float(np.sqrt(np.mean(np.square(values.to_numpy(dtype="float64")))))


def summarize(group: pd.core.groupby.generic.DataFrameGroupBy) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, part in group:
        abs_error = part["abs_error_f"]
        signed_error = part["error_f"]
        actual = part["actual_high_f"]
        pred = part["predicted_high_f"]
        if not isinstance(key, tuple):
            key = (key,)

        row: dict[str, object] = {
            "count": int(len(part)),
            "actual_mean_f": float(actual.mean()),
            "prediction_mean_f": float(pred.mean()),
            "bias_mean_f": float(signed_error.mean()),
            "mae_f": float(abs_error.mean()),
            "rmse_f": rmse(signed_error),
            "median_abs_error_f": float(abs_error.median()),
            "p75_abs_error_f": float(abs_error.quantile(0.75)),
            "p90_abs_error_f": float(abs_error.quantile(0.90)),
            "p95_abs_error_f": float(abs_error.quantile(0.95)),
            "within_1f_pct": float((abs_error <= 1.0).mean() * 100.0),
            "within_2f_pct": float((abs_error <= 2.0).mean() * 100.0),
            "within_3f_pct": float((abs_error <= 3.0).mean() * 100.0),
            "under_by_more_than_3f_pct": float((signed_error < -3.0).mean() * 100.0),
            "over_by_more_than_3f_pct": float((signed_error > 3.0).mean() * 100.0),
        }
        rows.append(row | {f"group_{idx}": value for idx, value in enumerate(key)})
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    with args.metrics_file.open("r", encoding="utf-8") as handle:
        metrics = json.load(handle)
    cutoff_year = int(metrics["validation_cutoff_year"])

    print(f"Loading feature file: {args.feature_file}")
    df = pd.read_csv(args.feature_file, low_memory=False)
    df = add_calendar_variants(df)
    validation = df[df["local_year"] >= cutoff_year].copy()
    print(f"Validation rows: {len(validation):,}; validation years >= {cutoff_year}")

    model = joblib.load(args.model_file)
    feature_columns = list(model.feature_name_)
    X = validation[feature_columns].apply(pd.to_numeric, errors="coerce").astype("float32")
    y = pd.to_numeric(validation["daily_high_f"], errors="coerce").astype("float32")
    predictions = model.predict(X, num_iteration=model.best_iteration_)

    evaluation = validation[["station", "valid", "metar", "local_year", "local_month", "local_day", "local_hour"]].copy()
    evaluation["actual_high_f"] = y.to_numpy(dtype="float32")
    evaluation["predicted_high_f"] = predictions.astype("float32")
    evaluation["error_f"] = evaluation["predicted_high_f"] - evaluation["actual_high_f"]
    evaluation["abs_error_f"] = evaluation["error_f"].abs()

    by_hour = summarize(evaluation.groupby("local_hour", sort=True)).rename(columns={"group_0": "local_hour"})
    by_station_hour = summarize(evaluation.groupby(["station", "local_hour"], sort=True)).rename(
        columns={"group_0": "station", "group_1": "local_hour"}
    )
    afternoon = by_hour[by_hour["local_hour"] >= 12].copy()

    by_hour_file = args.output_dir / "validation_by_local_hour.csv"
    by_station_hour_file = args.output_dir / "validation_by_station_local_hour.csv"
    afternoon_file = args.output_dir / "validation_after_noon_by_local_hour.csv"
    sample_file = args.output_dir / "validation_prediction_sample.csv"
    summary_file = args.output_dir / "validation_by_local_hour_summary.json"

    by_hour.to_csv(by_hour_file, index=False)
    by_station_hour.to_csv(by_station_hour_file, index=False)
    afternoon.to_csv(afternoon_file, index=False)
    evaluation.head(args.prediction_sample_rows).to_csv(sample_file, index=False)

    payload = {
        "model_file": str(args.model_file),
        "feature_file": str(args.feature_file),
        "validation_cutoff_year": cutoff_year,
        "validation_rows": int(len(evaluation)),
        "outputs": {
            "by_hour": str(by_hour_file),
            "by_station_hour": str(by_station_hour_file),
            "after_noon_by_hour": str(afternoon_file),
            "prediction_sample": str(sample_file),
        },
        "best_after_noon_hour_by_mae": int(afternoon.sort_values("mae_f").iloc[0]["local_hour"]),
        "best_after_noon_mae_f": float(afternoon.sort_values("mae_f").iloc[0]["mae_f"]),
        "worst_after_noon_hour_by_mae": int(afternoon.sort_values("mae_f", ascending=False).iloc[0]["local_hour"]),
        "worst_after_noon_mae_f": float(afternoon.sort_values("mae_f", ascending=False).iloc[0]["mae_f"]),
    }
    with summary_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print("By-hour validation:")
    print(by_hour.to_string(index=False))
    print(f"Saved hourly validation files to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
