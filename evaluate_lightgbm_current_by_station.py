from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_PREDICTIONS_FILE = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\models"
    r"\lightgbm_by_local_hour_rolling_6y_lag6_20260624_080651"
    r"\by_hour_validation_predictions.csv"
)

CITY_BY_STATION = {
    "ATL": "Atlanta",
    "AUS": "Austin",
    "DAL": "Dallas",
    "DEN": "Denver",
    "HOU": "Houston",
    "LAX": "Los Angeles",
    "LGA": "NYC",
    "MIA": "Miami",
    "ORD": "Chicago",
    "SEA": "Seattle",
    "SFO": "San Francisco",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize current LightGBM validation predictions by airport station and local hour."
    )
    parser.add_argument("--predictions-file", type=Path, default=DEFAULT_PREDICTIONS_FILE)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--local-hour-min", type=int)
    parser.add_argument("--local-hour-max", type=int)
    return parser.parse_args()


def rmse(values: pd.Series) -> float:
    return float(np.sqrt(np.mean(np.square(values.to_numpy(dtype="float64")))))


def summarize(group: pd.core.groupby.generic.DataFrameGroupBy) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for key, part in group:
        if not isinstance(key, tuple):
            key = (key,)
        signed_error = part["error_f"]
        abs_error = part["abs_error_f"]
        actual = part["actual_high_f"]
        pred = part["predicted_high_f"]
        rows.append(
            {
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
            | {f"group_{idx}": value for idx, value in enumerate(key)}
        )
    return pd.DataFrame(rows)


def add_station_rank_columns(by_station: pd.DataFrame, by_station_hour: pd.DataFrame) -> pd.DataFrame:
    best_hour = (
        by_station_hour.sort_values(["station", "mae_f", "rmse_f"])
        .groupby("station", as_index=False)
        .first()[["station", "local_hour", "mae_f", "rmse_f", "within_2f_pct"]]
        .rename(
            columns={
                "local_hour": "best_local_hour_by_mae",
                "mae_f": "best_local_hour_mae_f",
                "rmse_f": "best_local_hour_rmse_f",
                "within_2f_pct": "best_local_hour_within_2f_pct",
            }
        )
    )
    worst_hour = (
        by_station_hour.sort_values(["station", "mae_f", "rmse_f"], ascending=[True, False, False])
        .groupby("station", as_index=False)
        .first()[["station", "local_hour", "mae_f", "rmse_f", "within_2f_pct"]]
        .rename(
            columns={
                "local_hour": "worst_local_hour_by_mae",
                "mae_f": "worst_local_hour_mae_f",
                "rmse_f": "worst_local_hour_rmse_f",
                "within_2f_pct": "worst_local_hour_within_2f_pct",
            }
        )
    )
    ranked = by_station.merge(best_hour, on="station", how="left").merge(worst_hour, on="station", how="left")
    ranked = ranked.sort_values(["mae_f", "rmse_f"]).reset_index(drop=True)
    ranked.insert(0, "rank_by_mae", np.arange(1, len(ranked) + 1))
    return ranked


def main() -> int:
    args = parse_args()
    if not args.predictions_file.exists():
        raise SystemExit(f"Predictions file does not exist: {args.predictions_file}")

    output_dir = args.output_dir or args.predictions_file.parent / "station_validation"
    output_dir.mkdir(parents=True, exist_ok=True)

    predictions = pd.read_csv(args.predictions_file)
    required = {
        "station",
        "local_year",
        "local_hour",
        "actual_high_f",
        "predicted_high_f",
        "error_f",
        "abs_error_f",
    }
    missing = sorted(required - set(predictions.columns))
    if missing:
        raise SystemExit(f"Predictions file is missing required columns: {missing}")
    if args.local_hour_min is not None:
        predictions = predictions[predictions["local_hour"] >= args.local_hour_min].copy()
    if args.local_hour_max is not None:
        predictions = predictions[predictions["local_hour"] <= args.local_hour_max].copy()
    if predictions.empty:
        raise SystemExit("No prediction rows remain after applying local-hour filters")

    by_station = summarize(predictions.groupby("station", sort=True)).rename(columns={"group_0": "station"})
    by_station.insert(1, "city", by_station["station"].map(CITY_BY_STATION).fillna(""))
    by_station_hour = summarize(predictions.groupby(["station", "local_hour"], sort=True)).rename(
        columns={"group_0": "station", "group_1": "local_hour"}
    )
    by_station_hour.insert(1, "city", by_station_hour["station"].map(CITY_BY_STATION).fillna(""))
    by_station_year = summarize(predictions.groupby(["station", "local_year"], sort=True)).rename(
        columns={"group_0": "station", "group_1": "local_year"}
    )
    by_station_year.insert(1, "city", by_station_year["station"].map(CITY_BY_STATION).fillna(""))

    ranked = add_station_rank_columns(by_station, by_station_hour)

    by_station_file = output_dir / "validation_by_station.csv"
    by_station_hour_file = output_dir / "validation_by_station_local_hour.csv"
    by_station_year_file = output_dir / "validation_by_station_year.csv"
    summary_file = output_dir / "validation_by_station_summary.json"

    ranked.to_csv(by_station_file, index=False)
    by_station_hour.to_csv(by_station_hour_file, index=False)
    by_station_year.to_csv(by_station_year_file, index=False)

    payload = {
        "predictions_file": str(args.predictions_file),
        "validation_rows": int(len(predictions)),
        "local_hour_min": args.local_hour_min,
        "local_hour_max": args.local_hour_max,
        "stations": int(predictions["station"].nunique()),
        "local_hours": sorted(int(value) for value in predictions["local_hour"].dropna().unique()),
        "outputs": {
            "by_station": str(by_station_file),
            "by_station_local_hour": str(by_station_hour_file),
            "by_station_year": str(by_station_year_file),
        },
        "best_station_by_mae": ranked.iloc[0][["station", "city", "mae_f", "rmse_f", "within_2f_pct"]].to_dict(),
        "worst_station_by_mae": ranked.iloc[-1][["station", "city", "mae_f", "rmse_f", "within_2f_pct"]].to_dict(),
    }
    with summary_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print("Validation accuracy by airport station:")
    print(
        ranked[
            [
                "rank_by_mae",
                "station",
                "city",
                "count",
                "mae_f",
                "rmse_f",
                "median_abs_error_f",
                "p90_abs_error_f",
                "within_1f_pct",
                "within_2f_pct",
                "within_3f_pct",
                "bias_mean_f",
                "best_local_hour_by_mae",
                "worst_local_hour_by_mae",
            ]
        ].to_string(index=False)
    )
    print(f"Saved station validation files to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
