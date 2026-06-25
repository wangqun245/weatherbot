from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from train_lightgbm_metar_high import add_calendar_variants, coerce_features
from train_lightgbm_metar_high_rolling_6y_holdout import (
    TARGET_COLUMN,
    VARIANT_DROP_COLUMNS,
    add_split_calendar_columns,
    split_by_rolling_blocks,
)


DEFAULT_FEATURE_FILE = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed2"
    r"\all_stations_local_0000_2359_daily_high_features_lag6.csv"
)
DEFAULT_MODELS_ROOT = Path(r"C:\Users\Jack\Documents\git\weatherbot\models")
DEFAULT_BASELINE_DIR = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\models"
    r"\lightgbm_rolling_6y_holdout_24h_lag6_20260623_205608"
)
DEFAULT_DATA_ROOT = Path(r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed2")

NEVER_FEATURE_COLUMNS = {
    TARGET_COLUMN,
    "station",
    "valid",
    "metar",
    "local_year",
    "local_iso_year",
    "valid_utc_epoch",
    "local_hour",
    "split",
}


def parse_hour_range(value: str) -> list[int]:
    if "-" in value:
        start, end = value.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train one rolling-holdout LightGBM daily-high model per city-local hour."
    )
    parser.add_argument("--feature-file", type=Path, default=DEFAULT_FEATURE_FILE)
    parser.add_argument("--models-root", type=Path, default=DEFAULT_MODELS_ROOT)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--data-output-dir", type=Path)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE_DIR)
    parser.add_argument("--hours", default="12-18")
    parser.add_argument("--anchor-validation-year", type=int, default=2026)
    parser.add_argument("--train-years-before-validation", type=int, default=6)
    parser.add_argument("--n-estimators", type=int, default=6000)
    parser.add_argument("--early-stopping-rounds", type=int, default=200)
    parser.add_argument("--learning-rate", type=float, default=0.035)
    parser.add_argument("--num-leaves", type=int, default=96)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def make_run_dirs(args: argparse.Namespace) -> tuple[Path, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_dir = args.output_dir or args.models_root / f"lightgbm_by_local_hour_rolling_6y_lag6_{stamp}"
    data_dir = args.data_output_dir or args.data_root / f"by_local_hour_rolling_6y_lag6_{stamp}"
    for path in (model_dir, data_dir):
        if path.exists():
            raise SystemExit(f"Output directory already exists; choose a new path: {path}")
        path.mkdir(parents=True)
    return model_dir, data_dir


def feature_columns_for(df: pd.DataFrame, variant: str) -> list[str]:
    drop_columns = NEVER_FEATURE_COLUMNS | VARIANT_DROP_COLUMNS[variant]
    return [column for column in df.columns if column not in drop_columns]


def metrics_for(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    error = y_pred - y_true
    abs_error = np.abs(error)
    return {
        "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
        "bias_mean": float(np.mean(error)),
        "median_abs_error": float(np.median(abs_error)),
        "p90_abs_error": float(np.quantile(abs_error, 0.90)),
        "within_1f_pct": float(np.mean(abs_error <= 1.0) * 100.0),
        "within_2f_pct": float(np.mean(abs_error <= 2.0) * 100.0),
        "within_3f_pct": float(np.mean(abs_error <= 3.0) * 100.0),
    }


def train_variant(
    df: pd.DataFrame,
    variant: str,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    args: argparse.Namespace,
) -> tuple[lgb.LGBMRegressor, dict[str, object], pd.DataFrame, np.ndarray]:
    feature_columns = feature_columns_for(df, variant)
    X = coerce_features(df, feature_columns)
    y = pd.to_numeric(df[TARGET_COLUMN], errors="coerce").astype("float32")

    model = lgb.LGBMRegressor(
        objective="regression",
        metric="rmse",
        boosting_type="gbdt",
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        max_depth=-1,
        min_child_samples=80,
        subsample=0.85,
        subsample_freq=1,
        colsample_bytree=0.85,
        reg_alpha=0.05,
        reg_lambda=0.25,
        random_state=args.random_state,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(
        X.loc[train_mask],
        y.loc[train_mask],
        eval_set=[(X.loc[valid_mask], y.loc[valid_mask])],
        eval_metric="rmse",
        callbacks=[
            lgb.early_stopping(args.early_stopping_rounds, verbose=False),
            lgb.log_evaluation(period=100),
        ],
    )

    train_pred = model.predict(X.loc[train_mask], num_iteration=model.best_iteration_)
    valid_pred = model.predict(X.loc[valid_mask], num_iteration=model.best_iteration_)
    metrics = {
        "variant": variant,
        "feature_count": len(feature_columns),
        "best_iteration": int(model.best_iteration_ or args.n_estimators),
        "train": metrics_for(y.loc[train_mask].to_numpy(), train_pred),
        "validation": metrics_for(y.loc[valid_mask].to_numpy(), valid_pred),
    }
    importance = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance_gain": model.booster_.feature_importance(importance_type="gain"),
            "importance_split": model.booster_.feature_importance(importance_type="split"),
        }
    ).sort_values(["importance_gain", "importance_split"], ascending=False)
    return model, metrics, importance, valid_pred


def write_hour_predictions(
    df: pd.DataFrame,
    valid_mask: np.ndarray,
    valid_pred: np.ndarray,
    local_hour: int,
    output_dir: Path,
) -> pd.DataFrame:
    valid_df = df.loc[valid_mask].copy()
    y_valid = pd.to_numeric(valid_df[TARGET_COLUMN], errors="coerce").astype("float32")
    evaluation = valid_df[
        ["station", "valid", "local_year", "local_month", "local_day", "local_hour"]
    ].copy()
    evaluation["actual_high_f"] = y_valid.to_numpy(dtype="float32")
    evaluation["predicted_high_f"] = valid_pred.astype("float32")
    evaluation["error_f"] = evaluation["predicted_high_f"] - evaluation["actual_high_f"]
    evaluation["abs_error_f"] = evaluation["error_f"].abs()
    evaluation.to_csv(output_dir / f"validation_predictions_hour_{local_hour:02d}.csv", index=False)
    return evaluation


def compare_with_baseline(output_dir: Path, baseline_dir: Path, summary: pd.DataFrame) -> pd.DataFrame:
    baseline_file = baseline_dir / "rolling_6y_validation_by_local_hour.csv"
    if not baseline_file.exists():
        summary.to_csv(output_dir / "by_hour_model_comparison.csv", index=False)
        return summary

    baseline = pd.read_csv(baseline_file)
    baseline = baseline.rename(
        columns={
            "count": "baseline_count",
            "bias_mean_f": "baseline_bias_mean_f",
            "mae_f": "baseline_mae_f",
            "rmse_f": "baseline_rmse_f",
            "median_abs_error_f": "baseline_median_abs_error_f",
            "p90_abs_error_f": "baseline_p90_abs_error_f",
            "within_1f_pct": "baseline_within_1f_pct",
            "within_2f_pct": "baseline_within_2f_pct",
            "within_3f_pct": "baseline_within_3f_pct",
        }
    )
    merged = summary.merge(baseline, on="local_hour", how="left")
    merged["rmse_delta_specialized_minus_baseline"] = merged["rmse_f"] - merged["baseline_rmse_f"]
    merged["mae_delta_specialized_minus_baseline"] = merged["mae_f"] - merged["baseline_mae_f"]
    merged["median_abs_error_delta_specialized_minus_baseline"] = (
        merged["median_abs_error_f"] - merged["baseline_median_abs_error_f"]
    )
    merged.to_csv(output_dir / "by_hour_model_comparison.csv", index=False)
    return merged


def main() -> int:
    args = parse_args()
    if not args.feature_file.exists():
        raise SystemExit(f"Feature file does not exist: {args.feature_file}")
    output_dir, data_output_dir = make_run_dirs(args)
    hours = parse_hour_range(args.hours)

    print(f"Loading {args.feature_file}")
    df = pd.read_csv(args.feature_file, low_memory=False)
    df = add_calendar_variants(df)
    df = add_split_calendar_columns(df)

    all_metrics: dict[str, object] = {
        "feature_file": str(args.feature_file),
        "baseline_dir": str(args.baseline_dir),
        "target": TARGET_COLUMN,
        "hours": hours,
        "excluded_from_training": sorted(NEVER_FEATURE_COLUMNS),
        "hour_models": [],
    }
    summary_rows: list[dict[str, object]] = []
    all_predictions: list[pd.DataFrame] = []

    for local_hour in hours:
        hour_dir = output_dir / f"local_hour_{local_hour:02d}"
        hour_data_dir = data_output_dir / f"local_hour_{local_hour:02d}"
        hour_dir.mkdir(parents=True)
        hour_data_dir.mkdir(parents=True)
        hour_df = df[df["local_hour"].astype("int16") == local_hour].copy()
        if hour_df.empty:
            print(f"Skipping hour {local_hour}: no rows")
            continue

        train_mask, valid_mask, blocks, train_years, validation_years = split_by_rolling_blocks(
            df=hour_df,
            anchor_validation_year=args.anchor_validation_year,
            train_years_before_validation=args.train_years_before_validation,
        )
        print(
            f"Hour {local_hour:02d}: rows={len(hour_df):,}; train={int(train_mask.sum()):,}; "
            f"valid={int(valid_mask.sum()):,}; validation_years={sorted(validation_years)}"
        )
        hour_df = hour_df.copy()
        hour_df["split"] = np.where(train_mask, "train", np.where(valid_mask, "validation", "unused"))
        hour_df.to_csv(hour_data_dir / f"features_local_hour_{local_hour:02d}.csv.gz", index=False, compression="gzip")

        best_model: lgb.LGBMRegressor | None = None
        best_variant = ""
        best_metrics: dict[str, object] | None = None
        best_pred: np.ndarray | None = None
        best_rmse = float("inf")
        hour_metrics: list[dict[str, object]] = []

        for variant in ("month_day", "week_of_year", "cyclical_day_of_year"):
            print(f"Hour {local_hour:02d}: training variant {variant}")
            model, metrics, importance, valid_pred = train_variant(hour_df, variant, train_mask, valid_mask, args)
            hour_metrics.append(metrics)
            joblib.dump(model, hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_{variant}.pkl")
            model.booster_.save_model(hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_{variant}.txt")
            importance.to_csv(
                hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_{variant}_feature_importance.csv",
                index=False,
            )

            validation_rmse = float(metrics["validation"]["rmse"])  # type: ignore[index]
            print(
                f"Hour {local_hour:02d} {variant}: valid RMSE={validation_rmse:.4f}, "
                f"MAE={float(metrics['validation']['mae']):.4f}, "
                f"median={float(metrics['validation']['median_abs_error']):.4f}"
            )
            if validation_rmse < best_rmse:
                best_rmse = validation_rmse
                best_model = model
                best_variant = variant
                best_metrics = metrics
                best_pred = valid_pred

        if best_model is None or best_metrics is None or best_pred is None:
            raise RuntimeError(f"No model trained for local hour {local_hour}")

        joblib.dump(best_model, hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_best.pkl")
        best_model.booster_.save_model(hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_best.txt")
        predictions = write_hour_predictions(hour_df, valid_mask, best_pred, local_hour, hour_dir)
        all_predictions.append(predictions)

        validation = best_metrics["validation"]  # type: ignore[index]
        summary_rows.append(
            {
                "local_hour": local_hour,
                "best_variant": best_variant,
                "feature_count": int(best_metrics["feature_count"]),  # type: ignore[arg-type]
                "best_iteration": int(best_metrics["best_iteration"]),  # type: ignore[arg-type]
                "count": int(len(predictions)),
                "bias_mean_f": float(validation["bias_mean"]),  # type: ignore[index]
                "mae_f": float(validation["mae"]),  # type: ignore[index]
                "rmse_f": float(validation["rmse"]),  # type: ignore[index]
                "median_abs_error_f": float(validation["median_abs_error"]),  # type: ignore[index]
                "p90_abs_error_f": float(validation["p90_abs_error"]),  # type: ignore[index]
                "within_1f_pct": float(validation["within_1f_pct"]),  # type: ignore[index]
                "within_2f_pct": float(validation["within_2f_pct"]),  # type: ignore[index]
                "within_3f_pct": float(validation["within_3f_pct"]),  # type: ignore[index]
            }
        )
        payload = {
            "local_hour": local_hour,
            "split": {
                "type": "rolling_6_train_years_then_1_validation_year",
                "anchor_validation_year": args.anchor_validation_year,
                "train_years_before_validation": args.train_years_before_validation,
                "blocks": blocks,
                "train_years": sorted(train_years),
                "validation_years": sorted(validation_years),
                "train_rows": int(train_mask.sum()),
                "validation_rows": int(valid_mask.sum()),
            },
            "best_variant": best_variant,
            "variants": hour_metrics,
        }
        with (hour_dir / f"lightgbm_metar_high_hour_{local_hour:02d}_metrics.json").open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        all_metrics["hour_models"].append(payload)  # type: ignore[index]

    summary = pd.DataFrame(summary_rows).sort_values("local_hour")
    summary.to_csv(output_dir / "by_hour_specialized_summary.csv", index=False)
    summary.to_csv(data_output_dir / "by_hour_specialized_summary.csv", index=False)
    comparison = compare_with_baseline(output_dir, args.baseline_dir, summary)
    comparison.to_csv(data_output_dir / "by_hour_model_comparison.csv", index=False)
    if all_predictions:
        pd.concat(all_predictions, ignore_index=True).to_csv(output_dir / "by_hour_validation_predictions.csv", index=False)
        pd.concat(all_predictions, ignore_index=True).to_csv(data_output_dir / "by_hour_validation_predictions.csv.gz", index=False, compression="gzip")
    with (output_dir / "by_hour_training_metrics.json").open("w", encoding="utf-8") as handle:
        json.dump(all_metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print("Specialized hour model comparison:")
    print(
        comparison[
            [
                "local_hour",
                "best_variant",
                "rmse_f",
                "baseline_rmse_f",
                "rmse_delta_specialized_minus_baseline",
                "mae_f",
                "baseline_mae_f",
                "mae_delta_specialized_minus_baseline",
                "median_abs_error_f",
                "baseline_median_abs_error_f",
                "median_abs_error_delta_specialized_minus_baseline",
            ]
        ].to_string(index=False)
    )
    print(f"New per-hour model directory: {output_dir}")
    print(f"New per-hour data directory: {data_output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
