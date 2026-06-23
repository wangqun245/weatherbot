from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


DEFAULT_FEATURE_FILE = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed"
    r"\all_stations_local_0900_1900_daily_high_features.csv"
)
DEFAULT_OUTPUT_DIR = Path(r"C:\Users\Jack\Documents\git\weatherbot\models")

TARGET_COLUMN = "daily_high_f"
TRACKING_COLUMNS = {"station", "valid", "metar"}
NEVER_FEATURE_COLUMNS = {
    TARGET_COLUMN,
    "station",
    "valid",
    "metar",
    "local_year",
    "valid_utc_epoch",
}


VARIANT_DROP_COLUMNS = {
    "month_day": {"local_day_of_year", "local_doy_sin", "local_doy_cos", "local_week_of_year"},
    "week_of_year": {
        "local_month",
        "local_day",
        "local_day_of_year",
        "local_doy_sin",
        "local_doy_cos",
    },
    "cyclical_day_of_year": {"local_month", "local_day", "local_day_of_year", "local_week_of_year"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and compare LightGBM regressors for airport daily high temperature."
    )
    parser.add_argument("--feature-file", type=Path, default=DEFAULT_FEATURE_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--validation-year-fraction",
        type=float,
        default=0.2,
        help="Fraction of unique years held out at the end for validation.",
    )
    parser.add_argument("--n-estimators", type=int, default=3000)
    parser.add_argument("--early-stopping-rounds", type=int, default=100)
    parser.add_argument("--learning-rate", type=float, default=0.035)
    parser.add_argument("--num-leaves", type=int, default=96)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def add_calendar_variants(df: pd.DataFrame) -> pd.DataFrame:
    date_index = pd.to_datetime(
        {
            "year": df["local_year"].astype("int16"),
            "month": df["local_month"].astype("int8"),
            "day": df["local_day"].astype("int8"),
        },
        errors="coerce",
    )
    df["local_week_of_year"] = date_index.dt.isocalendar().week.astype("float32")
    return df


def split_by_recent_years(df: pd.DataFrame, validation_year_fraction: float) -> tuple[np.ndarray, np.ndarray, int]:
    years = np.array(sorted(df["local_year"].dropna().astype(int).unique()))
    if len(years) < 5:
        raise ValueError(f"Not enough unique years for a time split: {years.tolist()}")
    validation_year_count = max(1, int(math.ceil(len(years) * validation_year_fraction)))
    cutoff_year = int(years[-validation_year_count])
    train_mask = df["local_year"] < cutoff_year
    valid_mask = df["local_year"] >= cutoff_year
    return train_mask.to_numpy(), valid_mask.to_numpy(), cutoff_year


def feature_columns_for(df: pd.DataFrame, variant: str) -> list[str]:
    drop_columns = NEVER_FEATURE_COLUMNS | VARIANT_DROP_COLUMNS[variant]
    columns = [column for column in df.columns if column not in drop_columns]
    return columns


def coerce_features(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    features = df[columns].copy()
    for column in columns:
        features[column] = pd.to_numeric(features[column], errors="coerce")
    return features.astype("float32")


def metrics_for(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def train_variant(
    df: pd.DataFrame,
    variant: str,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    args: argparse.Namespace,
) -> tuple[lgb.LGBMRegressor, dict[str, object], pd.DataFrame]:
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

    return model, metrics, importance


def main() -> int:
    args = parse_args()
    if not args.feature_file.exists():
        raise SystemExit(f"Feature file does not exist: {args.feature_file}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {args.feature_file}")
    df = pd.read_csv(args.feature_file, low_memory=False)
    df = add_calendar_variants(df)
    train_mask, valid_mask, cutoff_year = split_by_recent_years(df, args.validation_year_fraction)
    print(
        f"Rows: {len(df):,}; train rows: {int(train_mask.sum()):,}; "
        f"validation rows: {int(valid_mask.sum()):,}; validation years >= {cutoff_year}"
    )

    all_metrics: list[dict[str, object]] = []
    best_model: lgb.LGBMRegressor | None = None
    best_variant: str | None = None
    best_rmse = float("inf")

    for variant in ("month_day", "week_of_year", "cyclical_day_of_year"):
        print(f"Training variant: {variant}")
        model, metrics, importance = train_variant(df, variant, train_mask, valid_mask, args)
        all_metrics.append(metrics)

        variant_model_file = args.output_dir / f"lightgbm_metar_high_{variant}.pkl"
        variant_text_file = args.output_dir / f"lightgbm_metar_high_{variant}.txt"
        variant_importance_file = args.output_dir / f"lightgbm_metar_high_{variant}_feature_importance.csv"
        joblib.dump(model, variant_model_file)
        model.booster_.save_model(variant_text_file)
        importance.to_csv(variant_importance_file, index=False)

        validation_rmse = float(metrics["validation"]["rmse"])  # type: ignore[index]
        print(
            f"{variant}: valid RMSE={validation_rmse:.4f}, "
            f"MAE={float(metrics['validation']['mae']):.4f}, "
            f"R2={float(metrics['validation']['r2']):.4f}"
        )
        if validation_rmse < best_rmse:
            best_rmse = validation_rmse
            best_model = model
            best_variant = variant

    if best_model is None or best_variant is None:
        raise RuntimeError("No model was trained")

    metrics_payload = {
        "feature_file": str(args.feature_file),
        "target": TARGET_COLUMN,
        "excluded_from_training": sorted(NEVER_FEATURE_COLUMNS),
        "validation_cutoff_year": cutoff_year,
        "validation_rule": f"train local_year < {cutoff_year}; validate local_year >= {cutoff_year}",
        "best_variant": best_variant,
        "variants": all_metrics,
    }

    metrics_file = args.output_dir / "lightgbm_metar_high_metrics.json"
    with metrics_file.open("w", encoding="utf-8") as handle:
        json.dump(metrics_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    joblib.dump(best_model, args.output_dir / "lightgbm_metar_high_best.pkl")
    best_model.booster_.save_model(args.output_dir / "lightgbm_metar_high_best.txt")

    print(f"Best variant: {best_variant} with validation RMSE={best_rmse:.4f}")
    print(f"Metrics written to {metrics_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
