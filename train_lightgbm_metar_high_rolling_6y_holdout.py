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


DEFAULT_FEATURE_FILE = Path(
    r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed"
    r"\all_stations_local_0900_1900_daily_high_features.csv"
)
DEFAULT_MODELS_ROOT = Path(r"C:\Users\Jack\Documents\git\weatherbot\models")

TARGET_COLUMN = "daily_high_f"
NEVER_FEATURE_COLUMNS = {
    TARGET_COLUMN,
    "station",
    "valid",
    "metar",
    "local_year",
    "local_iso_year",
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
        description=(
            "Train LightGBM using rolling year blocks: one validation year, "
            "the prior N years as training, stepping backward by N+1 years."
        )
    )
    parser.add_argument("--feature-file", type=Path, default=DEFAULT_FEATURE_FILE)
    parser.add_argument("--models-root", type=Path, default=DEFAULT_MODELS_ROOT)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--anchor-validation-year", type=int, default=2026)
    parser.add_argument("--train-years-before-validation", type=int, default=6)
    parser.add_argument(
        "--single-validation-block",
        action="store_true",
        help="Use only the anchor validation year and its prior N train years instead of rolling backward.",
    )
    parser.add_argument("--n-estimators", type=int, default=6000)
    parser.add_argument("--early-stopping-rounds", type=int, default=200)
    parser.add_argument("--learning-rate", type=float, default=0.035)
    parser.add_argument("--num-leaves", type=int, default=96)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Reuse and re-evaluate completed variant models in an existing "
            "output directory, training only missing variants."
        ),
    )
    return parser.parse_args()


def make_output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir is not None:
        output_dir = args.output_dir
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = args.models_root / f"lightgbm_rolling_6y_holdout_{stamp}"
    if output_dir.exists() and not args.resume:
        raise SystemExit(f"Output directory already exists; choose a new path: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=args.resume)
    return output_dir


def add_split_calendar_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_index = pd.to_datetime(
        {
            "year": df["local_year"].astype("int16"),
            "month": df["local_month"].astype("int8"),
            "day": df["local_day"].astype("int8"),
        },
        errors="coerce",
    )
    iso = date_index.dt.isocalendar()
    df["local_iso_year"] = iso.year.astype("int16")
    df["local_week_of_year"] = iso.week.astype("float32")
    return df


def rolling_year_blocks(
    available_years: list[int],
    anchor_validation_year: int,
    train_years_before_validation: int,
) -> tuple[list[dict[str, object]], set[int], set[int]]:
    available = set(available_years)
    min_year = min(available_years)
    step = train_years_before_validation + 1

    blocks: list[dict[str, object]] = []
    train_years: set[int] = set()
    validation_years: set[int] = set()
    validation_year = anchor_validation_year
    while validation_year >= min_year:
        candidate_train_years = list(
            range(validation_year - train_years_before_validation, validation_year)
        )
        present_train_years = [year for year in candidate_train_years if year in available]
        if validation_year in available and len(present_train_years) == train_years_before_validation:
            blocks.append(
                {
                    "validation_year": validation_year,
                    "train_years": present_train_years,
                }
            )
            validation_years.add(validation_year)
            train_years.update(present_train_years)
        validation_year -= step

    return blocks, train_years, validation_years


def split_by_rolling_blocks(
    df: pd.DataFrame,
    anchor_validation_year: int,
    train_years_before_validation: int,
    single_validation_block: bool = False,
) -> tuple[np.ndarray, np.ndarray, list[dict[str, object]], set[int], set[int]]:
    available_years = sorted(df["local_year"].dropna().astype(int).unique().tolist())
    blocks, train_years, validation_years = rolling_year_blocks(
        available_years=available_years,
        anchor_validation_year=anchor_validation_year,
        train_years_before_validation=train_years_before_validation,
    )
    if single_validation_block and blocks:
        blocks = [blocks[0]]
        validation_years = {int(blocks[0]["validation_year"])}
        train_years = {int(year) for year in blocks[0]["train_years"]}
    if not blocks:
        raise ValueError("No rolling year blocks could be constructed from the data")
    train_mask = df["local_year"].isin(train_years).to_numpy()
    valid_mask = df["local_year"].isin(validation_years).to_numpy()
    return train_mask, valid_mask, blocks, train_years, validation_years


def feature_columns_for(df: pd.DataFrame, variant: str) -> list[str]:
    drop_columns = NEVER_FEATURE_COLUMNS | VARIANT_DROP_COLUMNS[variant]
    return [column for column in df.columns if column not in drop_columns]


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


def evaluate_variant(
    df: pd.DataFrame,
    variant: str,
    train_mask: np.ndarray,
    valid_mask: np.ndarray,
    model: lgb.LGBMRegressor,
) -> tuple[dict[str, object], pd.DataFrame]:
    feature_columns = feature_columns_for(df, variant)
    if feature_columns != list(model.feature_name_):
        raise ValueError(
            f"Existing {variant} model feature columns do not match the dataset"
        )
    X = coerce_features(df, feature_columns)
    y = pd.to_numeric(df[TARGET_COLUMN], errors="coerce").astype("float32")
    train_pred = model.predict(
        X.loc[train_mask], num_iteration=model.best_iteration_
    )
    valid_pred = model.predict(
        X.loc[valid_mask], num_iteration=model.best_iteration_
    )
    metrics = {
        "variant": variant,
        "feature_count": len(feature_columns),
        "best_iteration": int(model.best_iteration_ or model.n_estimators),
        "train": metrics_for(y.loc[train_mask].to_numpy(), train_pred),
        "validation": metrics_for(y.loc[valid_mask].to_numpy(), valid_pred),
    }
    importance = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance_gain": model.booster_.feature_importance(
                importance_type="gain"
            ),
            "importance_split": model.booster_.feature_importance(
                importance_type="split"
            ),
        }
    ).sort_values(
        ["importance_gain", "importance_split"], ascending=False
    )
    return metrics, importance


def write_validation_reports(
    df: pd.DataFrame,
    model: lgb.LGBMRegressor,
    valid_mask: np.ndarray,
    blocks: list[dict[str, object]],
    output_dir: Path,
) -> None:
    feature_columns = list(model.feature_name_)
    valid_df = df.loc[valid_mask].copy()
    X_valid = coerce_features(valid_df, feature_columns)
    y_valid = pd.to_numeric(valid_df[TARGET_COLUMN], errors="coerce").astype("float32")
    pred = model.predict(X_valid, num_iteration=model.best_iteration_)

    evaluation = valid_df[
        ["station", "valid", "local_year", "local_month", "local_day", "local_hour"]
    ].copy()
    evaluation["actual_high_f"] = y_valid.to_numpy(dtype="float32")
    evaluation["predicted_high_f"] = pred.astype("float32")
    evaluation["error_f"] = evaluation["predicted_high_f"] - evaluation["actual_high_f"]
    evaluation["abs_error_f"] = evaluation["error_f"].abs()

    def summarize(group: pd.core.groupby.generic.DataFrameGroupBy) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for key, part in group:
            if not isinstance(key, tuple):
                key = (key,)
            error = part["error_f"].to_numpy(dtype="float64")
            abs_error = part["abs_error_f"]
            rows.append(
                {
                    "count": int(len(part)),
                    "bias_mean_f": float(part["error_f"].mean()),
                    "mae_f": float(abs_error.mean()),
                    "rmse_f": float(np.sqrt(np.mean(error * error))),
                    "median_abs_error_f": float(abs_error.median()),
                    "p90_abs_error_f": float(abs_error.quantile(0.90)),
                    "within_1f_pct": float((abs_error <= 1.0).mean() * 100.0),
                    "within_2f_pct": float((abs_error <= 2.0).mean() * 100.0),
                    "within_3f_pct": float((abs_error <= 3.0).mean() * 100.0),
                }
                | {f"group_{idx}": value for idx, value in enumerate(key)}
            )
        return pd.DataFrame(rows)

    by_year = summarize(evaluation.groupby("local_year", sort=True)).rename(columns={"group_0": "local_year"})
    by_hour = summarize(evaluation.groupby("local_hour", sort=True)).rename(columns={"group_0": "local_hour"})
    by_station_hour = summarize(evaluation.groupby(["station", "local_hour"], sort=True)).rename(
        columns={"group_0": "station", "group_1": "local_hour"}
    )
    evaluation.to_csv(output_dir / "rolling_6y_validation_predictions.csv", index=False)
    by_year.to_csv(output_dir / "rolling_6y_validation_by_year.csv", index=False)
    by_hour.to_csv(output_dir / "rolling_6y_validation_by_local_hour.csv", index=False)
    by_station_hour.to_csv(output_dir / "rolling_6y_validation_by_station_local_hour.csv", index=False)
    pd.DataFrame(blocks).to_csv(output_dir / "rolling_6y_split_blocks.csv", index=False)


def main() -> int:
    args = parse_args()
    if not args.feature_file.exists():
        raise SystemExit(f"Feature file does not exist: {args.feature_file}")
    output_dir = make_output_dir(args)

    print(f"Loading {args.feature_file}")
    df = pd.read_csv(args.feature_file, low_memory=False)
    df = add_calendar_variants(df)
    df = add_split_calendar_columns(df)
    train_mask, valid_mask, blocks, train_years, validation_years = split_by_rolling_blocks(
        df=df,
        anchor_validation_year=args.anchor_validation_year,
        train_years_before_validation=args.train_years_before_validation,
        single_validation_block=args.single_validation_block,
    )
    print(f"Split blocks: {blocks}")
    print(
        f"Rows: {len(df):,}; train rows: {int(train_mask.sum()):,}; "
        f"validation rows: {int(valid_mask.sum()):,}; "
        f"train years={sorted(train_years)}; validation years={sorted(validation_years)}"
    )

    all_metrics: list[dict[str, object]] = []
    best_model: lgb.LGBMRegressor | None = None
    best_variant: str | None = None
    best_rmse = float("inf")

    for variant in ("month_day", "week_of_year", "cyclical_day_of_year"):
        print(f"Training variant: {variant}")
        variant_model_file = (
            output_dir / f"lightgbm_metar_high_rolling_6y_{variant}.pkl"
        )
        if args.resume and variant_model_file.exists():
            print(f"Resuming completed variant: {variant}")
            model = joblib.load(variant_model_file)
            metrics, importance = evaluate_variant(
                df, variant, train_mask, valid_mask, model
            )
        else:
            model, metrics, importance = train_variant(
                df, variant, train_mask, valid_mask, args
            )
        all_metrics.append(metrics)

        joblib.dump(model, variant_model_file)
        model.booster_.save_model(output_dir / f"lightgbm_metar_high_rolling_6y_{variant}.txt")
        importance.to_csv(
            output_dir / f"lightgbm_metar_high_rolling_6y_{variant}_feature_importance.csv",
            index=False,
        )

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

    joblib.dump(best_model, output_dir / "lightgbm_metar_high_rolling_6y_best.pkl")
    best_model.booster_.save_model(output_dir / "lightgbm_metar_high_rolling_6y_best.txt")
    write_validation_reports(df, best_model, valid_mask, blocks, output_dir)

    metrics_payload = {
        "feature_file": str(args.feature_file),
        "target": TARGET_COLUMN,
        "excluded_from_training": sorted(NEVER_FEATURE_COLUMNS),
        "split": {
            "type": "rolling_6_train_years_then_1_validation_year",
            "anchor_validation_year": args.anchor_validation_year,
            "train_years_before_validation": args.train_years_before_validation,
            "single_validation_block": bool(args.single_validation_block),
            "blocks": blocks,
            "train_years": sorted(train_years),
            "validation_years": sorted(validation_years),
            "train_rows": int(train_mask.sum()),
            "validation_rows": int(valid_mask.sum()),
        },
        "best_variant": best_variant,
        "variants": all_metrics,
    }
    metrics_file = output_dir / "lightgbm_metar_high_rolling_6y_metrics.json"
    with metrics_file.open("w", encoding="utf-8") as handle:
        json.dump(metrics_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Best variant: {best_variant} with validation RMSE={best_rmse:.4f}")
    print(f"New model directory: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
