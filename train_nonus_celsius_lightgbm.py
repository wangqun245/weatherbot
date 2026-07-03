from __future__ import annotations

import json
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


ROOT = Path(__file__).resolve().parent
SPLIT_DIR = ROOT / "NONUS" / "newC" / "year_folds_6_to_1"
TRAIN_FILE = SPLIT_DIR / "nonus_celsius_train_year_folds.csv"
VALID_FILE = SPLIT_DIR / "nonus_celsius_validation_year_folds.csv"
OUTPUT_DIR = ROOT / "NONUS" / "newC" / "independent_model_c"
TARGET = "daily_high_c"
DROP_COLUMNS = {
    "fold_id", "validation_year", "split", TARGET, "station", "valid",
    "metar", "local_year", "valid_utc_epoch",
}
META_COLUMNS = [
    "fold_id", "validation_year", "station", "valid", "local_year",
    "local_month", "local_day", "local_hour", "local_minute",
]


def metrics(actual: np.ndarray, predicted: np.ndarray) -> dict:
    error = predicted - actual
    absolute = np.abs(error)
    return {
        "count": int(len(actual)),
        "rmse_c": float(np.sqrt(mean_squared_error(actual, predicted))),
        "mae_c": float(mean_absolute_error(actual, predicted)),
        "r2": (
            float(r2_score(actual, predicted))
            if len(actual) > 1
            else float("nan")
        ),
        "bias_mean_c": float(np.mean(error)),
        "median_abs_error_c": float(np.median(absolute)),
        "p90_abs_error_c": float(np.quantile(absolute, 0.90)),
        "p95_abs_error_c": float(np.quantile(absolute, 0.95)),
        "within_0_5c_pct": float(np.mean(absolute <= 0.5) * 100.0),
        "within_1c_pct": float(np.mean(absolute <= 1.0) * 100.0),
        "within_1_5c_pct": float(np.mean(absolute <= 1.5) * 100.0),
        "within_2c_pct": float(np.mean(absolute <= 2.0) * 100.0),
    }


def grouped_report(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = []
    grouper = columns[0] if len(columns) == 1 else columns
    for key, part in frame.groupby(grouper, sort=True, observed=True):
        values = key if isinstance(key, tuple) else (key,)
        row = dict(zip(columns, values))
        row.update(
            metrics(
                part["actual_high_c"].to_numpy(dtype=np.float64),
                part["predicted_high_c"].to_numpy(dtype=np.float64),
            )
        )
        output.append(row)
    return pd.DataFrame(output)


def main() -> int:
    if OUTPUT_DIR.exists():
        raise SystemExit(f"Output directory already exists: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True)
    header = pd.read_csv(TRAIN_FILE, nrows=0).columns.tolist()
    feature_columns = [column for column in header if column not in DROP_COLUMNS]
    numeric_columns = [TARGET, *feature_columns]
    print(f"Features: {len(feature_columns)}")
    print("Loading Celsius training data")
    train = pd.read_csv(
        TRAIN_FILE,
        usecols=numeric_columns,
        dtype={column: "float32" for column in numeric_columns},
        low_memory=False,
    )
    valid_columns = list(dict.fromkeys([*numeric_columns, *META_COLUMNS]))
    valid_dtypes = {
        column: "float32"
        for column in numeric_columns
        if column not in META_COLUMNS
    }
    print("Loading Celsius validation data")
    valid = pd.read_csv(
        VALID_FILE,
        usecols=valid_columns,
        dtype=valid_dtypes,
        low_memory=False,
    )
    X_train = train[feature_columns]
    y_train = train[TARGET]
    X_valid = valid[feature_columns]
    y_valid = valid[TARGET].astype("float32")
    print(f"Train: {len(train):,}; validation: {len(valid):,}")

    parameters = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "n_estimators": 5000,
        "learning_rate": 0.025,
        "num_leaves": 96,
        "max_depth": -1,
        "min_child_samples": 100,
        "subsample": 0.85,
        "subsample_freq": 1,
        "colsample_bytree": 0.85,
        "reg_alpha": 0.05,
        "reg_lambda": 0.30,
        "random_state": 42,
        "n_jobs": -1,
        "verbosity": -1,
        "force_col_wise": True,
    }
    model = lgb.LGBMRegressor(**parameters)
    categorical = ["station_id"] if "station_id" in feature_columns else []
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_valid, y_valid)],
        eval_metric=["rmse", "mae"],
        categorical_feature=categorical,
        callbacks=[
            lgb.early_stopping(250, first_metric_only=True, verbose=True),
            lgb.log_evaluation(100),
        ],
    )

    train_prediction = model.predict(
        X_train, num_iteration=model.best_iteration_
    ).astype("float32")
    valid_prediction = model.predict(
        X_valid, num_iteration=model.best_iteration_
    ).astype("float32")
    train_metrics = metrics(
        y_train.to_numpy(dtype=np.float64),
        train_prediction.astype(np.float64),
    )
    valid_metrics = metrics(
        y_valid.to_numpy(dtype=np.float64),
        valid_prediction.astype(np.float64),
    )

    predictions = valid[META_COLUMNS].copy()
    predictions["actual_high_c"] = y_valid.to_numpy(dtype="float32")
    predictions["predicted_high_c"] = valid_prediction
    predictions["error_c"] = (
        predictions["predicted_high_c"] - predictions["actual_high_c"]
    )
    predictions["abs_error_c"] = predictions["error_c"].abs()
    predictions.to_csv(OUTPUT_DIR / "validation_predictions_c.csv", index=False)

    reports = {
        "validation_by_station_c.csv": ["station"],
        "validation_by_validation_year_c.csv": ["validation_year"],
        "validation_by_fold_c.csv": ["fold_id"],
        "validation_by_local_hour_c.csv": ["local_hour"],
        "validation_by_local_half_hour_c.csv": ["local_hour", "local_minute"],
        "validation_by_station_fold_c.csv": ["station", "fold_id"],
    }
    for filename, groups in reports.items():
        grouped_report(predictions, groups).to_csv(
            OUTPUT_DIR / filename, index=False
        )

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
    importance.to_csv(OUTPUT_DIR / "feature_importance_c.csv", index=False)
    joblib.dump(model, OUTPUT_DIR / "nonus_daily_high_c_lightgbm.pkl")
    model.booster_.save_model(OUTPUT_DIR / "nonus_daily_high_c_lightgbm.txt")
    schema = {
        "model_type": "LightGBM LGBMRegressor",
        "target": TARGET,
        "target_unit": "Celsius",
        "feature_order": feature_columns,
        "categorical_features": categorical,
        "missing_value": "NaN",
    }
    (OUTPUT_DIR / "service_feature_schema_c.json").write_text(
        json.dumps(schema, indent=2), encoding="utf-8"
    )
    result = {
        "model_scope": "independent NONUS Celsius service",
        "feature_count": len(feature_columns),
        "best_iteration": int(model.best_iteration_),
        "parameters": parameters,
        "train": train_metrics,
        "validation": valid_metrics,
    }
    (OUTPUT_DIR / "metrics_c.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
