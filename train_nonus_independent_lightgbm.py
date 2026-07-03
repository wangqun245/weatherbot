from __future__ import annotations

import gc
import json
from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


ROOT = Path(__file__).resolve().parent
SPLIT_DIR = ROOT / "NONUS" / "year_folds_6_to_1"
TRAIN_FILE = SPLIT_DIR / "nonus_train_year_folds.csv"
VALIDATION_FILE = SPLIT_DIR / "nonus_validation_year_folds.csv"
OUTPUT_DIR = ROOT / "NONUS" / "independent_model"
TARGET = "daily_high_f"
METADATA_COLUMNS = {
    "fold_id",
    "validation_year",
    "split",
    TARGET,
    "station",
    "valid",
    "metar",
    "local_year",
    "valid_utc_epoch",
}
VALIDATION_METADATA = [
    "fold_id",
    "validation_year",
    "station",
    "valid",
    "local_year",
    "local_month",
    "local_day",
    "local_hour",
    "local_minute",
]


def metric_values(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float | int]:
    error = predicted - actual
    absolute = np.abs(error)
    return {
        "count": int(len(actual)),
        "rmse_f": float(np.sqrt(mean_squared_error(actual, predicted))),
        "mae_f": float(mean_absolute_error(actual, predicted)),
        "r2": float(r2_score(actual, predicted)) if len(actual) > 1 else float("nan"),
        "bias_mean_f": float(np.mean(error)),
        "median_abs_error_f": float(np.median(absolute)),
        "p90_abs_error_f": float(np.quantile(absolute, 0.90)),
        "p95_abs_error_f": float(np.quantile(absolute, 0.95)),
        "within_1f_pct": float(np.mean(absolute <= 1.0) * 100.0),
        "within_2f_pct": float(np.mean(absolute <= 2.0) * 100.0),
        "within_3f_pct": float(np.mean(absolute <= 3.0) * 100.0),
    }


def grouped_metrics(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    output = []
    grouper = group_columns[0] if len(group_columns) == 1 else group_columns
    for key, part in frame.groupby(grouper, sort=True, observed=True):
        key_values = key if isinstance(key, tuple) else (key,)
        row = dict(zip(group_columns, key_values))
        row.update(
            metric_values(
                part["actual_high_f"].to_numpy(dtype=np.float64),
                part["predicted_high_f"].to_numpy(dtype=np.float64),
            )
        )
        output.append(row)
    return pd.DataFrame(output)


def main() -> int:
    if not TRAIN_FILE.exists() or not VALIDATION_FILE.exists():
        raise SystemExit("Training or validation split file is missing")
    if OUTPUT_DIR.exists():
        raise SystemExit(f"Output directory already exists: {OUTPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True)

    header = pd.read_csv(TRAIN_FILE, nrows=0).columns.tolist()
    feature_columns = [
        column for column in header if column not in METADATA_COLUMNS
    ]
    numeric_columns = [TARGET, *feature_columns]
    print(f"Features: {len(feature_columns)}")
    print("Loading training numeric matrix")
    train = pd.read_csv(
        TRAIN_FILE,
        usecols=numeric_columns,
        dtype={column: "float32" for column in numeric_columns},
        low_memory=False,
    )
    print("Loading validation numeric matrix and metadata")
    validation_usecols = list(
        dict.fromkeys([*numeric_columns, *VALIDATION_METADATA])
    )
    validation_dtypes = {
        column: "float32"
        for column in numeric_columns
        if column not in VALIDATION_METADATA
    }
    validation = pd.read_csv(
        VALIDATION_FILE,
        usecols=validation_usecols,
        dtype=validation_dtypes,
        low_memory=False,
    )

    X_train = train[feature_columns]
    y_train = train[TARGET]
    X_valid = validation[feature_columns]
    y_valid = validation[TARGET].astype("float32")
    print(
        f"Train rows: {len(train):,}; validation rows: {len(validation):,}"
    )

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
    validation_prediction = model.predict(
        X_valid, num_iteration=model.best_iteration_
    ).astype("float32")
    overall_train = metric_values(
        y_train.to_numpy(dtype=np.float64),
        train_prediction.astype(np.float64),
    )
    overall_validation = metric_values(
        y_valid.to_numpy(dtype=np.float64),
        validation_prediction.astype(np.float64),
    )

    prediction_columns = VALIDATION_METADATA.copy()
    predictions = validation[prediction_columns].copy()
    predictions["actual_high_f"] = y_valid.to_numpy(dtype="float32")
    predictions["predicted_high_f"] = validation_prediction
    predictions["error_f"] = (
        predictions["predicted_high_f"] - predictions["actual_high_f"]
    )
    predictions["abs_error_f"] = predictions["error_f"].abs()
    predictions.to_csv(
        OUTPUT_DIR / "validation_predictions.csv", index=False
    )

    reports = {
        "validation_by_station.csv": ["station"],
        "validation_by_validation_year.csv": ["validation_year"],
        "validation_by_fold.csv": ["fold_id"],
        "validation_by_local_hour.csv": ["local_hour"],
        "validation_by_local_half_hour.csv": ["local_hour", "local_minute"],
        "validation_by_station_fold.csv": ["station", "fold_id"],
    }
    for filename, groups in reports.items():
        grouped_metrics(predictions, groups).to_csv(
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
    importance.to_csv(OUTPUT_DIR / "feature_importance.csv", index=False)

    joblib.dump(model, OUTPUT_DIR / "nonus_daily_high_lightgbm.pkl")
    model.booster_.save_model(
        OUTPUT_DIR / "nonus_daily_high_lightgbm.txt"
    )
    service_schema = {
        "model_type": "LightGBM LGBMRegressor",
        "target": TARGET,
        "feature_order": feature_columns,
        "categorical_features": categorical,
        "missing_value": "NaN",
    }
    (OUTPUT_DIR / "service_feature_schema.json").write_text(
        json.dumps(service_schema, indent=2), encoding="utf-8"
    )

    metrics = {
        "model_scope": "independent NONUS service",
        "train_file": str(TRAIN_FILE),
        "validation_file": str(VALIDATION_FILE),
        "feature_count": len(feature_columns),
        "best_iteration": int(model.best_iteration_ or parameters["n_estimators"]),
        "parameters": parameters,
        "train": overall_train,
        "validation": overall_validation,
    }
    (OUTPUT_DIR / "metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8"
    )
    print(json.dumps(metrics, indent=2))
    del train, validation, X_train, X_valid
    gc.collect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
