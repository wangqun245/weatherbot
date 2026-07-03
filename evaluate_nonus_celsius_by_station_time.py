from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MODEL_DIR = ROOT / "NONUS" / "newC" / "independent_model_c"
PREDICTIONS = MODEL_DIR / "validation_predictions_c.csv"
MIN_RECOMMENDATION_SAMPLES = 100


def metrics(part: pd.DataFrame) -> dict[str, float | int]:
    error = part["error_c"].to_numpy(dtype=np.float64)
    absolute = np.abs(error)
    return {
        "count": int(len(part)),
        "bias_mean_c": float(np.mean(error)),
        "mae_c": float(np.mean(absolute)),
        "rmse_c": float(np.sqrt(np.mean(error * error))),
        "median_abs_error_c": float(np.median(absolute)),
        "p75_abs_error_c": float(np.quantile(absolute, 0.75)),
        "p90_abs_error_c": float(np.quantile(absolute, 0.90)),
        "p95_abs_error_c": float(np.quantile(absolute, 0.95)),
        "within_0_5c_pct": float(np.mean(absolute <= 0.5) * 100.0),
        "within_1c_pct": float(np.mean(absolute <= 1.0) * 100.0),
        "within_1_5c_pct": float(np.mean(absolute <= 1.5) * 100.0),
        "within_2c_pct": float(np.mean(absolute <= 2.0) * 100.0),
    }


def snap_to_station_schedule(
    predictions: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, tuple[int, int]]]:
    scheduled_minutes = {}
    for station, part in predictions.groupby("station", observed=True):
        primary = int(part["local_minute"].value_counts().index[0])
        scheduled_minutes[station] = (primary, (primary + 30) % 60)

    def snap(row: pd.Series) -> tuple[int, int]:
        current = int(row["local_hour"]) * 60 + int(row["local_minute"])
        candidates = []
        for hour in range(24):
            for minute in scheduled_minutes[row["station"]]:
                target = hour * 60 + minute
                distance = min(
                    abs(target - current), 1440 - abs(target - current)
                )
                candidates.append((distance, target))
        _distance, target = min(candidates)
        target %= 1440
        return target // 60, target % 60

    snapped = predictions.apply(snap, axis=1, result_type="expand")
    output = predictions.copy()
    output["scheduled_local_hour"] = snapped[0].astype(int)
    output["scheduled_local_minute"] = snapped[1].astype(int)
    return output, scheduled_minutes


def grouped_report(
    frame: pd.DataFrame, group_columns: list[str]
) -> pd.DataFrame:
    rows = []
    grouper = group_columns[0] if len(group_columns) == 1 else group_columns
    for key, part in frame.groupby(grouper, sort=True, observed=True):
        values = key if isinstance(key, tuple) else (key,)
        row = dict(zip(group_columns, values))
        row.update(metrics(part))
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> int:
    predictions = pd.read_csv(PREDICTIONS, low_memory=False)
    predictions, scheduled_minutes = snap_to_station_schedule(predictions)

    hourly = grouped_report(
        predictions, ["station", "scheduled_local_hour"]
    ).rename(columns={"scheduled_local_hour": "local_hour"})
    hourly["local_hour_label"] = hourly["local_hour"].map(
        lambda value: f"{int(value):02d}:00-{int(value):02d}:59"
    )
    hourly["mae_rank_within_station"] = hourly.groupby("station")[
        "mae_c"
    ].rank(method="first").astype(int)
    hourly.to_csv(
        MODEL_DIR / "validation_by_station_local_hour_c.csv", index=False
    )

    halfhour = grouped_report(
        predictions,
        ["station", "scheduled_local_hour", "scheduled_local_minute"],
    ).rename(
        columns={
            "scheduled_local_hour": "local_hour",
            "scheduled_local_minute": "local_minute",
        }
    )
    halfhour["local_time"] = halfhour.apply(
        lambda row: f"{int(row.local_hour):02d}:{int(row.local_minute):02d}",
        axis=1,
    )
    halfhour["mae_rank_within_station"] = halfhour.groupby("station")[
        "mae_c"
    ].rank(method="first").astype(int)
    halfhour.to_csv(
        MODEL_DIR / "validation_by_station_local_half_hour_c.csv",
        index=False,
    )

    recommendations = {}
    for station, part in halfhour.groupby("station", sort=True):
        supported = part[part["count"] >= MIN_RECOMMENDATION_SAMPLES]
        chronological = supported.sort_values(["local_hour", "local_minute"])
        reliable = chronological[
            (chronological["mae_c"] <= 0.6)
            & (chronological["within_1c_pct"] >= 90.0)
        ]
        best = supported.sort_values(
            ["mae_c", "rmse_c", "local_hour", "local_minute"]
        ).head(5)
        earliest = reliable.head(1)
        columns = [
            "local_time", "count", "bias_mean_c", "mae_c", "rmse_c",
            "median_abs_error_c", "p75_abs_error_c", "p90_abs_error_c",
            "p95_abs_error_c", "within_0_5c_pct", "within_1c_pct",
            "within_1_5c_pct", "within_2c_pct",
        ]
        recommendations[station] = {
            "scheduled_minutes": list(scheduled_minutes[station]),
            "minimum_sample_count": MIN_RECOMMENDATION_SAMPLES,
            "reliable_definition": "MAE <= 0.6C and within 1C >= 90%",
            "earliest_reliable_time": (
                None
                if earliest.empty
                else earliest.iloc[0][columns].to_dict()
            ),
            "best_five_by_mae": best[columns].to_dict(orient="records"),
        }

    (MODEL_DIR / "station_trade_time_recommendations_c.json").write_text(
        json.dumps(recommendations, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        f"Hourly groups: {len(hourly)}; half-hour groups: {len(halfhour)}"
    )
    print(json.dumps(recommendations, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
