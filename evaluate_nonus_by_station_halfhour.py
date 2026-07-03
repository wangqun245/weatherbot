from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MODEL_DIR = ROOT / "NONUS" / "independent_model"
PREDICTIONS = MODEL_DIR / "validation_predictions.csv"


def metrics(part: pd.DataFrame) -> dict[str, float | int]:
    error = part["error_f"].to_numpy(dtype=np.float64)
    absolute = np.abs(error)
    return {
        "count": int(len(part)),
        "bias_mean_f": float(np.mean(error)),
        "mae_f": float(np.mean(absolute)),
        "rmse_f": float(np.sqrt(np.mean(error * error))),
        "median_abs_error_f": float(np.median(absolute)),
        "p90_abs_error_f": float(np.quantile(absolute, 0.90)),
        "p95_abs_error_f": float(np.quantile(absolute, 0.95)),
        "within_1f_pct": float(np.mean(absolute <= 1.0) * 100.0),
        "within_2f_pct": float(np.mean(absolute <= 2.0) * 100.0),
        "within_3f_pct": float(np.mean(absolute <= 3.0) * 100.0),
    }


def main() -> int:
    predictions = pd.read_csv(PREDICTIONS, low_memory=False)
    scheduled_minutes = {}
    for station, part in predictions.groupby("station", observed=True):
        counts = part["local_minute"].value_counts()
        primary = int(counts.index[0])
        scheduled_minutes[station] = (primary, (primary + 30) % 60)

    def snap_time(row: pd.Series) -> tuple[int, int]:
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

    snapped = predictions.apply(snap_time, axis=1, result_type="expand")
    predictions["scheduled_local_hour"] = snapped[0].astype(int)
    predictions["scheduled_local_minute"] = snapped[1].astype(int)
    rows = []
    for (station, hour, minute), part in predictions.groupby(
        ["station", "scheduled_local_hour", "scheduled_local_minute"],
        sort=True,
        observed=True,
    ):
        row = {
            "station": station,
            "local_hour": int(hour),
            "local_minute": int(minute),
            "local_time": f"{int(hour):02d}:{int(minute):02d}",
        }
        row.update(metrics(part))
        rows.append(row)

    report = pd.DataFrame(rows)
    report["mae_rank_within_station"] = report.groupby("station")[
        "mae_f"
    ].rank(method="first", ascending=True).astype(int)
    report["rmse_rank_within_station"] = report.groupby("station")[
        "rmse_f"
    ].rank(method="first", ascending=True).astype(int)
    report.to_csv(
        MODEL_DIR / "validation_by_station_local_half_hour.csv",
        index=False,
    )

    recommendations = {}
    for station, part in report.groupby("station", sort=True):
        chronological = part.sort_values(["local_hour", "local_minute"])
        supported = chronological[chronological["count"] >= 100]
        practical = supported[
            (supported["mae_f"] <= 1.0)
            & (supported["within_2f_pct"] >= 90.0)
        ]
        best = supported.sort_values(
            ["mae_f", "rmse_f", "local_hour", "local_minute"]
        ).head(5)
        earliest = practical.head(1)
        recommendations[station] = {
            "scheduled_minutes": list(scheduled_minutes[station]),
            "minimum_recommendation_sample_count": 100,
            "earliest_mae_le_1f_and_within_2f_ge_90pct": (
                None
                if earliest.empty
                else earliest.iloc[0][
                    [
                        "local_time",
                        "count",
                        "mae_f",
                        "rmse_f",
                        "within_1f_pct",
                        "within_2f_pct",
                        "within_3f_pct",
                    ]
                ].to_dict()
            ),
            "best_five_by_mae": best[
                [
                    "local_time",
                    "count",
                    "mae_f",
                    "rmse_f",
                    "bias_mean_f",
                    "p90_abs_error_f",
                    "within_1f_pct",
                    "within_2f_pct",
                    "within_3f_pct",
                ]
            ].to_dict(orient="records"),
        }

    (MODEL_DIR / "station_halfhour_trade_time_recommendations.json").write_text(
        json.dumps(recommendations, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(recommendations, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
