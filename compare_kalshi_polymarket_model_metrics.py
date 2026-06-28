from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


CITY_BY_STATION = {
    "KATL": "Atlanta",
    "KAUS": "Austin",
    "KBOS": "Boston",
    "KDCA": "Washington DC",
    "KDFW": "Dallas",
    "KLAS": "Las Vegas",
    "KMDW": "Chicago",
    "KMIA": "Miami",
    "KMSP": "Minneapolis",
    "KNYC": "NYC",
    "KOKC": "Oklahoma City",
    "KPHL": "Philadelphia",
    "KPHX": "Phoenix",
    "KSAT": "San Antonio",
    "KSEA": "Seattle",
    "KSFO": "San Francisco",
}

DEFAULT_KALSHI = Path(
    "kalshi/models/"
    "rolling_6y_holdout_lag10_speci_two_asos_16stations_20260628/"
    "rolling_6y_validation_predictions.csv"
)
DEFAULT_POLYMARKET = Path(
    "models/"
    "lightgbm_rolling_6y_holdout_24h_lag10_speci_context_regular_20260626/"
    "rolling_6y_validation_predictions.csv"
)
DEFAULT_OUTPUT_DIR = Path(
    "kalshi/models/"
    "rolling_6y_holdout_lag10_speci_two_asos_16stations_20260628/"
    "comparison_with_online_polymarket"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kalshi-predictions", type=Path, default=DEFAULT_KALSHI)
    parser.add_argument(
        "--polymarket-predictions", type=Path, default=DEFAULT_POLYMARKET
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def summarize(frame: pd.DataFrame) -> pd.Series:
    error = frame["predicted_high_f"] - frame["actual_high_f"]
    absolute = error.abs()
    return pd.Series(
        {
            "count": len(frame),
            "bias_f": error.mean(),
            "mae_f": absolute.mean(),
            "rmse_f": np.sqrt(np.mean(error * error)),
            "within_1f_pct": (absolute <= 1).mean() * 100,
            "within_2f_pct": (absolute <= 2).mean() * 100,
        }
    )


def grouped(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    return (
        frame.groupby(keys, sort=True)
        .apply(summarize, include_groups=False)
        .reset_index()
    )


def main() -> int:
    args = parse_args()
    kalshi = pd.read_csv(args.kalshi_predictions)
    polymarket = pd.read_csv(args.polymarket_predictions)
    common_years = sorted(
        set(kalshi["local_year"].unique())
        & set(polymarket["local_year"].unique())
    )
    kalshi = kalshi[kalshi["local_year"].isin(common_years)].copy()
    polymarket = polymarket[
        polymarket["local_year"].isin(common_years)
    ].copy()
    kalshi["city"] = kalshi["station"].map(CITY_BY_STATION)
    polymarket["city"] = polymarket["station"].map(CITY_BY_STATION)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    kalshi_hour = grouped(
        kalshi, ["city", "station", "local_hour"]
    )
    kalshi_hour.to_csv(
        args.output_dir / "kalshi_by_city_station_hour.csv", index=False
    )

    common_stations = sorted(
        set(kalshi["station"].unique())
        & set(polymarket["station"].unique())
    )
    new_common = grouped(
        kalshi[kalshi["station"].isin(common_stations)],
        ["city", "station", "local_hour"],
    ).rename(
        columns={
            column: f"kalshi_{column}"
            for column in [
                "count",
                "bias_f",
                "mae_f",
                "rmse_f",
                "within_1f_pct",
                "within_2f_pct",
            ]
        }
    )
    old_common = grouped(
        polymarket[polymarket["station"].isin(common_stations)],
        ["city", "station", "local_hour"],
    ).rename(
        columns={
            column: f"polymarket_{column}"
            for column in [
                "count",
                "bias_f",
                "mae_f",
                "rmse_f",
                "within_1f_pct",
                "within_2f_pct",
            ]
        }
    )
    comparison = new_common.merge(
        old_common, on=["city", "station", "local_hour"], how="inner"
    )
    comparison["mae_change_f"] = (
        comparison["kalshi_mae_f"] - comparison["polymarket_mae_f"]
    )
    comparison["mae_improvement_pct"] = (
        (comparison["polymarket_mae_f"] - comparison["kalshi_mae_f"])
        / comparison["polymarket_mae_f"]
        * 100
    )
    comparison["rmse_change_f"] = (
        comparison["kalshi_rmse_f"] - comparison["polymarket_rmse_f"]
    )
    comparison["rmse_improvement_pct"] = (
        (comparison["polymarket_rmse_f"] - comparison["kalshi_rmse_f"])
        / comparison["polymarket_rmse_f"]
        * 100
    )
    comparison.to_csv(
        args.output_dir
        / "kalshi_vs_online_polymarket_common_station_hour.csv",
        index=False,
    )

    hours = range(12, 19)
    period_rows = []
    for station in sorted(kalshi["station"].unique()):
        new_part = kalshi[
            (kalshi["station"] == station)
            & kalshi["local_hour"].isin(hours)
        ]
        new_metrics = summarize(new_part)
        row = {
            "city": CITY_BY_STATION.get(station, station),
            "station": station,
            **{f"kalshi_{key}": value for key, value in new_metrics.items()},
        }
        if station in common_stations:
            old_part = polymarket[
                (polymarket["station"] == station)
                & polymarket["local_hour"].isin(hours)
            ]
            old_metrics = summarize(old_part)
            row.update(
                {
                    f"polymarket_{key}": value
                    for key, value in old_metrics.items()
                }
            )
            row["mae_improvement_pct"] = (
                (old_metrics["mae_f"] - new_metrics["mae_f"])
                / old_metrics["mae_f"]
                * 100
            )
            row["rmse_improvement_pct"] = (
                (old_metrics["rmse_f"] - new_metrics["rmse_f"])
                / old_metrics["rmse_f"]
                * 100
            )
        period_rows.append(row)
    pd.DataFrame(period_rows).to_csv(
        args.output_dir / "local_12_18_summary.csv", index=False
    )
    print(f"Common validation years: {common_years}")
    print(f"Common stations: {common_stations}")
    print(f"Outputs: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
