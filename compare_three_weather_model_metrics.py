from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_NEW_KALSHI = Path(
    "kalshi/models/"
    "rolling_6y_nws_cli_polymarket_context_18stations_20260702/"
    "rolling_6y_validation_predictions.csv"
)
DEFAULT_ONLINE_KALSHI = Path(
    "kalshi/models/"
    "rolling_6y_holdout_lag10_speci_two_asos_16stations_20260628/"
    "rolling_6y_validation_predictions.csv"
)
DEFAULT_ONLINE_POLYMARKET = Path(
    "models/"
    "lightgbm_rolling_6y_holdout_24h_lag10_speci_context_regular_20260626/"
    "rolling_6y_validation_predictions.csv"
)
DEFAULT_OUTPUT_DIR = Path(
    "kalshi/models/"
    "rolling_6y_nws_cli_polymarket_context_18stations_20260702/"
    "comparison_with_online_models"
)

CITY_BY_STATION = {
    "KATL": "Atlanta",
    "KAUS": "Austin",
    "KBOS": "Boston",
    "KDCA": "Washington DC",
    "KDEN": "Denver",
    "KDFW": "Dallas",
    "KHOU": "Houston",
    "KLAS": "Las Vegas",
    "KLAX": "Los Angeles",
    "KMDW": "Chicago",
    "KMIA": "Miami",
    "KMSP": "Minneapolis",
    "KOKC": "Oklahoma City",
    "KPHL": "Philadelphia",
    "KPHX": "Phoenix",
    "KSAT": "San Antonio",
    "KSEA": "Seattle",
    "KSFO": "San Francisco",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-kalshi", type=Path, default=DEFAULT_NEW_KALSHI)
    parser.add_argument(
        "--online-kalshi", type=Path, default=DEFAULT_ONLINE_KALSHI
    )
    parser.add_argument(
        "--online-polymarket",
        type=Path,
        default=DEFAULT_ONLINE_POLYMARKET,
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    error = frame["predicted_high_f"] - frame["actual_high_f"]
    absolute = error.abs()
    return {
        "count": int(len(frame)),
        "bias_mean_f": float(error.mean()),
        "mae_f": float(absolute.mean()),
        "median_abs_error_f": float(absolute.median()),
        "rmse_f": float(np.sqrt(np.mean(error * error))),
        "p90_abs_error_f": float(absolute.quantile(0.90)),
        "within_1f_pct": float((absolute <= 1).mean() * 100),
        "within_2f_pct": float((absolute <= 2).mean() * 100),
        "within_3f_pct": float((absolute <= 3).mean() * 100),
    }


def summarize(
    frame: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    rows = []
    for key, part in frame.groupby(keys, sort=True):
        values = key if isinstance(key, tuple) else (key,)
        row = dict(zip(keys, values))
        row.update(metrics(part))
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    inputs = {
        "new_kalshi_nws_cli": (
            args.new_kalshi,
            "NWS CLI YESTERDAY MAXIMUM",
        ),
        "online_kalshi": (
            args.online_kalshi,
            "legacy METAR-derived Kalshi label",
        ),
        "online_polymarket": (
            args.online_polymarket,
            "legacy Polymarket METAR-derived label",
        ),
    }
    frames = []
    metadata: dict[str, object] = {"models": {}}
    for model, (path, label) in inputs.items():
        frame = pd.read_csv(path)
        frame["model"] = model
        frame["label_definition"] = label
        frame["city"] = frame["station"].map(CITY_BY_STATION).fillna(
            frame["station"]
        )
        frames.append(frame)
        metadata["models"][model] = {
            "predictions": str(path),
            "label_definition": label,
            "validation_years": sorted(
                map(int, frame["local_year"].unique())
            ),
            "stations": sorted(frame["station"].unique()),
        }

    common_years = sorted(
        set.intersection(
            *(set(frame["local_year"].unique()) for frame in frames)
        )
    )
    common_stations = sorted(
        set.intersection(
            *(set(frame["station"].unique()) for frame in frames)
        )
    )
    metadata["common_validation_years"] = list(map(int, common_years))
    metadata["common_stations"] = common_stations
    metadata["comparison_warning"] = (
        "The online Kalshi and Polymarket artifacts use legacy target "
        "definitions. Their historical validation metrics are useful "
        "baselines but are not same-label A/B evaluations against the new "
        "NWS CLI target."
    )

    combined = pd.concat(frames, ignore_index=True)
    comparable_years = combined[
        combined["local_year"].isin(common_years)
    ].copy()
    common = comparable_years[
        comparable_years["station"].isin(common_stations)
    ].copy()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    overall_rows = []
    common_rows = []
    for model, part in comparable_years.groupby("model", sort=True):
        row = {
            "model": model,
            "label_definition": part["label_definition"].iloc[0],
            "station_count": int(part["station"].nunique()),
            **metrics(part),
        }
        overall_rows.append(row)
    for model, part in common.groupby("model", sort=True):
        common_rows.append(
            {
                "model": model,
                "label_definition": part["label_definition"].iloc[0],
                "station_count": int(part["station"].nunique()),
                **metrics(part),
            }
        )

    pd.DataFrame(overall_rows).to_csv(
        args.output_dir / "overall_common_years_native_stations.csv",
        index=False,
    )
    pd.DataFrame(common_rows).to_csv(
        args.output_dir / "overall_common_years_common_stations.csv",
        index=False,
    )
    summarize(
        comparable_years,
        ["model", "label_definition", "city", "station"],
    ).to_csv(args.output_dir / "by_model_city_station.csv", index=False)
    summarize(
        comparable_years,
        ["model", "label_definition", "city", "station", "local_hour"],
    ).to_csv(
        args.output_dir / "by_model_city_station_local_hour.csv",
        index=False,
    )
    summarize(
        common,
        ["model", "label_definition", "city", "station", "local_hour"],
    ).to_csv(
        args.output_dir / "common_station_hour_comparison_long.csv",
        index=False,
    )

    trading = comparable_years[
        comparable_years["local_hour"].between(12, 17)
    ]
    summarize(
        trading,
        ["model", "label_definition", "city", "station"],
    ).to_csv(
        args.output_dir / "trading_hours_12_17_by_station.csv",
        index=False,
    )
    (args.output_dir / "comparison_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Common years: {common_years}")
    print(f"Common stations: {common_stations}")
    print(pd.DataFrame(overall_rows).to_string(index=False))
    print(f"Outputs: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
