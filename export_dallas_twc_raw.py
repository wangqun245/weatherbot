from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "outputs" / "strategy_twc_analysis"
TWC_RAW = ROOT / "polymarket_weather_twc_raw_wide (2).csv"


def temp_text(values: pd.Series) -> str:
    nums = pd.to_numeric(values, errors="coerce").dropna()
    return " | ".join(str(int(v) if float(v).is_integer() else v) for v in nums)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    header = pd.read_csv(TWC_RAW, nrows=0).columns.tolist()
    forecast_cols = [c for c in header if re.fullmatch(r"forecast_p\d{2}", c)]
    base_cols = [
        "city",
        "kind",
        "station",
        "target_date",
        "cycle_id",
        "observed_at_utc",
        "city_local_time",
        "city_timezone",
        "first_valid_time_local",
        "last_valid_time_local",
        "combined_high",
        "combined_low",
        "forecast_point_count",
        "observed_point_count",
        "event_market_unit",
        "temperature_unit",
        "raw_forecast_payload_json",
    ]
    df = pd.read_csv(TWC_RAW, usecols=base_cols + forecast_cols)
    dallas = df[df["city"].astype(str).str.lower().eq("dallas")].copy()
    dallas["observed_at_utc_dt"] = pd.to_datetime(
        dallas["observed_at_utc"], errors="coerce"
    )
    dallas = dallas.sort_values(["observed_at_utc_dt", "cycle_id"])
    dallas["wide_calc_high"] = dallas[forecast_cols].max(axis=1, skipna=True)
    dallas["wide_calc_low"] = dallas[forecast_cols].min(axis=1, skipna=True)
    dallas["wide_forecast_values"] = dallas[forecast_cols].apply(temp_text, axis=1)

    summary_cols = [
        "observed_at_utc",
        "city_local_time",
        "cycle_id",
        "kind",
        "station",
        "target_date",
        "first_valid_time_local",
        "last_valid_time_local",
        "combined_high",
        "combined_low",
        "wide_calc_high",
        "wide_calc_low",
        "forecast_point_count",
        "wide_forecast_values",
    ]
    dallas[summary_cols].to_csv(OUT / "dallas_twc_snapshot_summary.csv", index=False)
    unique_dallas = dallas.drop_duplicates(
        subset=["observed_at_utc", "city_local_time", "cycle_id", "raw_forecast_payload_json"]
    ).copy()
    unique_dallas[summary_cols].to_csv(
        OUT / "dallas_twc_unique_snapshot_summary.csv", index=False
    )

    arrays = [
        "validTimeLocal",
        "validTimeUtc",
        "temperature",
        "temperatureFeelsLike",
        "temperatureHeatIndex",
        "temperatureDewPoint",
        "relativeHumidity",
        "precipChance",
        "qpf",
        "windSpeed",
        "windGust",
        "wxPhraseLong",
        "wxPhraseShort",
        "dayOrNight",
        "cloudCover",
    ]
    rows: list[dict] = []
    for _, row in unique_dallas.iterrows():
        try:
            payload = (
                json.loads(row["raw_forecast_payload_json"])
                if isinstance(row["raw_forecast_payload_json"], str)
                and row["raw_forecast_payload_json"].strip()
                else {}
            )
        except json.JSONDecodeError:
            payload = {}
        n = max((len(payload.get(key) or []) for key in arrays), default=0)
        for idx in range(n):
            expanded = {
                "observed_at_utc": row["observed_at_utc"],
                "city_local_time": row["city_local_time"],
                "cycle_id": row["cycle_id"],
                "station": row["station"],
                "target_date": row["target_date"],
                "combined_high": row["combined_high"],
                "combined_low": row["combined_low"],
                "hour_index": idx + 1,
            }
            for key in arrays:
                values = payload.get(key) or []
                expanded[key] = values[idx] if idx < len(values) else None
            expanded["is_target_date"] = str(expanded.get("validTimeLocal") or "").startswith(
                str(row["target_date"])
            )
            rows.append(expanded)

    expanded = pd.DataFrame(rows)
    expanded.to_csv(OUT / "dallas_twc_raw_hourly_expanded.csv", index=False)

    target = expanded[expanded["is_target_date"] == True].copy()
    target["temperature_num"] = pd.to_numeric(target["temperature"], errors="coerce")
    by_snapshot = (
        target.groupby(["observed_at_utc", "city_local_time", "cycle_id"], dropna=False)
        .agg(
            raw_target_high=("temperature_num", "max"),
            raw_target_low=("temperature_num", "min"),
            target_hour_count=("temperature_num", "count"),
            target_hours=("validTimeLocal", lambda s: " | ".join(map(str, s))),
            target_temps=("temperature", temp_text),
        )
        .reset_index()
    )
    by_snapshot.to_csv(
        OUT / "dallas_twc_raw_target_date_by_snapshot.csv", index=False
    )

    high_counts = by_snapshot["raw_target_high"].value_counts(dropna=False).sort_index()
    high_86 = by_snapshot[by_snapshot["raw_target_high"] >= 86][
        [
            "observed_at_utc",
            "city_local_time",
            "cycle_id",
            "raw_target_high",
            "raw_target_low",
            "target_temps",
        ]
    ]
    print(
        json.dumps(
            {
                "raw_snapshot_rows": int(len(dallas)),
                "unique_snapshot_rows": int(len(unique_dallas)),
                "expanded_rows": int(len(expanded)),
                "target_date_rows": int(len(target)),
                "raw_target_high_counts": {
                    str(k): int(v) for k, v in high_counts.to_dict().items()
                },
                "rows_with_86_or_more": int(len(high_86)),
                "files": [
                    str(OUT / "dallas_twc_snapshot_summary.csv"),
                    str(OUT / "dallas_twc_unique_snapshot_summary.csv"),
                    str(OUT / "dallas_twc_raw_hourly_expanded.csv"),
                    str(OUT / "dallas_twc_raw_target_date_by_snapshot.csv"),
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(high_86.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
