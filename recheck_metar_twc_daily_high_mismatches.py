from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import compare_metar_twc_all_stations as comparison


START = date(2020, 1, 1)
END = date(2026, 7, 1)
OUTPUT_DIR = comparison.DEFAULT_OUTPUT_DIR
STATIONS = (
    "RJTT", "RKSI", "ZSPD", "RCSS", "NZWN", "LEMD", "LFPB", "WSSS",
    "EGLC", "LTAC", "RKPK", "EDDM", "WMKK", "EFHK", "LIMC", "EPWA",
    "VILK", "OPKC", "EHAM",
)


def main() -> int:
    api_key = comparison.load_api_key()
    verified_rows: list[dict] = []

    for station in STATIONS:
        path = OUTPUT_DIR / (
            f"{station}_daily_comparison_{START:%Y%m%d}_{END:%Y%m%d}.csv"
        )
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        timezone_name, country_code = comparison.STATION_LOCATIONS[station]
        tz = ZoneInfo(timezone_name)
        candidates = [
            row for row in rows
            if (
                not row["metar_high_f"]
                or not row["twc_high_f"]
                or row["high_diff_twc_minus_metar"] not in ("", "0")
            )
        ]
        print(f"Rechecking {station}: {len(candidates)} candidate days")

        for row in candidates:
            local_day = date.fromisoformat(row["local_date"])
            error = ""
            try:
                twc_points = comparison.load_twc_points(
                    station, country_code, api_key, local_day, local_day, 0.0
                )
            except Exception as exc:
                print(f"  {station} {local_day}: {exc}")
                twc_points = {}
                error = f"{type(exc).__name__}: {exc}"
            twc_daily = comparison.local_daily(twc_points, tz, "twc_temp_f")
            verified_twc = twc_daily.get(row["local_date"], {})
            verified_high = verified_twc.get("high")
            metar_high = (
                int(row["metar_high_f"]) if row["metar_high_f"] else None
            )
            verified_rows.append(
                {
                    "station": station,
                    "local_date": row["local_date"],
                    "metar_count": row["metar_count"],
                    "original_twc_count": row["twc_count"],
                    "verified_twc_count": verified_twc.get("count", 0),
                    "metar_high_f": metar_high,
                    "original_twc_high_f": (
                        int(row["twc_high_f"]) if row["twc_high_f"] else None
                    ),
                    "verified_twc_high_f": verified_high,
                    "verified_diff_twc_minus_metar": (
                        None
                        if metar_high is None or verified_high is None
                        else verified_high - metar_high
                    ),
                    "error": error,
                }
            )

    detail_path = OUTPUT_DIR / (
        f"verified_daily_high_candidates_{START:%Y%m%d}_{END:%Y%m%d}.csv"
    )
    with detail_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(verified_rows[0]))
        writer.writeheader()
        writer.writerows(verified_rows)

    mismatches = [
        row for row in verified_rows
        if row["verified_diff_twc_minus_metar"] not in (None, 0)
    ]
    summary = {
        "candidate_days_rechecked": len(verified_rows),
        "verified_mismatch_days": len(mismatches),
        "verified_exact_candidate_days": sum(
            row["verified_diff_twc_minus_metar"] == 0 for row in verified_rows
        ),
        "still_missing_one_side": sum(
            row["verified_diff_twc_minus_metar"] is None for row in verified_rows
        ),
        "api_error_days": sum(bool(row["error"]) for row in verified_rows),
        "mismatches_by_station": dict(
            sorted(Counter(row["station"] for row in mismatches).items())
        ),
        "difference_counts": dict(
            sorted(
                Counter(
                    row["verified_diff_twc_minus_metar"] for row in mismatches
                ).items()
            )
        ),
        "detail_file": str(detail_path),
    }
    summary_path = OUTPUT_DIR / (
        f"verified_daily_high_summary_{START:%Y%m%d}_{END:%Y%m%d}.json"
    )
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
