from __future__ import annotations

import csv
import io
import json
import math
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "polymarket_weather_config.json"
OUT = ROOT / "outputs" / "strategy_twc_analysis"

IEM_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    "network=CA_ASOS&station=LAX&data=metar&year1=2026&month1=6&day1=1"
    "&year2=2026&month2=6&day2=26&tz=Etc%2FUTC&format=onlycomma&latlon=no"
    "&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4"
)

T_RE = re.compile(r"\bT(?P<t>[01]\d{3}|////)(?P<d>[01]\d{3}|////)\b")
MAIN_TEMP_RE = re.compile(r"\b(?P<t>M?\d{2})/(?P<d>M?\d{2})\b")


def fetch_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 compare-lax-iem-twc",
            "Accept": "text/csv,application/json,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8")


def parse_metar_temp_c(metar: str) -> float | None:
    match = T_RE.search(metar)
    if match and match.group("t") != "////":
        raw = match.group("t")
        sign = -1 if raw[0] == "1" else 1
        return sign * (int(raw[1:]) / 10.0)
    match = MAIN_TEMP_RE.search(metar)
    if not match:
        return None
    raw = match.group("t")
    return -float(raw[1:]) if raw.startswith("M") else float(raw)


def c_to_f(value: float) -> float:
    return value * 9.0 / 5.0 + 32.0


def rounded_f_from_c(value: float) -> int:
    # TWC historical observations are integer degrees. Python's banker's round
    # would make x.5 handling surprising, so use ordinary half-up rounding.
    return int(math.floor(c_to_f(value) + 0.5))


def parse_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc)
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_iem() -> list[dict]:
    rows = []
    text = fetch_text(IEM_URL)
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        valid = datetime.strptime(row["valid"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        temp_c = parse_metar_temp_c(row["metar"])
        if temp_c is None:
            continue
        rows.append(
            {
                "utc": valid,
                "utc_minute": valid.strftime("%Y-%m-%d %H:%M"),
                "metar": row["metar"],
                "iem_temp_c": temp_c,
                "iem_temp_f_rounded": rounded_f_from_c(temp_c),
                "is_special_or_correction": not valid.minute == 53 or " COR " in f" {row['metar']} ",
            }
        )
    return rows


def load_twc() -> list[dict]:
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    api_key = config["api"]["twc_api_key"]
    params = urllib.parse.urlencode(
        {
            "apiKey": api_key,
            "units": "e",
            "startDate": "20260601",
            "endDate": "20260625",
        }
    )
    url = f"https://api.weather.com/v1/location/KLAX:9:US/observations/historical.json?{params}"
    payload = json.loads(fetch_text(url))
    rows = []
    for row in payload.get("observations") or []:
        if row.get("temp") is None:
            continue
        dt = parse_utc(row.get("valid_time_gmt") or row.get("expire_time_gmt") or row.get("obsTime"))
        if dt is None:
            continue
        rows.append(
            {
                "utc": dt,
                "utc_minute": dt.strftime("%Y-%m-%d %H:%M"),
                "twc_temp_f": int(row["temp"]),
                "raw": row,
            }
        )
    return sorted(rows, key=lambda x: x["utc"])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tz = ZoneInfo("America/Los_Angeles")
    iem = load_iem()
    twc = load_twc()

    iem_by_ts = {r["utc_minute"]: r for r in iem}
    twc_by_ts = {r["utc_minute"]: r for r in twc}
    common = sorted(set(iem_by_ts) & set(twc_by_ts))
    mismatches = []
    for ts in common:
        if iem_by_ts[ts]["iem_temp_f_rounded"] != twc_by_ts[ts]["twc_temp_f"]:
            mismatches.append(
                {
                    "utc_minute": ts,
                    "local_minute": iem_by_ts[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                    "iem_temp_c": iem_by_ts[ts]["iem_temp_c"],
                    "iem_temp_f_rounded": iem_by_ts[ts]["iem_temp_f_rounded"],
                    "twc_temp_f": twc_by_ts[ts]["twc_temp_f"],
                    "metar": iem_by_ts[ts]["metar"],
                }
            )

    only_iem = sorted(set(iem_by_ts) - set(twc_by_ts))
    only_twc = sorted(set(twc_by_ts) - set(iem_by_ts))

    daily: dict[str, dict] = {}
    for source, rows, temp_key in (("iem", iem, "iem_temp_f_rounded"), ("twc", twc, "twc_temp_f")):
        for r in rows:
            day = r["utc"].astimezone(tz).date().isoformat()
            daily.setdefault(day, {})
            daily[day].setdefault(source, []).append((r["utc_minute"], r[temp_key]))

    daily_rows = []
    for day in sorted(daily):
        i_vals = daily[day].get("iem", [])
        t_vals = daily[day].get("twc", [])
        i_high = max((v for _, v in i_vals), default=None)
        t_high = max((v for _, v in t_vals), default=None)
        i_low = min((v for _, v in i_vals), default=None)
        t_low = min((v for _, v in t_vals), default=None)
        daily_rows.append(
            {
                "local_date": day,
                "iem_count": len(i_vals),
                "twc_count": len(t_vals),
                "iem_high_f": i_high,
                "twc_high_f": t_high,
                "high_diff_twc_minus_iem": None if i_high is None or t_high is None else t_high - i_high,
                "iem_low_f": i_low,
                "twc_low_f": t_low,
                "low_diff_twc_minus_iem": None if i_low is None or t_low is None else t_low - i_low,
            }
        )

    detail_path = OUT / "lax_iem_twc_point_comparison_20260601_20260625.csv"
    with detail_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "utc_minute",
                "local_minute",
                "iem_temp_c",
                "iem_temp_f_rounded",
                "twc_temp_f",
                "metar",
            ],
        )
        writer.writeheader()
        writer.writerows(mismatches)

    daily_path = OUT / "lax_iem_twc_daily_comparison_20260601_20260625.csv"
    with daily_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(daily_rows[0].keys()))
        writer.writeheader()
        writer.writerows(daily_rows)

    summary = {
        "iem_rows": len(iem),
        "twc_rows": len(twc),
        "iem_special_or_correction_rows": sum(1 for r in iem if r["is_special_or_correction"]),
        "common_timestamps": len(common),
        "point_mismatches": len(mismatches),
        "only_iem_count": len(only_iem),
        "only_twc_count": len(only_twc),
        "only_iem_sample": [
            {
                "utc_minute": ts,
                "local_minute": iem_by_ts[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                "temp_f": iem_by_ts[ts]["iem_temp_f_rounded"],
                "metar": iem_by_ts[ts]["metar"],
            }
            for ts in only_iem[:20]
        ],
        "only_twc_sample": [
            {
                "utc_minute": ts,
                "local_minute": twc_by_ts[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                "temp_f": twc_by_ts[ts]["twc_temp_f"],
            }
            for ts in only_twc[:20]
        ],
        "mismatch_sample": mismatches[:20],
        "daily_rows": daily_rows,
        "files": [str(detail_path), str(daily_path)],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
