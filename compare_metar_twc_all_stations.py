from __future__ import annotations

import argparse
import csv
import io
import json
import math
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "polymarket_weather_config.json"
DEFAULT_INPUT_DIR = Path(r"C:\weather\metar_history")
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "metar_twc_comparison"

STATION_TIMEZONES = {
    "KATL": "America/New_York",
    "KAUS": "America/Chicago",
    "KBKF": "America/Denver",
    "KDAL": "America/Chicago",
    "KHOU": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KLGA": "America/New_York",
    "KMIA": "America/New_York",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
}

T_RE = re.compile(r"\bT(?P<t>[01]\d{3}|////)(?P<d>[01]\d{3}|////)\b")
MAIN_TEMP_RE = re.compile(r"\b(?P<t>M?\d{2})/(?P<d>M?\d{2})\b")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare downloaded IEM METAR/SPECI temperatures against TWC historical observations."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-06-25")
    parser.add_argument("--stations", nargs="*", help="Optional ICAO stations to compare.")
    parser.add_argument("--sleep", type=float, default=0.15)
    return parser.parse_args()


def parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def load_api_key() -> str:
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    api_key = str(config["api"].get("twc_api_key") or "").strip()
    if not api_key:
        raise RuntimeError("Missing api.twc_api_key in polymarket_weather_config.json")
    return api_key


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


def rounded_f_from_c(value: float) -> int:
    return int(math.floor(value * 9.0 / 5.0 + 32.0 + 0.5))


def parse_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def metar_file_for_year(input_dir: Path, station: str, year: int) -> Path:
    return input_dir / station / f"{station}_{year}_metar.csv"


def load_metar_points(input_dir: Path, station: str, start: date, end: date) -> dict[str, dict]:
    points: dict[str, dict] = {}
    for year in range(start.year, end.year + 1):
        path = metar_file_for_year(input_dir, station, year)
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                valid_text = (row.get("valid") or "").strip()
                metar = (row.get("metar") or "").strip()
                if not valid_text or not metar:
                    continue
                try:
                    dt = datetime.strptime(valid_text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if dt.date() < start or dt.date() > end:
                    continue
                temp_c = parse_metar_temp_c(metar)
                if temp_c is None:
                    continue
                points[dt.strftime("%Y-%m-%d %H:%M")] = {
                    "utc": dt,
                    "metar_temp_c": temp_c,
                    "metar_temp_f": rounded_f_from_c(temp_c),
                    "metar": metar,
                    "is_extra_report": dt.minute != 53 or " COR " in f" {metar} ",
                }
    return points


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks = []
    current = start
    while current <= end:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(end, next_month - timedelta(days=1))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def fetch_json(url: str, params: dict) -> object:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        full_url,
        headers={
            "User-Agent": "Mozilla/5.0 metar-twc-compare",
            "Accept": "application/json,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def load_twc_points(station: str, api_key: str, start: date, end: date, sleep: float) -> dict[str, dict]:
    points: dict[str, dict] = {}
    base = f"https://api.weather.com/v1/location/{station}:9:US/observations/historical.json"
    for chunk_start, chunk_end in month_chunks(start, end):
        payload = fetch_json(
            base,
            {
                "apiKey": api_key,
                "units": "e",
                "startDate": chunk_start.strftime("%Y%m%d"),
                "endDate": chunk_end.strftime("%Y%m%d"),
            },
        )
        for row in (payload.get("observations") if isinstance(payload, dict) else []) or []:
            if row.get("temp") is None:
                continue
            dt = parse_utc(row.get("valid_time_gmt") or row.get("expire_time_gmt") or row.get("obsTime"))
            if dt is None or dt.date() < start or dt.date() > end:
                continue
            points[dt.strftime("%Y-%m-%d %H:%M")] = {
                "utc": dt,
                "twc_temp_f": int(row["temp"]),
                "raw": row,
            }
        time.sleep(sleep)
    return points


def local_daily(points: dict[str, dict], tz: ZoneInfo, temp_key: str) -> dict[str, dict]:
    grouped: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for point in points.values():
        day = point["utc"].astimezone(tz).date().isoformat()
        grouped[day].append((point["utc"].strftime("%Y-%m-%d %H:%M"), point[temp_key]))
    daily = {}
    for day, values in grouped.items():
        temps = [temp for _ts, temp in values]
        daily[day] = {
            "count": len(values),
            "high": max(temps),
            "low": min(temps),
        }
    return daily


def compare_station(station: str, input_dir: Path, output_dir: Path, api_key: str, start: date, end: date, sleep: float) -> dict:
    tz = ZoneInfo(STATION_TIMEZONES[station])
    metar = load_metar_points(input_dir, station, start, end)
    twc = load_twc_points(station, api_key, start, end, sleep)
    common = sorted(set(metar) & set(twc))
    only_metar = sorted(set(metar) - set(twc))
    only_twc = sorted(set(twc) - set(metar))

    mismatches = []
    for ts in common:
        if metar[ts]["metar_temp_f"] != twc[ts]["twc_temp_f"]:
            mismatches.append(
                {
                    "station": station,
                    "utc_minute": ts,
                    "local_minute": metar[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                    "metar_temp_c": metar[ts]["metar_temp_c"],
                    "metar_temp_f": metar[ts]["metar_temp_f"],
                    "twc_temp_f": twc[ts]["twc_temp_f"],
                    "metar": metar[ts]["metar"],
                }
            )

    metar_daily = local_daily(metar, tz, "metar_temp_f")
    twc_daily = local_daily(twc, tz, "twc_temp_f")
    common_metar = {ts: metar[ts] for ts in common}
    common_twc = {ts: twc[ts] for ts in common}
    common_metar_daily = local_daily(common_metar, tz, "metar_temp_f")
    common_twc_daily = local_daily(common_twc, tz, "twc_temp_f")
    daily_rows = []
    for day in sorted(set(metar_daily) | set(twc_daily)):
        m = metar_daily.get(day, {})
        t = twc_daily.get(day, {})
        cm = common_metar_daily.get(day, {})
        ct = common_twc_daily.get(day, {})
        m_high = m.get("high")
        t_high = t.get("high")
        m_low = m.get("low")
        t_low = t.get("low")
        cm_high = cm.get("high")
        ct_high = ct.get("high")
        cm_low = cm.get("low")
        ct_low = ct.get("low")
        daily_rows.append(
            {
                "station": station,
                "local_date": day,
                "metar_count": m.get("count", 0),
                "twc_count": t.get("count", 0),
                "metar_high_f": m_high,
                "twc_high_f": t_high,
                "high_diff_twc_minus_metar": None if m_high is None or t_high is None else t_high - m_high,
                "metar_low_f": m_low,
                "twc_low_f": t_low,
                "low_diff_twc_minus_metar": None if m_low is None or t_low is None else t_low - m_low,
                "common_count": cm.get("count", 0),
                "common_metar_high_f": cm_high,
                "common_twc_high_f": ct_high,
                "common_high_diff_twc_minus_metar": None if cm_high is None or ct_high is None else ct_high - cm_high,
                "common_metar_low_f": cm_low,
                "common_twc_low_f": ct_low,
                "common_low_diff_twc_minus_metar": None if cm_low is None or ct_low is None else ct_low - cm_low,
            }
        )

    mismatch_path = output_dir / f"{station}_point_mismatches_{start:%Y%m%d}_{end:%Y%m%d}.csv"
    with mismatch_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = ["station", "utc_minute", "local_minute", "metar_temp_c", "metar_temp_f", "twc_temp_f", "metar"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(mismatches)

    daily_path = output_dir / f"{station}_daily_comparison_{start:%Y%m%d}_{end:%Y%m%d}.csv"
    with daily_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(daily_rows[0].keys()))
        writer.writeheader()
        writer.writerows(daily_rows)

    return {
        "station": station,
        "metar_points": len(metar),
        "twc_points": len(twc),
        "metar_extra_report_points": sum(1 for point in metar.values() if point["is_extra_report"]),
        "common_points": len(common),
        "point_mismatches": len(mismatches),
        "only_metar_points": len(only_metar),
        "only_twc_points": len(only_twc),
        "daily_rows": len(daily_rows),
        "daily_high_mismatches": sum(
            1 for row in daily_rows if row["high_diff_twc_minus_metar"] not in (None, 0)
        ),
        "daily_low_mismatches": sum(
            1 for row in daily_rows if row["low_diff_twc_minus_metar"] not in (None, 0)
        ),
        "common_daily_high_mismatches": sum(
            1 for row in daily_rows if row["common_high_diff_twc_minus_metar"] not in (None, 0)
        ),
        "common_daily_low_mismatches": sum(
            1 for row in daily_rows if row["common_low_diff_twc_minus_metar"] not in (None, 0)
        ),
        "sample_point_mismatches": mismatches[:10],
        "sample_only_metar": [
            {
                "utc_minute": ts,
                "local_minute": metar[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                "metar_temp_f": metar[ts]["metar_temp_f"],
                "metar": metar[ts]["metar"],
            }
            for ts in only_metar[:10]
        ],
        "sample_only_twc": [
            {
                "utc_minute": ts,
                "local_minute": twc[ts]["utc"].astimezone(tz).strftime("%Y-%m-%d %H:%M"),
                "twc_temp_f": twc[ts]["twc_temp_f"],
            }
            for ts in only_twc[:10]
        ],
        "mismatch_file": str(mismatch_path),
        "daily_file": str(daily_path),
    }


def main() -> int:
    args = parse_args()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start > end:
        raise SystemExit("--start-date must be <= --end-date")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    requested = {station.upper() for station in (args.stations or [])}
    stations = [
        station
        for station in sorted(STATION_TIMEZONES)
        if (not requested or station in requested) and (args.input_dir / station).exists()
    ]
    if requested and not stations:
        raise SystemExit(f"No requested station directories found: {', '.join(sorted(requested))}")

    api_key = load_api_key()
    summaries = []
    for station in stations:
        print(f"Comparing {station} {start}..{end}")
        summaries.append(compare_station(station, args.input_dir, args.output_dir, api_key, start, end, args.sleep))

    summary_path = args.output_dir / f"summary_{start:%Y%m%d}_{end:%Y%m%d}.json"
    summary = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "station_count": len(summaries),
        "total_metar_points": sum(row["metar_points"] for row in summaries),
        "total_twc_points": sum(row["twc_points"] for row in summaries),
        "total_common_points": sum(row["common_points"] for row in summaries),
        "total_point_mismatches": sum(row["point_mismatches"] for row in summaries),
        "total_daily_high_mismatches": sum(row["daily_high_mismatches"] for row in summaries),
        "total_daily_low_mismatches": sum(row["daily_low_mismatches"] for row in summaries),
        "total_common_daily_high_mismatches": sum(row["common_daily_high_mismatches"] for row in summaries),
        "total_common_daily_low_mismatches": sum(row["common_daily_low_mismatches"] for row in summaries),
        "stations": summaries,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"summary_file": str(summary_path), **summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
