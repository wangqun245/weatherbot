from __future__ import annotations

import csv
import json
import re
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "outputs" / "strategy_twc_analysis"
CONFIG = ROOT / "polymarket_weather_config.json"

US_CITIES = {
    "Atlanta",
    "Austin",
    "Chicago",
    "Dallas",
    "Denver",
    "Houston",
    "Los Angeles",
    "Miami",
    "NYC",
    "San Francisco",
    "Seattle",
}

CITY_TIMEZONES = {
    "Amsterdam": "Europe/Amsterdam",
    "Ankara": "Europe/Istanbul",
    "Beijing": "Asia/Shanghai",
    "Buenos Aires": "America/Argentina/Buenos_Aires",
    "Busan": "Asia/Seoul",
    "Cape Town": "Africa/Johannesburg",
    "Chengdu": "Asia/Shanghai",
    "Chongqing": "Asia/Shanghai",
    "Guangzhou": "Asia/Shanghai",
    "Helsinki": "Europe/Helsinki",
    "Hong Kong": "Asia/Hong_Kong",
    "Istanbul": "Europe/Istanbul",
    "Jeddah": "Asia/Riyadh",
    "Jinan": "Asia/Shanghai",
    "Karachi": "Asia/Karachi",
    "Kuala Lumpur": "Asia/Kuala_Lumpur",
    "London": "Europe/London",
    "Lucknow": "Asia/Kolkata",
    "Madrid": "Europe/Madrid",
    "Manila": "Asia/Manila",
    "Mexico City": "America/Mexico_City",
    "Milan": "Europe/Rome",
    "Moscow": "Europe/Moscow",
    "Munich": "Europe/Berlin",
    "Panama City": "America/Panama",
    "Paris": "Europe/Paris",
    "Qingdao": "Asia/Shanghai",
    "Sao Paulo": "America/Sao_Paulo",
    "Seoul": "Asia/Seoul",
    "Shanghai": "Asia/Shanghai",
    "Shenzhen": "Asia/Shanghai",
    "Singapore": "Asia/Singapore",
    "Taipei": "Asia/Taipei",
    "Tel Aviv": "Asia/Jerusalem",
    "Tokyo": "Asia/Tokyo",
    "Toronto": "America/Toronto",
    "Warsaw": "Europe/Warsaw",
    "Wellington": "Pacific/Auckland",
    "Wuhan": "Asia/Shanghai",
    "Zhengzhou": "Asia/Shanghai",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

TITLE_RE = re.compile(r"^(Highest|Lowest)\s+temperature\s+in\s+(.+?)\s+on\s+([A-Za-z]+\s+\d{1,2})(?:\?)?$", re.I)
WU_RE = re.compile(r"https?://(?:www\.)?wunderground\.com/history/daily/([a-z]{2})/[^/]+/([A-Za-z0-9]{4})", re.I)
NOAA_TIMESERIES_RE = re.compile(r"https?://www\.weather\.gov/wrh/timeseries\?site=([A-Za-z0-9]{4})", re.I)

MANUAL_SOURCES = {
    "Hong Kong": {
        "station": "VHHH",
        "country": "HK",
        "source_url": "https://www.weather.gov.hk/en/cis/climat.htm",
        "source_note": "Polymarket resolves on Hong Kong Observatory, but this comparison uses airport METAR/TWC at VHHH.",
    },
}


def load_config() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def get_json(url: str, params: dict | None = None) -> object:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=35) as resp:
        return json.loads(resp.read().decode("utf-8"))


def infer_year(month_day: str, today: date) -> date:
    parsed = datetime.strptime(f"{month_day} {today.year}", "%B %d %Y").date()
    if parsed < today - timedelta(days=180):
        parsed = datetime.strptime(f"{month_day} {today.year + 1}", "%B %d %Y").date()
    return parsed


def discover_non_us_temperature_events(today: date) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for base in ({"tag_slug": "weather"}, {"q": "Highest temperature"}, {"q": "Lowest temperature"}):
        for offset in range(0, 1200, 100):
            params = {"limit": 100, "offset": offset, "closed": "false", "archived": "false", **base}
            batch = get_json("https://gamma-api.polymarket.com/events", params)
            if not isinstance(batch, list) or not batch:
                break
            for event in batch:
                title = event.get("title") or event.get("question") or ""
                match = TITLE_RE.match(title.strip())
                if not match:
                    continue
                kind, city, month_day = match.group(1).title(), match.group(2).strip(), match.group(3)
                if city in US_CITIES:
                    continue
                event_date = infer_year(month_day, today)
                if event_date < today:
                    continue
                key = f"{city}|{kind}|{event_date.isoformat()}"
                found[key] = {
                    "city": city,
                    "kind": kind,
                    "event_date": event_date.isoformat(),
                    "title": title,
                    "slug": event.get("slug") or "",
                    "url": f"https://polymarket.com/event/{event.get('slug')}" if event.get("slug") else "",
                }
            if len(batch) < 100:
                break
            time.sleep(0.15)
    return found


def event_detail(slug: str) -> dict:
    value = get_json(f"https://gamma-api.polymarket.com/events/slug/{slug}")
    return value if isinstance(value, dict) else {}


def infer_unit(text: str) -> str:
    lower = text.lower()
    if "celsius" in lower:
        return "C"
    if "fahrenheit" in lower:
        return "F"
    return "C"


def city_metadata(events: dict[str, dict]) -> dict[str, dict]:
    by_city: dict[str, dict] = {}
    for event in sorted(events.values(), key=lambda x: (x["city"], x["event_date"], x["kind"])):
        city = event["city"]
        if city in by_city:
            by_city[city]["event_titles"].append(event["title"])
            by_city[city]["event_dates"].add(event["event_date"])
            by_city[city]["kinds"].add(event["kind"])
            continue
        detail = event_detail(event["slug"])
        text = " ".join(
            str(detail.get(k) or "") for k in ("description", "resolutionSource", "title")
        )
        for market in detail.get("markets") or []:
            text += " " + " ".join(str(market.get(k) or "") for k in ("description", "resolutionSource", "question"))
        match = WU_RE.search(text)
        noaa_match = NOAA_TIMESERIES_RE.search(text)
        manual = MANUAL_SOURCES.get(city)
        if not match:
            if noaa_match:
                country = {
                    "LTFM": "TR",
                    "UUWW": "RU",
                    "LLBG": "IL",
                }.get(noaa_match.group(1).upper(), "")
                by_city[city] = {
                    "city": city,
                    "station": noaa_match.group(1).upper(),
                    "country": country,
                    "unit": infer_unit(text),
                    "timezone": CITY_TIMEZONES.get(city, "UTC"),
                    "source_url": noaa_match.group(0),
                    "source_note": "Polymarket uses NOAA timeseries; compared against the same ICAO in AviationWeather and TWC.",
                    "event_titles": [event["title"]],
                    "event_dates": {event["event_date"]},
                    "kinds": {event["kind"]},
                }
                continue
            if manual:
                by_city[city] = {
                    "city": city,
                    "station": manual["station"],
                    "country": manual["country"],
                    "unit": infer_unit(text),
                    "timezone": CITY_TIMEZONES.get(city, "UTC"),
                    "source_url": manual["source_url"],
                    "source_note": manual["source_note"],
                    "event_titles": [event["title"]],
                    "event_dates": {event["event_date"]},
                    "kinds": {event["kind"]},
                }
                continue
            by_city[city] = {
                "city": city,
                "station": "",
                "country": "",
                "unit": infer_unit(text),
                "timezone": CITY_TIMEZONES.get(city, "UTC"),
                "source_url": "",
                "source_note": "No ICAO station could be parsed from the Polymarket resolution source.",
                "event_titles": [event["title"]],
                "event_dates": {event["event_date"]},
                "kinds": {event["kind"]},
            }
            continue
        country, station = match.group(1).upper(), match.group(2).upper()
        by_city[city] = {
            "city": city,
            "station": station,
            "country": country,
            "unit": infer_unit(text),
            "timezone": CITY_TIMEZONES.get(city, "UTC"),
            "source_url": match.group(0),
            "source_note": "Polymarket Wunderground source station used for TWC and AviationWeather comparison.",
            "event_titles": [event["title"]],
            "event_dates": {event["event_date"]},
            "kinds": {event["kind"]},
        }
        time.sleep(0.1)
    for meta in by_city.values():
        meta["event_dates"] = sorted(meta["event_dates"])
        meta["kinds"] = sorted(meta["kinds"])
    return by_city


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


def convert_temp(value: float, src: str, dst: str) -> float:
    if src == dst:
        return float(value)
    if src == "C" and dst == "F":
        return float(value) * 9.0 / 5.0 + 32.0
    if src == "F" and dst == "C":
        return (float(value) - 32.0) * 5.0 / 9.0
    return float(value)


def twc_points(config: dict, meta: dict, target: date) -> list[tuple[str, int]]:
    api_key = str(config["api"].get("twc_api_key") or "").strip()
    if not api_key:
        raise RuntimeError("Missing config api.twc_api_key")
    units = "m" if meta["unit"] == "C" else "e"
    ymd = target.isoformat().replace("-", "")
    loc = f"{meta['station']}:9:{meta['country']}"
    payload = get_json(
        f"https://api.weather.com/v1/location/{loc}/observations/historical.json",
        {"apiKey": api_key, "units": units, "startDate": ymd, "endDate": ymd},
    )
    tz = ZoneInfo(meta["timezone"])
    points: list[tuple[str, int]] = []
    for row in (payload.get("observations") if isinstance(payload, dict) else []) or []:
        if not isinstance(row, dict) or row.get("temp") is None:
            continue
        dt_utc = parse_utc(row.get("valid_time_gmt") or row.get("expire_time_gmt") or row.get("obsTime"))
        if dt_utc is None:
            continue
        local = dt_utc.astimezone(tz)
        if local.date() != target:
            continue
        points.append((local.isoformat(timespec="minutes"), round(float(row["temp"]))))
    return sorted(set(points))


def aviation_points(meta: dict, target: date) -> list[tuple[str, int]]:
    tz = ZoneInfo(meta["timezone"])
    end_local = datetime(target.year, target.month, target.day, 23, 59, tzinfo=tz)
    end_utc = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    params = {"ids": meta["station"], "format": "json", "date": end_utc, "hours": 30}
    payload = get_json("https://aviationweather.gov/api/data/metar", params)
    points: list[tuple[str, int]] = []
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, dict) or row.get("temp") is None:
            continue
        dt_utc = parse_utc(row.get("obsTime") or row.get("reportTime") or row.get("receiptTime"))
        if dt_utc is None:
            continue
        local = dt_utc.astimezone(tz)
        if local.date() != target:
            continue
        temp = convert_temp(float(row["temp"]), "C", meta["unit"])
        points.append((local.isoformat(timespec="minutes"), round(temp)))
    return sorted(set(points))


def summarize_points(twc: list[tuple[str, int]], aw: list[tuple[str, int]]) -> dict:
    twc_map = dict(twc)
    aw_map = dict(aw)
    common = sorted(set(twc_map) & set(aw_map))
    only_twc = sorted(set(twc_map) - set(aw_map))
    only_aw = sorted(set(aw_map) - set(twc_map))
    mismatches = [(ts, twc_map[ts], aw_map[ts]) for ts in common if twc_map[ts] != aw_map[ts]]
    twc_vals = list(twc_map.values())
    aw_vals = list(aw_map.values())
    twc_min = min(twc_vals) if twc_vals else None
    twc_max = max(twc_vals) if twc_vals else None
    aw_min = min(aw_vals) if aw_vals else None
    aw_max = max(aw_vals) if aw_vals else None
    return {
        "twc_count": len(twc_map),
        "aw_count": len(aw_map),
        "common_count": len(common),
        "mismatch_count": len(mismatches),
        "only_twc_count": len(only_twc),
        "only_aw_count": len(only_aw),
        "exact_match": not mismatches and not only_twc and not only_aw,
        "twc_min": twc_min,
        "twc_max": twc_max,
        "aw_min": aw_min,
        "aw_max": aw_max,
        "low_diff_twc_minus_aw": None if twc_min is None or aw_min is None else twc_min - aw_min,
        "high_diff_twc_minus_aw": None if twc_max is None or aw_max is None else twc_max - aw_max,
        "sample_mismatches": mismatches[:5],
        "sample_only_twc_local": only_twc[:5],
        "sample_only_aw_local": only_aw[:5],
    }


def main() -> None:
    today = date(2026, 6, 5)
    start = today - timedelta(days=14)
    config = load_config()
    events = discover_non_us_temperature_events(today)
    meta_by_city = city_metadata(events)

    rows: list[dict] = []
    errors: list[dict] = []
    for city, meta in sorted(meta_by_city.items()):
        if not meta["station"] or not meta["country"]:
            errors.append({"city": city, "error": "missing_station_or_country", "meta": meta})
            continue
        for i in range(15):
            target = start + timedelta(days=i)
            try:
                twc = twc_points(config, meta, target)
                time.sleep(0.08)
                aw = aviation_points(meta, target)
                time.sleep(0.08)
                summary = summarize_points(twc, aw)
                rows.append({
                    "date": target.isoformat(),
                    "city": city,
                    "station": meta["station"],
                    "country": meta["country"],
                    "unit": meta["unit"],
                    "timezone": meta["timezone"],
                    **summary,
                })
            except Exception as exc:
                errors.append({"city": city, "date": target.isoformat(), "station": meta["station"], "error": str(exc)})

    by_city = []
    for city, meta in sorted(meta_by_city.items()):
        city_rows = [r for r in rows if r["city"] == city]
        by_city.append({
            "city": city,
            "station": meta["station"],
            "country": meta["country"],
            "unit": meta["unit"],
            "timezone": meta["timezone"],
            "days": len(city_rows),
            "exact_days": sum(1 for r in city_rows if r["exact_match"]),
            "non_exact_days": sum(1 for r in city_rows if not r["exact_match"]),
            "max_abs_low_diff": max((abs(r["low_diff_twc_minus_aw"]) for r in city_rows if r["low_diff_twc_minus_aw"] is not None), default=None),
            "max_abs_high_diff": max((abs(r["high_diff_twc_minus_aw"]) for r in city_rows if r["high_diff_twc_minus_aw"] is not None), default=None),
            "total_mismatch_points": sum(r["mismatch_count"] for r in city_rows),
            "total_only_twc_points": sum(r["only_twc_count"] for r in city_rows),
            "total_only_aw_points": sum(r["only_aw_count"] for r in city_rows),
            "active_event_dates": meta["event_dates"],
            "active_kinds": meta["kinds"],
            "source_url": meta["source_url"],
            "source_note": meta.get("source_note", ""),
        })

    OUT.mkdir(parents=True, exist_ok=True)
    stem = f"non_us_city_twc_vs_aviationweather_15day_{start.isoformat().replace('-', '')}_{today.isoformat().replace('-', '')}"
    csv_path = OUT / f"{stem}.csv"
    json_path = OUT / f"{stem}.json"
    if rows:
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    json_path.write_text(
        json.dumps(
            {
                "summary": {
                    "compared_start": start.isoformat(),
                    "compared_end": today.isoformat(),
                    "city_count": len(meta_by_city),
                    "city_day_count": len(rows),
                    "error_count": len(errors),
                    "exact_city_days": sum(1 for r in rows if r["exact_match"]),
                    "non_exact_city_days": sum(1 for r in rows if not r["exact_match"]),
                    "cities": by_city,
                },
                "events": sorted(events.values(), key=lambda x: (x["event_date"], x["city"], x["kind"])),
                "rows": rows,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"csv": str(csv_path), "json": str(json_path), "rows": len(rows), "errors": len(errors)}, indent=2))


if __name__ == "__main__":
    main()
