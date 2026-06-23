from __future__ import annotations

import argparse
import csv
import json
import math
import re
from bisect import bisect_left
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_INPUT_DIR = Path(r"C:\Users\Jack\Documents\git\weatherbot\metar_history_processed")
DEFAULT_COMBINED_NAME = "all_stations_local_0900_1900_daily_high_features.csv"

STATION_TIMEZONES = {
    "KATL": "America/New_York",
    "KAUS": "America/Chicago",
    "KDAL": "America/Chicago",
    "KDEN": "America/Denver",
    "KHOU": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KLGA": "America/New_York",
    "KMIA": "America/New_York",
    "KORD": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
}

SHORT_TO_ICAO = {
    "ATL": "KATL",
    "AUS": "KAUS",
    "DAL": "KDAL",
    "DEN": "KDEN",
    "HOU": "KHOU",
    "LAX": "KLAX",
    "LGA": "KLGA",
    "MIA": "KMIA",
    "ORD": "KORD",
    "SEA": "KSEA",
    "SFO": "KSFO",
}

STATION_IDS = {station: index for index, station in enumerate(sorted(STATION_TIMEZONES), start=1)}

RMK_TEMP_RE = re.compile(r"(?:^|\s)T([01])(\d{3})([01])(\d{3})(?:\s|$)")
BODY_TEMP_RE = re.compile(r"^(M?\d{2}|//)/(M?\d{2}|//)$")
WIND_RE = re.compile(r"^(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?KT$")
VIS_RE = re.compile(r"^(?:(\d+) )?(\d+)?(?:/(\d+))?SM$")
ALTIMETER_RE = re.compile(r"^A(\d{4})$")
SLP_RE = re.compile(r"(?:^|\s)SLP(\d{3})(?:\s|$)")
TENDENCY_RE = re.compile(r"(?:^|\s)5([0-8])(\d{3})(?:\s|$)")
CLOUD_RE = re.compile(r"^(FEW|SCT|BKN|OVC|VV)(\d{3}|///)?")
PRECIP_1H_RE = re.compile(r"(?:^|\s)P(\d{4})(?:\s|$)")

CLOUD_AMOUNT = {
    "CLR": 0,
    "SKC": 0,
    "NSC": 0,
    "FEW": 1,
    "SCT": 2,
    "BKN": 3,
    "OVC": 4,
    "VV": 4,
}

WX_CODES = (
    "TS",
    "RA",
    "SN",
    "DZ",
    "FG",
    "BR",
    "HZ",
    "FU",
    "VA",
    "DU",
    "SA",
    "SQ",
    "FC",
    "GR",
    "GS",
    "PL",
    "SG",
    "IC",
    "UP",
)


@dataclass(frozen=True)
class MetarRow:
    daily_high_f: str
    station: str
    valid_utc: datetime
    valid_text: str
    metar: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decode processed METAR rows into numeric LightGBM-friendly feature CSVs."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--lag-tolerance-minutes", type=int, default=30)
    parser.add_argument("--combined-file", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def blank(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:.6g}"
    return str(value)


def parse_signed_celsius(text: str) -> float | None:
    if text == "//":
        return None
    sign = -1.0 if text.startswith("M") else 1.0
    digits = text[1:] if text.startswith("M") else text
    try:
        return sign * float(int(digits))
    except ValueError:
        return None


def c_to_f(temp_c: float | None) -> float | None:
    if temp_c is None:
        return None
    return temp_c * 9.0 / 5.0 + 32.0


def parse_rmk_temp_dewpoint(metar: str) -> tuple[float | None, float | None]:
    match = RMK_TEMP_RE.search(metar)
    if not match:
        return None, None
    temp_sign, temp_tenths, dew_sign, dew_tenths = match.groups()
    temp_c = int(temp_tenths) / 10.0
    dewpoint_c = int(dew_tenths) / 10.0
    if temp_sign == "1":
        temp_c *= -1.0
    if dew_sign == "1":
        dewpoint_c *= -1.0
    return temp_c, dewpoint_c


def parse_visibility_sm(token: str) -> float | None:
    match = VIS_RE.match(token)
    if not match:
        return None
    whole, numerator, denominator = match.groups()
    value = float(whole) if whole else 0.0
    if numerator and denominator:
        value += float(numerator) / float(denominator)
    elif numerator:
        value += float(numerator)
    return value


def relative_humidity_pct(temp_c: float | None, dewpoint_c: float | None) -> float | None:
    if temp_c is None or dewpoint_c is None:
        return None
    a = 17.625
    b = 243.04
    rh = 100.0 * math.exp((a * dewpoint_c) / (b + dewpoint_c) - (a * temp_c) / (b + temp_c))
    return max(0.0, min(100.0, rh))


def heat_index_f(temp_f: float | None, rh: float | None) -> float | None:
    if temp_f is None or rh is None or temp_f < 80.0:
        return None
    t = temp_f
    r = rh
    return (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * r
        - 0.22475541 * t * r
        - 0.00683783 * t * t
        - 0.05481717 * r * r
        + 0.00122874 * t * t * r
        + 0.00085282 * t * r * r
        - 0.00000199 * t * t * r * r
    )


def wind_chill_f(temp_f: float | None, wind_speed_mph: float | None) -> float | None:
    if temp_f is None or wind_speed_mph is None or temp_f > 50.0 or wind_speed_mph < 3.0:
        return None
    return 35.74 + 0.6215 * temp_f - 35.75 * (wind_speed_mph ** 0.16) + 0.4275 * temp_f * (wind_speed_mph ** 0.16)


def tokenize_body(metar: str) -> tuple[list[str], str]:
    body, sep, remarks = metar.partition(" RMK ")
    return body.split(), remarks if sep else ""


def decode_metar(row: MetarRow, station_icao: str, tz: ZoneInfo) -> dict[str, object | None]:
    tokens, remarks = tokenize_body(row.metar)
    local_dt = row.valid_utc.astimezone(tz)

    features: dict[str, object | None] = {
        "station_icao": station_icao,
        "valid_utc_epoch": int(row.valid_utc.timestamp()),
        "local_year": local_dt.year,
        "local_month": local_dt.month,
        "local_day": local_dt.day,
        "local_day_of_year": local_dt.timetuple().tm_yday,
        "local_day_of_week": local_dt.weekday(),
        "local_hour": local_dt.hour,
        "local_minute": local_dt.minute,
        "local_minutes_since_midnight": local_dt.hour * 60 + local_dt.minute,
        "local_hour_sin": math.sin(2.0 * math.pi * (local_dt.hour * 60 + local_dt.minute) / 1440.0),
        "local_hour_cos": math.cos(2.0 * math.pi * (local_dt.hour * 60 + local_dt.minute) / 1440.0),
        "local_doy_sin": math.sin(2.0 * math.pi * local_dt.timetuple().tm_yday / 366.0),
        "local_doy_cos": math.cos(2.0 * math.pi * local_dt.timetuple().tm_yday / 366.0),
    }

    body_temp_c = None
    body_dewpoint_c = None
    temp_c, dewpoint_c = parse_rmk_temp_dewpoint(row.metar)
    cloud_amounts: list[int] = []
    cloud_bases: list[int] = []
    ceiling_bases: list[int] = []
    wx_tokens: list[str] = []

    idx = 1
    while idx < len(tokens):
        token = tokens[idx]
        next_token = tokens[idx + 1] if idx + 1 < len(tokens) else ""
        combined_visibility = None
        if re.match(r"^\d+$", token) and next_token.endswith("SM"):
            combined_visibility = f"{token} {next_token}"
            idx += 1

        visibility_token = combined_visibility or token
        idx += 1

        wind_match = WIND_RE.match(token)
        if wind_match:
            direction, speed, gust = wind_match.groups()
            wind_speed_kt = float(speed)
            wind_dir_degrees = None if direction == "VRB" else float(direction)
            features["wind_dir_degrees"] = wind_dir_degrees
            features["wind_variable"] = 1 if direction == "VRB" else 0
            features["wind_speed_kt"] = wind_speed_kt
            features["wind_gust_kt"] = float(gust) if gust else None
            features["wind_speed_mph"] = wind_speed_kt * 1.15078
            if wind_dir_degrees is not None:
                radians = math.radians(wind_dir_degrees)
                features["wind_u_kt"] = -wind_speed_kt * math.sin(radians)
                features["wind_v_kt"] = -wind_speed_kt * math.cos(radians)
            continue

        if token == "00000KT":
            features["wind_dir_degrees"] = 0
            features["wind_variable"] = 0
            features["wind_speed_kt"] = 0
            features["wind_gust_kt"] = None
            features["wind_speed_mph"] = 0
            features["wind_u_kt"] = 0
            features["wind_v_kt"] = 0
            continue

        if visibility_token.endswith("SM"):
            vis = parse_visibility_sm(visibility_token)
            if vis is not None:
                features["visibility_sm"] = vis
                continue

        altimeter_match = ALTIMETER_RE.match(token)
        if altimeter_match:
            features["altimeter_inhg"] = int(altimeter_match.group(1)) / 100.0
            continue

        body_temp_match = BODY_TEMP_RE.match(token)
        if body_temp_match:
            body_temp_c = parse_signed_celsius(body_temp_match.group(1))
            body_dewpoint_c = parse_signed_celsius(body_temp_match.group(2))
            continue

        if token in ("CLR", "SKC", "NSC"):
            cloud_amounts.append(CLOUD_AMOUNT[token])
            continue

        cloud_match = CLOUD_RE.match(token)
        if cloud_match:
            amount, base = cloud_match.groups()
            cloud_amounts.append(CLOUD_AMOUNT[amount])
            if base and base != "///":
                base_ft = int(base) * 100
                cloud_bases.append(base_ft)
                if amount in ("BKN", "OVC", "VV"):
                    ceiling_bases.append(base_ft)
            continue

        compact = token.lstrip("+-")
        if any(code in compact for code in WX_CODES):
            wx_tokens.append(token)

    if temp_c is None:
        temp_c = body_temp_c
    if dewpoint_c is None:
        dewpoint_c = body_dewpoint_c

    temp_f = c_to_f(temp_c)
    dewpoint_f = c_to_f(dewpoint_c)
    rh = relative_humidity_pct(temp_c, dewpoint_c)

    features.update(
        {
            "temp_c": temp_c,
            "temp_f": temp_f,
            "dewpoint_c": dewpoint_c,
            "dewpoint_f": dewpoint_f,
            "relative_humidity_pct": rh,
            "temp_dewpoint_spread_c": None if temp_c is None or dewpoint_c is None else temp_c - dewpoint_c,
            "heat_index_f": heat_index_f(temp_f, rh),
            "wind_chill_f": wind_chill_f(temp_f, features.get("wind_speed_mph")),
            "cloud_layer_count": len(cloud_amounts) if cloud_amounts else None,
            "cloud_cover_max": max(cloud_amounts) if cloud_amounts else None,
            "lowest_cloud_base_ft": min(cloud_bases) if cloud_bases else None,
            "lowest_ceiling_ft": min(ceiling_bases) if ceiling_bases else None,
        }
    )

    slp_match = SLP_RE.search(remarks)
    if slp_match:
        raw = int(slp_match.group(1))
        features["sea_level_pressure_hpa"] = raw / 10.0 + (1000.0 if raw < 500 else 900.0)

    tendency_match = TENDENCY_RE.search(remarks)
    if tendency_match:
        features["pressure_tendency_code"] = int(tendency_match.group(1))
        features["pressure_tendency_3h_hpa"] = int(tendency_match.group(2)) / 10.0

    precip_match = PRECIP_1H_RE.search(remarks)
    if precip_match:
        features["precip_1h_in"] = int(precip_match.group(1)) / 100.0

    for code in WX_CODES:
        features[f"wx_{code.lower()}"] = 1 if any(code in token.lstrip("+-") for token in wx_tokens) else 0
    features["wx_token_count"] = len(wx_tokens) if wx_tokens else 0

    return features


FEATURE_COLUMNS = [
    "station_id",
    "valid_utc_epoch",
    "local_year",
    "local_month",
    "local_day",
    "local_day_of_year",
    "local_day_of_week",
    "local_hour",
    "local_minute",
    "local_minutes_since_midnight",
    "local_hour_sin",
    "local_hour_cos",
    "local_doy_sin",
    "local_doy_cos",
    "temp_c",
    "temp_f",
    "dewpoint_c",
    "dewpoint_f",
    "relative_humidity_pct",
    "temp_dewpoint_spread_c",
    "heat_index_f",
    "wind_chill_f",
    "wind_dir_degrees",
    "wind_variable",
    "wind_speed_kt",
    "wind_speed_mph",
    "wind_gust_kt",
    "wind_u_kt",
    "wind_v_kt",
    "visibility_sm",
    "altimeter_inhg",
    "sea_level_pressure_hpa",
    "pressure_tendency_code",
    "pressure_tendency_3h_hpa",
    "precip_1h_in",
    "cloud_layer_count",
    "cloud_cover_max",
    "lowest_cloud_base_ft",
    "lowest_ceiling_ft",
    "wx_token_count",
    *[f"wx_{code.lower()}" for code in WX_CODES],
    "temp_f_lag_1h",
    "temp_f_lag_2h",
    "temp_f_lag_3h",
    "temp_f_change_1h",
    "temp_f_change_2h",
    "temp_f_change_3h",
]

OUTPUT_COLUMNS = ["daily_high_f", "station", "valid", "metar", *FEATURE_COLUMNS]


def read_rows(path: Path) -> list[MetarRow]:
    rows: list[MetarRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for source in reader:
            valid_utc = datetime.strptime(source["valid"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            rows.append(
                MetarRow(
                    daily_high_f=source["daily_high_f"],
                    station=source["station"],
                    valid_utc=valid_utc,
                    valid_text=source["valid"],
                    metar=source["metar"],
                )
            )
    return rows


def nearest_lag_value(
    target_time: datetime,
    valid_times: list[datetime],
    temp_values: list[float | None],
    tolerance: timedelta,
) -> float | None:
    if not valid_times:
        return None
    idx = bisect_left(valid_times, target_time)
    candidates = []
    if idx < len(valid_times):
        candidates.append(idx)
    if idx > 0:
        candidates.append(idx - 1)

    best_idx = None
    best_delta = None
    for candidate_idx in candidates:
        value = temp_values[candidate_idx]
        if value is None:
            continue
        delta = abs(valid_times[candidate_idx] - target_time)
        if delta <= tolerance and (best_delta is None or delta < best_delta):
            best_idx = candidate_idx
            best_delta = delta

    return temp_values[best_idx] if best_idx is not None else None


def feature_file_for(input_file: Path) -> Path:
    return input_file.with_name(f"{input_file.stem}_features{input_file.suffix}")


def station_icao_from_path(path: Path, rows: list[MetarRow]) -> str:
    first = path.name.split("_", 1)[0].upper()
    if first.startswith("K"):
        return first
    if rows:
        return SHORT_TO_ICAO.get(rows[0].station.upper(), first)
    return first


def write_station_features(
    input_file: Path,
    output_file: Path,
    combined_writer: csv.DictWriter,
    tolerance: timedelta,
) -> Counter:
    rows = read_rows(input_file)
    station_icao = station_icao_from_path(input_file, rows)
    tz_name = STATION_TIMEZONES.get(station_icao)
    if tz_name is None:
        raise ValueError(f"Missing timezone mapping for {station_icao}")
    tz = ZoneInfo(tz_name)

    decoded = [decode_metar(row, station_icao, tz) for row in rows]
    for features in decoded:
        features["station_id"] = STATION_IDS[station_icao]
    valid_times = [row.valid_utc for row in rows]
    temp_values = [features.get("temp_f") for features in decoded]

    stats: Counter = Counter(input_rows=len(rows), output_rows=0)
    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()

        for row, features in zip(rows, decoded):
            temp_f = features.get("temp_f")
            for hours in (1, 2, 3):
                lag_value = nearest_lag_value(
                    row.valid_utc - timedelta(hours=hours),
                    valid_times,
                    temp_values,
                    tolerance,
                )
                features[f"temp_f_lag_{hours}h"] = lag_value
                features[f"temp_f_change_{hours}h"] = (
                    None if temp_f is None or lag_value is None else float(temp_f) - lag_value
                )

            output_row = {
                "daily_high_f": row.daily_high_f,
                "station": row.station,
                "valid": row.valid_text,
                "metar": row.metar,
            }
            output_row.update({column: blank(features.get(column)) for column in FEATURE_COLUMNS})
            writer.writerow(output_row)
            combined_writer.writerow(output_row)
            stats["output_rows"] += 1
            for hours in (1, 2, 3):
                if features.get(f"temp_f_lag_{hours}h") is None:
                    stats[f"missing_temp_lag_{hours}h"] += 1

    return stats


def input_feature_sources(input_dir: Path) -> list[Path]:
    return [
        path
        for path in sorted(input_dir.glob("K*_local_0900_1900_daily_high.csv"))
        if not path.name.endswith("_features.csv")
    ]


def main() -> int:
    args = parse_args()
    if not args.input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")
    if args.combined_file is None:
        args.combined_file = args.input_dir / DEFAULT_COMBINED_NAME
    sources = input_feature_sources(args.input_dir)
    if not sources:
        raise SystemExit(f"No processed station CSV files found in {args.input_dir}")

    output_files = [feature_file_for(path) for path in sources]
    output_files.append(args.combined_file)
    existing = [path for path in output_files if path.exists()]
    if existing and not args.overwrite:
        raise SystemExit(
            "Output file(s) already exist; rerun with --overwrite to replace them: "
            + ", ".join(str(path) for path in existing[:5])
        )

    summary: dict[str, dict[str, int | str]] = {}
    totals: Counter = Counter()
    tolerance = timedelta(minutes=args.lag_tolerance_minutes)

    with args.combined_file.open("w", encoding="utf-8", newline="") as combined_handle:
        combined_writer = csv.DictWriter(combined_handle, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        combined_writer.writeheader()

        for source in sources:
            output_file = feature_file_for(source)
            stats = write_station_features(source, output_file, combined_writer, tolerance)
            stats["output_file"] = str(output_file)
            summary[source.name] = dict(stats)
            totals.update({key: value for key, value in stats.items() if isinstance(value, int)})
            print(
                f"{source.name}: wrote {stats['output_rows']} feature rows "
                f"to {output_file.name}"
            )

    summary["_totals"] = dict(totals)
    summary["_config"] = {
        "lag_tolerance_minutes": args.lag_tolerance_minutes,
        "combined_file": str(args.combined_file),
    }
    summary_file = args.input_dir / "feature_summary.json"
    with summary_file.open("w", encoding="utf-8", newline="") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Combined feature file written to {args.combined_file}")
    print(f"Summary written to {summary_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
