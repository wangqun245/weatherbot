import requests
import os
import time
import csv
import io
from datetime import datetime

AIRPORTS = {
    "Tokyo": ("RJTT", "JP__ASOS", "RJTT"),
    "Seoul": ("RKSI", "KR__ASOS", "RKSI"),
    "Shanghai": ("ZSPD", "CN__ASOS", "ZSPD"),
    "Taipei": ("RCSS", "TW__ASOS", "RCSS"),
    "Wellington": ("NZWN", "NZ__ASOS", "NZWN"),
    "Madrid": ("LEMD", "ES__ASOS", "LEMD"),
    "Paris": ("LFPB", "FR__ASOS", "LFPB"),
    "Singapore": ("WSSS", "SG__ASOS", "WSSS"),
    "London": ("EGLC", "GB__ASOS", "EGLC"),
    "Ankara": ("LTAC", "TR__ASOS", "LTAC"),
    "Busan": ("RKPK", "KR__ASOS", "RKPK"),
    "Munich": ("EDDM", "DE__ASOS", "EDDM"),
    "Kuala Lumpur": ("WMKK", "MY__ASOS", "WMKK"),
    "Helsinki": ("EFHK", "FI__ASOS", "EFHK"),
    "Milan": ("LIMC", "IT__ASOS", "LIMC"),
    "Warsaw": ("EPWA", "PL__ASOS", "EPWA"),
    "Lucknow": ("VILK", "IN__ASOS", "VILK"),
    "Karachi": ("OPKC", "PK__ASOS", "OPKC"),
    "Amsterdam": ("EHAM", "NL__ASOS", "EHAM"),
}

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 49
OVERWRITE_EXISTING = False
MAX_RETRIES = 5

BASE_DIR = r"C:\weather\metar_history"

os.makedirs(BASE_DIR, exist_ok=True)

def normalize_iem_csv(text, icao_station):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["station", "valid", "metar"])

    reader = csv.DictReader(io.StringIO(text))
    records = 0
    for row in reader:
        valid = (row.get("valid") or "").strip()
        metar = (row.get("metar") or "").strip()
        if not valid or not metar or metar == "M":
            continue
        writer.writerow([icao_station, valid, metar])
        records += 1

    return output.getvalue(), records


for city, (station, network, iem_station) in AIRPORTS.items():

    station_dir = os.path.join(BASE_DIR, station)
    os.makedirs(station_dir, exist_ok=True)

    print(f"\n===== {city} ({station}) =====")

    for year in range(START_YEAR, CURRENT_YEAR + 1):

        output_file = os.path.join(
            station_dir,
            f"{station}_{year}_metar.csv"
        )

        if os.path.exists(output_file) and not OVERWRITE_EXISTING:
            print(f"Skip existing {year}")
            continue
        if os.path.exists(output_file):
            print(f"Overwrite existing {year}")

        url = (
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
            f"?network={network}"
            f"&station={iem_station}"
            "&data=metar"
            f"&year1={year}&month1=1&day1=1"
            f"&year2={year}&month2=12&day2=31"
            "&tz=Etc%2FUTC"
            "&format=onlycomma"
            "&latlon=no"
            "&elev=no"
            "&missing=M"
            "&trace=T"
            "&direct=no"
            "&report_type=3"
            "&report_type=4"
        )

        try:
            print(f"Downloading {station} {year}")

            for attempt in range(1, MAX_RETRIES + 1):
                r = requests.get(url, timeout=120)
                if r.status_code != 429:
                    break

                retry_after = r.headers.get("Retry-After")
                wait_seconds = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else attempt * 5
                )
                print(
                    f"Rate limited for {year}; "
                    f"retry {attempt}/{MAX_RETRIES} in {wait_seconds}s"
                )
                time.sleep(wait_seconds)

            if r.status_code != 200:
                print(f"Failed {year}: HTTP {r.status_code}")
                continue

            normalized_text, records = normalize_iem_csv(r.text, station)

            # 只有表头或者空内容
            if records == 0:
                print(f"No data for {year}")
                continue

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(normalized_text)

            print(
                f"Saved {year}, "
                f"{records:,} records"
            )

            time.sleep(1)

        except Exception as e:
            print(f"Error {year}: {e}")
