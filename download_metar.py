import requests
import os
import time
import csv
import io
from datetime import datetime

AIRPORTS = {
    "Atlanta": ("KATL", "GA_ASOS", "ATL"),
     "Austin": ("KAUS", "TX_ASOS", "AUS"),
     "Chicago": ("KORD", "IL_ASOS", "ORD"),
        "Dallas": ("KDAL", "TX_ASOS", "DAL"),
    "Denver": ("KBKF", "CO_ASOS", "BKF"),
     "Houston": ("KHOU", "TX_ASOS", "HOU"),
      "Los Angeles": ("KLAX", "CA_ASOS", "LAX"),
       "Miami": ("KMIA", "FL_ASOS", "MIA"),
       "NYC": ("KLGA", "NY_ASOS", "LGA"),
      "San Francisco": ("KSFO", "CA_ASOS", "SFO"),
     "Seattle": ("KSEA", "WA_ASOS", "SEA"),
}

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 49
OVERWRITE_EXISTING = True

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

            r = requests.get(url, timeout=120)

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
