import requests
import os
import time
from datetime import datetime

AIRPORTS = {
  ##  "Atlanta": "KATL",
    ## "Austin": "KAUS",
    ## "Chicago": "KORD",
    ##    "Dallas": "KDAL",
    "Denver": "KBKF",
    ## "Houston": "KHOU",
    ##  "Los Angeles": "KLAX",
    ##   "Miami": "KMIA",
    ##   "NYC": "KLGA",
    ##  "San Francisco": "KSFO",
    ##  "Seattle": "KSEA",
}

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 49

BASE_DIR = r"C:\weather\metar_history"

os.makedirs(BASE_DIR, exist_ok=True)

for city, station in AIRPORTS.items():

    station_dir = os.path.join(BASE_DIR, station)
    os.makedirs(station_dir, exist_ok=True)

    print(f"\n===== {city} ({station}) =====")

    for year in range(START_YEAR, CURRENT_YEAR + 1):

        output_file = os.path.join(
            station_dir,
            f"{station}_{year}_metar.csv"
        )

        if os.path.exists(output_file):
            print(f"Skip existing {year}")
            continue

        url = (
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
            f"?station={station}"
            "&data=metar"
            f"&year1={year}&month1=1&day1=1"
            f"&year2={year}&month2=12&day2=31"
            "&tz=Etc/UTC"
            "&format=comma"
            "&missing=M"
            "&trace=T"
            "&direct=no"
            "&report_type=1"
            "&report_type=2"
        )

        try:
            print(f"Downloading {station} {year}")

            r = requests.get(url, timeout=120)

            if r.status_code != 200:
                print(f"Failed {year}: HTTP {r.status_code}")
                continue

            lines = r.text.splitlines()

            # 只有表头或者空内容
            if len(lines) <= 1:
                print(f"No data for {year}")
                continue

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(r.text)

            print(
                f"Saved {year}, "
                f"{len(lines)-1:,} records"
            )

            time.sleep(1)

        except Exception as e:
            print(f"Error {year}: {e}")