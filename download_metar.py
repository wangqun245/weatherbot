import csv
import io
import os
import random
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


AIRPORTS = {
 ##   "NYC": ("KNYC", "NY_ASOS", "NYC"),                 # Central Park
    "Chicago": ("KMDW", "IL_ASOS", "MDW"),            # Chicago Midway
    "Denver": ("KDEN", "CO_ASOS", "DEN"),             # Denver Intl
    "Phoenix": ("KPHX", "AZ_ASOS", "PHX"),            # Phoenix Sky Harbor
    "Philadelphia": ("KPHL", "PA_ASOS", "PHL"),       # Philadelphia Intl
    "Houston": ("KHOU", "TX_ASOS", "HOU"),            # Houston Hobby
    "Minneapolis": ("KMSP", "MN_ASOS", "MSP"),        # Minneapolis-St Paul
    "Oklahoma City": ("KOKC", "OK_ASOS", "OKC"),      # Will Rogers
    "Washington DC": ("KDCA", "VA_ASOS", "DCA"),      # Reagan National
    "Boston": ("KBOS", "MA_ASOS", "BOS"),             # Boston Logan
    "Dallas": ("KDFW", "TX_ASOS", "DFW"),             # Dallas/Fort Worth
    "Las Vegas": ("KLAS", "NV_ASOS", "LAS"),          # Harry Reid / Las Vegas
    "San Antonio": ("KSAT", "TX_ASOS", "SAT"),        # San Antonio Intl
    "New Orleans": ("KMSY", "LA_ASOS", "MSY"),        # New Orleans Intl
}

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 50
OVERWRITE_EXISTING = True

BASE_DIR = r"C:\weather\metar_history"
os.makedirs(BASE_DIR, exist_ok=True)

# ===== Rate-limit friendly settings =====
# One year of METAR data can be a large request. Keep this conservative.
MIN_SECONDS_BETWEEN_REQUESTS = 1.0
MAX_SECONDS_BETWEEN_REQUESTS = 3.0
MAX_RETRIES_PER_FILE = 3
REQUEST_TIMEOUT_SECONDS = 180

# Retry backoff will be roughly: 10s, 20s, 40s, 80s... plus jitter,
# but capped so the script can still recover without hammering the server.
BACKOFF_BASE_SECONDS = 10
BACKOFF_CAP_SECONDS = 10 * 60

FAILED_LOG = os.path.join(BASE_DIR, "failed_downloads.csv")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "metar-history-downloader/1.0 "
            "(+personal research; respectful rate limiting)"
        )
    })

    # urllib3 retry covers transient connection-level failures.
    # We still handle HTTP 429 manually below because we want to honor Retry-After.
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=0,
        allowed_methods={"GET"},
        backoff_factor=2,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        retry_dt = parsedate_to_datetime(value)
        now = datetime.now(retry_dt.tzinfo)
        return max(0.0, (retry_dt - now).total_seconds())
    except Exception:
        return None


def polite_sleep(reason: str, seconds: float) -> None:
    seconds = max(0.0, seconds)
    print(f"Sleep {seconds:.1f}s ({reason})")
    time.sleep(seconds)


def normalize_iem_csv(text: str, icao_station: str):
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


def build_iem_url(network: str, iem_station: str, year: int) -> str:
    return (
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


def write_failed(city: str, station: str, year: int, reason: str) -> None:
    file_exists = os.path.exists(FAILED_LOG)
    with open(FAILED_LOG, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["time_utc", "city", "station", "year", "reason"])
        writer.writerow([
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
            city,
            station,
            year,
            reason,
        ])


def download_one_year(session: requests.Session, city: str, station: str, network: str, iem_station: str, year: int, output_file: str) -> bool:
    url = build_iem_url(network, iem_station, year)

    for attempt in range(1, MAX_RETRIES_PER_FILE + 1):
        try:
            print(f"Downloading {city} {station} {year} attempt {attempt}/{MAX_RETRIES_PER_FILE}")
            r = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)

            if r.status_code == 200:
                normalized_text, records = normalize_iem_csv(r.text, station)
                if records == 0:
                    reason = "No data rows returned"
                    print(f"No data for {station} {year}")
                    write_failed(city, station, year, reason)
                    return False

                tmp_file = output_file + ".tmp"
                with open(tmp_file, "w", encoding="utf-8", newline="") as f:
                    f.write(normalized_text)
                os.replace(tmp_file, output_file)

                print(f"Saved {station} {year}, {records:,} records")
                return True

            if r.status_code == 429:
                retry_after = parse_retry_after_seconds(r.headers.get("Retry-After"))
                backoff = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
                wait = retry_after if retry_after is not None else backoff
                wait += random.uniform(5, 25)
                print(f"429 rate limited for {station} {year}. Retry-After={r.headers.get('Retry-After')!r}")
                polite_sleep("rate limited", wait)
                continue

            if 500 <= r.status_code < 600:
                wait = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
                wait += random.uniform(3, 15)
                print(f"Server error HTTP {r.status_code} for {station} {year}")
                polite_sleep("server error retry", wait)
                continue

            reason = f"HTTP {r.status_code}: {r.text[:200].replace(chr(10), ' ')}"
            print(f"Failed {station} {year}: {reason}")
            write_failed(city, station, year, reason)
            return False

        except requests.exceptions.RequestException as e:
            wait = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
            wait += random.uniform(3, 15)
            print(f"Network error for {station} {year}: {e}")
            polite_sleep("network retry", wait)

    reason = f"Exceeded retries after {MAX_RETRIES_PER_FILE} attempts"
    print(f"Failed {station} {year}: {reason}")
    write_failed(city, station, year, reason)
    return False


def main() -> None:
    session = build_session()
    total_ok = 0
    total_failed = 0

    for city, (station, network, iem_station) in AIRPORTS.items():
        station_dir = os.path.join(BASE_DIR, station)
        os.makedirs(station_dir, exist_ok=True)

        print(f"\n===== {city} ({station}) =====")

        for year in range(START_YEAR, CURRENT_YEAR + 1):
            output_file = os.path.join(station_dir, f"{station}_{year}_metar.csv")

            if os.path.exists(output_file) and not OVERWRITE_EXISTING:
                print(f"Skip existing {station} {year}")
                continue
            if os.path.exists(output_file):
                print(f"Overwrite existing {station} {year}")

            ok = download_one_year(session, city, station, network, iem_station, year, output_file)
            if ok:
                total_ok += 1
            else:
                total_failed += 1

            # Always wait between requests, even after failures.
            polite_sleep(
                "between IEM requests",
                random.uniform(MIN_SECONDS_BETWEEN_REQUESTS, MAX_SECONDS_BETWEEN_REQUESTS),
            )

    print(f"\nDone. Success files: {total_ok}, failed files: {total_failed}")
    if total_failed:
        print(f"Failed list saved to: {FAILED_LOG}")


if __name__ == "__main__":
    main()