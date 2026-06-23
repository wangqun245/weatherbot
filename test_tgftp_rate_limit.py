#!/usr/bin/env python3
"""Probe NOAA TGFTP station TXT rate behavior with no-cache requests.

Example:
  python test_tgftp_rate_limit.py --station KAUS --interval 2 --count 120
  python test_tgftp_rate_limit.py --station KAUS --interval 0.5 --duration 300
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TGFTP no-cache rate probe")
    parser.add_argument("--station", default="KAUS", help="ICAO station code, e.g. KAUS")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between request starts")
    parser.add_argument("--count", type=int, default=0, help="Number of requests; 0 means use duration")
    parser.add_argument("--duration", type=float, default=300.0, help="Run seconds when count is 0")
    parser.add_argument("--timeout", type=float, default=15.0, help="Request timeout seconds")
    parser.add_argument("--output", default="", help="Optional CSV output path")
    return parser


def latest_metar_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[1] if len(lines) >= 2 else ""


def main() -> None:
    args = build_parser().parse_args()
    station = args.station.strip().upper()
    base_url = f"https://tgftp.nws.noaa.gov/data/observations/metar/stations/{station}.TXT"
    output = Path(args.output) if args.output else Path(f"tgftp_rate_{station}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    deadline = time.monotonic() + max(0.0, args.duration)
    request_no = 0
    rows: list[dict[str, str]] = []

    print(f"Probing {base_url}")
    print(f"interval={args.interval}s count={args.count or 'duration'} duration={args.duration}s output={output}")

    while True:
        if args.count and request_no >= args.count:
            break
        if not args.count and time.monotonic() >= deadline:
            break

        request_no += 1
        started_mono = time.monotonic()
        requested_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        cache_bust = f"{int(time.time() * 1000)}-{random.randint(100000, 999999)}"
        url = f"{base_url}?nocache={cache_bust}"
        row = {
            "request_no": str(request_no),
            "requested_at_utc": requested_at,
            "status_code": "",
            "elapsed_ms": "",
            "server_date": "",
            "last_modified": "",
            "content_length": "",
            "body_sha256": "",
            "metar_line": "",
            "error": "",
        }
        try:
            response = requests.get(
                url,
                headers={
                    "Cache-Control": "no-cache, no-store, max-age=0",
                    "Pragma": "no-cache",
                    "User-Agent": "weatherbot-tgftp-rate-probe/1.0",
                },
                timeout=args.timeout,
            )
            elapsed_ms = int(response.elapsed.total_seconds() * 1000)
            body = response.text
            row.update(
                {
                    "status_code": str(response.status_code),
                    "elapsed_ms": str(elapsed_ms),
                    "server_date": response.headers.get("Date", ""),
                    "last_modified": response.headers.get("Last-Modified", ""),
                    "content_length": response.headers.get("Content-Length", str(len(response.content))),
                    "body_sha256": hashlib.sha256(response.content).hexdigest(),
                    "metar_line": latest_metar_line(body),
                }
            )
            print(
                f"{request_no:04d} status={response.status_code} elapsed={elapsed_ms}ms "
                f"last_modified={row['last_modified']} metar={row['metar_line'][:90]}"
            )
        except Exception as exc:
            row["error"] = repr(exc)
            print(f"{request_no:04d} ERROR {exc!r}")
        rows.append(row)

        sleep_for = args.interval - (time.monotonic() - started_mono)
        if sleep_for > 0:
            time.sleep(sleep_for)

    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["request_no"])
        writer.writeheader()
        writer.writerows(rows)
    errors = sum(1 for row in rows if row.get("error") or row.get("status_code") not in {"200"})
    print(f"Done. requests={len(rows)} errors_or_non_200={errors} output={output}")


if __name__ == "__main__":
    main()
