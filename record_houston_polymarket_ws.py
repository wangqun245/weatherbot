from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import websocket


GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/highest-temperature-in-houston-on-june-10-2026"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def parse_jsonish(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def market_tokens(market: dict[str, Any]) -> list[dict[str, str]]:
    outcomes = parse_jsonish(market.get("outcomes"), [])
    token_ids = parse_jsonish(market.get("clobTokenIds") or market.get("clobTokenIDs"), [])
    rows: list[dict[str, str]] = []
    for idx, token_id in enumerate(token_ids or []):
        outcome = str(outcomes[idx]) if idx < len(outcomes or []) else ""
        rows.append(
            {
                "asset_id": str(token_id),
                "outcome": outcome,
                "market_id": str(market.get("id") or ""),
                "question": str(market.get("question") or ""),
            }
        )
    return rows


def load_assets() -> list[dict[str, str]]:
    response = requests.get(GAMMA_EVENT_URL, timeout=30)
    response.raise_for_status()
    event = response.json()
    rows: list[dict[str, str]] = []
    for market in event.get("markets") or []:
        if isinstance(market, dict):
            rows.extend(market_tokens(market))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    assets = load_assets()
    asset_ids = sorted({row["asset_id"] for row in assets if row.get("asset_id")})
    if not asset_ids:
        raise RuntimeError("No CLOB asset ids found for Houston June 10 event")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out or f"houston_0610_polymarket_ws_raw_{stamp}.jsonl")
    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(
        json.dumps(
            {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "seconds": args.seconds,
                "event_url": GAMMA_EVENT_URL,
                "market_ws_url": MARKET_WS_URL,
                "asset_count": len(asset_ids),
                "assets": assets,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    ws = websocket.create_connection(MARKET_WS_URL, timeout=10)
    count = 0
    try:
        subscribe = {"assets_ids": asset_ids, "type": "market", "custom_feature_enabled": True}
        ws.send(json.dumps(subscribe))
        deadline = time.monotonic() + max(1.0, args.seconds)
        with out_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(
                json.dumps(
                    {
                        "event_type": "subscribe",
                        "received_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                        "received_ms": int(time.time() * 1000),
                        "payload": subscribe,
                    },
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            )
            while time.monotonic() < deadline:
                try:
                    message = ws.recv()
                except Exception as exc:
                    handle.write(
                        json.dumps(
                            {
                                "event_type": "recv_error",
                                "received_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                                "received_ms": int(time.time() * 1000),
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    break
                count += 1
                handle.write(
                    json.dumps(
                        {
                            "event_type": "websocket_raw",
                            "received_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                            "received_ms": int(time.time() * 1000),
                            "raw": message,
                        },
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                handle.flush()
    finally:
        try:
            ws.close()
        except Exception:
            pass

    print(json.dumps({"out": str(out_path), "meta": str(meta_path), "messages": count}, ensure_ascii=False))


if __name__ == "__main__":
    main()
