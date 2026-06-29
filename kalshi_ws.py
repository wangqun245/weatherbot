from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

import websocket

from kalshi_client import KalshiClient


LOGGER = logging.getLogger("kalshi_weather_trader")


class KalshiWebSocketFeed:
    """Authenticated Kalshi order-book and private order/fill stream."""

    def __init__(
        self,
        client: KalshiClient,
        url: str,
        on_message: Callable[[dict[str, Any]], None],
        reconnect_seconds: float = 2.0,
    ) -> None:
        self.client = client
        self.url = url
        self.on_message = on_message
        self.reconnect_seconds = reconnect_seconds
        self._lock = threading.RLock()
        self._books: dict[str, dict[str, dict[float, float]]] = {}
        self._snapshots: set[str] = set()
        self._tickers: set[str] = set()
        self._running = False
        self._thread: threading.Thread | None = None
        self._ws: websocket.WebSocket | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="kalshi-websocket", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        with self._lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)

    def subscribe(self, tickers: list[str]) -> None:
        desired = {str(ticker) for ticker in tickers if ticker}
        with self._lock:
            if desired == self._tickers:
                return
            self._tickers = desired
            self._books = {
                ticker: self._books.get(
                    ticker, {"yes_bids": {}, "yes_asks": {}}
                )
                for ticker in desired
            }
            self._snapshots.intersection_update(desired)
            ws = self._ws
        # Reconnect to establish one clean snapshot for the exact active set.
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def wait_ready(self, timeout: float = 10.0) -> bool:
        return self._ready.wait(timeout)

    def has_book(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._snapshots

    def buy_levels(
        self, ticker: str, outcome_side: str
    ) -> list[tuple[float, float]]:
        with self._lock:
            book = self._books.get(ticker) or {}
            yes_bids = dict(book.get("yes_bids") or {})
            yes_asks = dict(book.get("yes_asks") or {})
        if outcome_side.upper() == "YES":
            return sorted(
                (
                    (price, quantity)
                    for price, quantity in yes_asks.items()
                    if quantity > 0
                ),
                key=lambda item: item[0],
            )
        # A YES bid at p is an immediately executable NO ask at 1-p.
        return sorted(
            (
                (round(1.0 - price, 4), quantity)
                for price, quantity in yes_bids.items()
                if quantity > 0
            ),
            key=lambda item: item[0],
        )

    def _subscription_commands(self, tickers: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": tickers,
                    "use_yes_price": True,
                },
            },
            {
                "id": 2,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker"],
                    "market_tickers": tickers,
                },
            },
            {
                "id": 3,
                "cmd": "subscribe",
                "params": {
                    "channels": ["user_orders", "fill"],
                    "market_tickers": tickers,
                },
            },
        ]

    def _run(self) -> None:
        while self._running:
            with self._lock:
                tickers = sorted(self._tickers)
            if not tickers:
                time.sleep(0.25)
                continue
            self._ready.clear()
            try:
                with self._lock:
                    self._snapshots.difference_update(tickers)
                headers = self.client.websocket_auth_headers()
                parsed_url = urlparse(self.url)
                key_tail = self.client.api_key_id[-4:] if self.client.api_key_id else "none"
                LOGGER.info(
                    "Kalshi websocket handshake url=%s key_suffix=%s content_type=%s",
                    f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}",
                    key_tail,
                    headers.get("Content-Type", ""),
                )
                ws = websocket.create_connection(
                    self.url,
                    header=[f"{key}: {value}" for key, value in headers.items()],
                    timeout=5,
                )
                with self._lock:
                    self._ws = ws
                for command in self._subscription_commands(tickers):
                    ws.send(json.dumps(command))
                LOGGER.info(
                    "Kalshi websocket connected tickers=%s", tickers
                )
                while self._running:
                    raw = ws.recv()
                    if not raw:
                        raise ConnectionError("Kalshi websocket closed")
                    message = json.loads(raw)
                    self._process(message)
            except Exception as exc:
                if self._running:
                    LOGGER.warning(
                        "Kalshi websocket error=%s reconnecting_in=%.1fs",
                        exc,
                        self.reconnect_seconds,
                    )
                    time.sleep(self.reconnect_seconds)
            finally:
                self._ready.clear()
                with self._lock:
                    self._ws = None

    @staticmethod
    def _levels(values: Any) -> dict[float, float]:
        levels: dict[float, float] = {}
        for value in values or []:
            try:
                price = float(value[0])
                quantity = float(value[1])
            except (TypeError, ValueError, IndexError):
                continue
            if quantity > 0:
                levels[price] = quantity
        return levels

    def _process(self, message: dict[str, Any]) -> None:
        message_type = str(message.get("type") or "")
        payload = message.get("msg") or {}
        ticker = str(
            payload.get("market_ticker") or payload.get("ticker") or ""
        )
        if message_type == "orderbook_snapshot" and ticker:
            with self._lock:
                self._books[ticker] = {
                    "yes_bids": self._levels(
                        payload.get("yes_dollars_fp")
                        or payload.get("yes_dollars")
                    ),
                    # With use_yes_price=true, NO-side levels arrive on the
                    # unified YES scale and are directly executable YES asks.
                    "yes_asks": self._levels(
                        payload.get("no_dollars_fp")
                        or payload.get("no_dollars")
                    ),
                }
                self._snapshots.add(ticker)
            self._ready.set()
        elif message_type == "orderbook_delta" and ticker:
            try:
                price = float(payload["price_dollars"])
                delta = float(payload.get("delta_fp", payload.get("delta", 0)))
            except (KeyError, TypeError, ValueError):
                return
            key = "yes_bids" if payload.get("side") == "yes" else "yes_asks"
            with self._lock:
                book = self._books.setdefault(
                    ticker, {"yes_bids": {}, "yes_asks": {}}
                )
                updated = book[key].get(price, 0.0) + delta
                if updated > 0:
                    book[key][price] = updated
                else:
                    book[key].pop(price, None)
        try:
            self.on_message(message)
        except Exception:
            LOGGER.exception(
                "Kalshi websocket callback failed type=%s", message_type
            )
