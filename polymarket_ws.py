"""Real-time Polymarket CLOB market WebSocket feed."""

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Callable


POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_USER_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


@dataclass
class TokenPrice:
    asset_id: str
    best_bid: float = 0.0
    best_ask: float = 0.0
    last_trade: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0
    timestamp: float = 0.0
    received_ts: float = 0.0
    label: str = ""
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    bid_history: list[float] = field(default_factory=list)
    ask_history: list[float] = field(default_factory=list)
    mid_history: list[float] = field(default_factory=list)

    @property
    def mid(self) -> float:
        if self.best_bid > 0 and self.best_ask > 0:
            return (self.best_bid + self.best_ask) / 2
        return self.last_trade


class PolymarketMarketFeed:
    """Maintains live best bid/ask for subscribed Polymarket token IDs."""

    def __init__(self, url: str = POLYMARKET_MARKET_WS, on_raw_message: Callable = None):
        self.url = url
        self._on_raw_message = on_raw_message
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._asset_ids: list[str] = []
        self._labels: dict[str, str] = {}
        self._prices: dict[str, TokenPrice] = {}
        self._subscription_version = 0
        self._connected = False
        self._last_message = 0.0

    def set_raw_callback(self, callback: Callable = None):
        self._on_raw_message = callback

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print("[poly-ws] Market WebSocket feed starting...")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def subscribe(self, asset_ids: list[str], labels: dict[str, str] = None):
        clean_ids = [str(asset_id) for asset_id in asset_ids if asset_id]
        labels = labels or {}
        with self._lock:
            if clean_ids == self._asset_ids:
                return
            self._asset_ids = clean_ids
            self._labels = {str(k): v for k, v in labels.items()}
            self._prices = {}
            for asset_id in clean_ids:
                self._prices[asset_id] = TokenPrice(
                    asset_id=asset_id,
                    label=self._labels.get(asset_id, ""),
                )
            self._subscription_version += 1
            self._last_message = 0.0
        short = ", ".join(f"{self._labels.get(a, '')}:{a[:8]}" for a in clean_ids)
        print(f"[poly-ws] Subscribed assets: {short}")

    def get_price(self, asset_id: str) -> Optional[TokenPrice]:
        with self._lock:
            price = self._prices.get(str(asset_id))
            if not price:
                return None
            return self._copy_price(price)

    def get_prices(self) -> dict[str, TokenPrice]:
        with self._lock:
            return {asset_id: self._copy_price(price) for asset_id, price in self._prices.items()}

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def last_message_age(self) -> float:
        if self._last_message <= 0:
            return 999999.0
        return time.time() - self._last_message

    def _run_loop(self):
        try:
            asyncio.run(self._connect_loop())
        except Exception as e:
            if self._running:
                print(f"[poly-ws] Feed stopped: {e}")

    async def _connect_loop(self):
        import websockets

        while self._running:
            try:
                with self._lock:
                    asset_ids = list(self._asset_ids)
                    version = self._subscription_version
                if not asset_ids:
                    await asyncio.sleep(0.25)
                    continue

                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._connected = True
                    await self._send_subscription(ws, asset_ids)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while self._running:
                            with self._lock:
                                if version != self._subscription_version:
                                    break
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            received_ts = time.time()
                            self._last_message = received_ts
                            self._handle_raw_message(raw, received_ts=received_ts)
                    finally:
                        ping_task.cancel()
                        self._connected = False
            except Exception as e:
                self._connected = False
                if self._running:
                    print(f"[poly-ws] WebSocket error: {e}; reconnecting in 2s")
                    await asyncio.sleep(2)

    async def _send_subscription(self, ws, asset_ids: list[str]):
        msg = {
            "assets_ids": asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(msg))

    async def _ping_loop(self, ws):
        while self._running:
            await asyncio.sleep(10)
            try:
                await ws.send("PING")
            except Exception:
                return

    def _handle_raw_message(self, raw: str, received_ts: float = None):
        received_ts = received_ts or time.time()
        if self._on_raw_message:
            try:
                self._on_raw_message(raw, received_ts=received_ts)
            except TypeError:
                self._on_raw_message(raw)
            except Exception as e:
                print(f"[poly-ws] Raw callback failed: {e}")
        try:
            payload = json.loads(raw)
        except Exception:
            return

        messages = payload if isinstance(payload, list) else [payload]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            event_type = msg.get("event_type")
            if event_type == "book":
                self._handle_book(msg, received_ts=received_ts)
            elif event_type == "price_change":
                for change in msg.get("price_changes", []):
                    self._handle_price_change(change, msg.get("timestamp"), received_ts=received_ts)
            elif event_type == "best_bid_ask":
                self._handle_best_bid_ask(msg, received_ts=received_ts)
            elif event_type == "last_trade_price":
                self._handle_last_trade(msg, received_ts=received_ts)

    def _handle_book(self, msg: dict, received_ts: float = None):
        asset_id = str(msg.get("asset_id", ""))
        if not asset_id:
            return
        bids = msg.get("bids", []) or []
        asks = msg.get("asks", []) or []
        bid_levels = self._normalize_levels(bids, highest=True)
        ask_levels = self._normalize_levels(asks, highest=False)
        best_bid, bid_size = bid_levels[0] if bid_levels else (0.0, 0.0)
        best_ask, ask_size = ask_levels[0] if ask_levels else (0.0, 0.0)
        self._update_price(
            asset_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_size=bid_size,
            ask_size=ask_size,
            bids=bid_levels,
            asks=ask_levels,
            received_ts=received_ts,
        )

    def _handle_price_change(self, msg: dict, timestamp=None, received_ts: float = None):
        asset_id = str(msg.get("asset_id", ""))
        if not asset_id:
            return
        side = str(msg.get("side") or "").upper()
        level_price = self._as_float(msg.get("price"))
        level_size = self._as_float(msg.get("size"))
        with self._lock:
            if asset_id in self._asset_ids and level_price > 0:
                price = self._prices.setdefault(
                    asset_id,
                    TokenPrice(asset_id=asset_id, label=self._labels.get(asset_id, "")),
                )
                if side in {"BUY", "BID", "BIDS"}:
                    price.bids = self._upsert_level(price.bids, level_price, level_size, highest=True)
                elif side in {"SELL", "ASK", "ASKS"}:
                    price.asks = self._upsert_level(price.asks, level_price, level_size, highest=False)
                if price.bids:
                    price.best_bid, price.bid_size = price.bids[0]
                if price.asks:
                    price.best_ask, price.ask_size = price.asks[0]
        self._update_price(
            asset_id,
            best_bid=self._as_float(msg.get("best_bid")),
            best_ask=self._as_float(msg.get("best_ask")),
            timestamp=timestamp,
            received_ts=received_ts,
        )

    def _handle_best_bid_ask(self, msg: dict, received_ts: float = None):
        asset_id = str(msg.get("asset_id", ""))
        if not asset_id:
            return
        self._update_price(
            asset_id,
            best_bid=self._as_float(msg.get("best_bid")),
            best_ask=self._as_float(msg.get("best_ask")),
            timestamp=msg.get("timestamp"),
            received_ts=received_ts,
        )

    def _handle_last_trade(self, msg: dict, received_ts: float = None):
        asset_id = str(msg.get("asset_id", ""))
        if not asset_id:
            return
        self._update_price(
            asset_id,
            last_trade=self._as_float(msg.get("price")),
            timestamp=msg.get("timestamp"),
            received_ts=received_ts,
        )

    def _update_price(
        self,
        asset_id: str,
        best_bid: float = None,
        best_ask: float = None,
        last_trade: float = None,
        bid_size: float = None,
        ask_size: float = None,
        bids: list[tuple[float, float]] = None,
        asks: list[tuple[float, float]] = None,
        timestamp=None,
        received_ts: float = None,
    ):
        with self._lock:
            if asset_id not in self._asset_ids:
                return
            price = self._prices.setdefault(
                asset_id,
                TokenPrice(asset_id=asset_id, label=self._labels.get(asset_id, "")),
            )
            if best_bid is not None and best_bid >= 0:
                price.best_bid = best_bid
            if best_ask is not None and best_ask >= 0:
                price.best_ask = best_ask
            if last_trade is not None and last_trade >= 0:
                price.last_trade = last_trade
            if bid_size is not None and bid_size >= 0:
                price.bid_size = bid_size
            if ask_size is not None and ask_size >= 0:
                price.ask_size = ask_size
            if bids is not None:
                price.bids = bids
            if asks is not None:
                price.asks = asks
            price.timestamp = self._normalize_ts(timestamp) or time.time()
            price.received_ts = received_ts or time.time()
            self._append_history(price)

    def _copy_price(self, price: TokenPrice) -> TokenPrice:
        data = price.__dict__.copy()
        for key in ("bids", "asks", "bid_history", "ask_history", "mid_history"):
            data[key] = list(data.get(key) or [])
        return TokenPrice(**data)

    def _append_history(self, price: TokenPrice) -> None:
        max_points = 200
        if price.best_bid > 0:
            self._append_changed(price.bid_history, price.best_bid)
            price.bid_history = price.bid_history[-max_points:]
        if price.best_ask > 0:
            self._append_changed(price.ask_history, price.best_ask)
            price.ask_history = price.ask_history[-max_points:]
        mid = price.mid
        if mid > 0:
            self._append_changed(price.mid_history, mid)
            price.mid_history = price.mid_history[-max_points:]

    @staticmethod
    def _append_changed(values: list[float], value: float) -> None:
        if not values or abs(values[-1] - value) > 1e-9:
            values.append(value)

    def _normalize_levels(self, levels: list, highest: bool) -> list[tuple[float, float]]:
        normalized: dict[float, float] = {}
        for level in levels:
            price = self._as_float(level.get("price") if isinstance(level, dict) else None)
            size = self._as_float(level.get("size") if isinstance(level, dict) else None)
            if price <= 0 or size <= 0:
                continue
            normalized[round(price, 4)] = normalized.get(round(price, 4), 0.0) + size
        return sorted(normalized.items(), key=lambda item: item[0], reverse=highest)

    def _upsert_level(self, levels: list[tuple[float, float]], price: float, size: float, highest: bool) -> list[tuple[float, float]]:
        rounded_price = round(price, 4)
        next_levels = [(p, s) for p, s in levels if abs(p - rounded_price) > 1e-9]
        if size > 0:
            next_levels.append((rounded_price, size))
        return sorted(next_levels, key=lambda item: item[0], reverse=highest)

    def _as_float(self, value) -> float:
        try:
            if value in ("", None):
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    def _normalize_ts(self, value) -> float:
        ts = self._as_float(value)
        if ts <= 0:
            return 0.0
        if ts > 10_000_000_000:
            return ts / 1000.0
        return ts


class PolymarketUserFeed:
    """Authenticated user WebSocket for own order and trade updates."""

    def __init__(self, api_creds, url: str = POLYMARKET_USER_WS, on_message: Callable = None):
        self.url = url
        self.api_creds = api_creds
        self._on_message = on_message
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._condition_ids: list[str] = []
        self._subscription_version = 0
        self._connected = False
        self._last_message = 0.0

    def start(self):
        if self._running:
            return
        if not self.api_creds:
            print("[poly-user-ws] No API creds; user order feed disabled")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print("[poly-user-ws] User WebSocket feed starting...")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def subscribe(self, condition_ids: list[str]):
        clean_ids = [str(condition_id) for condition_id in condition_ids if condition_id]
        with self._lock:
            if clean_ids == self._condition_ids:
                return
            self._condition_ids = clean_ids
            self._subscription_version += 1
            self._last_message = 0.0
        short = ", ".join(condition_id[:10] for condition_id in clean_ids)
        print(f"[poly-user-ws] Subscribed markets: {short}")

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def last_message_age(self) -> float:
        if self._last_message <= 0:
            return 999999.0
        return time.time() - self._last_message

    def _run_loop(self):
        try:
            asyncio.run(self._connect_loop())
        except Exception as e:
            if self._running:
                print(f"[poly-user-ws] Feed stopped: {e}")

    async def _connect_loop(self):
        import websockets

        while self._running:
            try:
                with self._lock:
                    condition_ids = list(self._condition_ids)
                    version = self._subscription_version
                if not condition_ids:
                    await asyncio.sleep(0.25)
                    continue

                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._connected = True
                    await self._send_subscription(ws, condition_ids)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while self._running:
                            with self._lock:
                                if version != self._subscription_version:
                                    break
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            received_ts = time.time()
                            self._last_message = received_ts
                            self._handle_raw_message(raw, received_ts=received_ts)
                    finally:
                        ping_task.cancel()
                        self._connected = False
            except Exception as e:
                self._connected = False
                if self._running:
                    print(f"[poly-user-ws] WebSocket error: {e}; reconnecting in 2s")
                    await asyncio.sleep(2)

    async def _send_subscription(self, ws, condition_ids: list[str]):
        msg = {
            "auth": {
                "apiKey": self.api_creds.api_key,
                "secret": self.api_creds.api_secret,
                "passphrase": self.api_creds.api_passphrase,
            },
            "markets": condition_ids,
            "type": "user",
        }
        await ws.send(json.dumps(msg))

    async def _ping_loop(self, ws):
        while self._running:
            await asyncio.sleep(10)
            try:
                await ws.send("PING")
            except Exception:
                return

    def _handle_raw_message(self, raw: str, received_ts: float = None):
        if not self._on_message:
            return
        try:
            self._on_message(raw, received_ts=received_ts or time.time())
        except TypeError:
            self._on_message(raw)
        except Exception as e:
            print(f"[poly-user-ws] Callback failed: {e}")
