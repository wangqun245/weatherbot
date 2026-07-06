from __future__ import annotations

import logging
import math
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from kalshi_client import KalshiClient
from kalshi_ws import KalshiWebSocketFeed


LOGGER = logging.getLogger("kalshi_weather_trader")


def depth_price(
    levels: list[tuple[float, float]],
    shares: int,
    max_price: float,
) -> float | None:
    """Worst executable price for exactly `shares` contracts."""
    remaining = int(shares)
    worst = 0.0
    for price, quantity in levels:
        if price > max_price + 1e-9:
            break
        take = min(remaining, int(math.floor(quantity + 1e-9)))
        if take <= 0:
            continue
        remaining -= take
        worst = max(worst, price)
        if remaining == 0:
            return round(worst, 4)
    return None


def executable_shares(
    levels: list[tuple[float, float]], max_shares: int, max_price: float
) -> int:
    available = sum(
        int(math.floor(quantity + 1e-9))
        for price, quantity in levels
        if price <= max_price + 1e-9
    )
    return max(0, min(int(max_shares), available))


@dataclass(frozen=True)
class ManagedLeg:
    ticker: str
    outcome_side: str


@dataclass
class ManagedOrder:
    order_id: str
    leg_index: int
    requested: int
    price: float
    filled: float
    remaining: float
    time_in_force: str


@dataclass
class HourlyBatch:
    batch_id: str
    window_key: str
    mode: str
    legs: tuple[ManagedLeg, ...]
    target_shares: int
    predicted_high_f: float
    created_ts: float
    expires_ts: float
    acquired: list[float]
    total_cost: list[float]
    orders: dict[str, ManagedOrder] = field(default_factory=dict)
    repair_order_id: str = ""
    closed: bool = False
    next_action_ts: float = 0.0
    target_notional_dollars: float = 0.0

    def average_price(self, index: int) -> float:
        quantity = self.acquired[index]
        return self.total_cost[index] / quantity if quantity > 0 else 0.0


class KalshiHourlyExecutionManager:
    """Manage one 40-minute weather-model accumulation batch per hour."""

    def __init__(
        self,
        client: KalshiClient,
        feed: KalshiWebSocketFeed,
        trading: dict[str, Any],
        subaccount: int,
        event_callback: Callable[[dict[str, Any]], None] | None = None,
        state_path: Path | None = None,
    ) -> None:
        self.client = client
        self.feed = feed
        self.trading = trading
        self.subaccount = subaccount
        self.event_callback = event_callback or (lambda _event: None)
        self.state_path = state_path
        self._lock = threading.RLock()
        self._batches: dict[str, HourlyBatch] = {}
        self._seen_fill_ids: set[str] = set()
        self._running = False
        self._thread: threading.Thread | None = None
        self._wakeup = threading.Event()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._recover()
        self.feed.start()
        self._thread = threading.Thread(
            target=self._loop, name="kalshi-hourly-orders", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._wakeup.set()
        with self._lock:
            batches = list(self._batches.values())
        for batch in batches:
            self.close_batch(batch, "manager_stop")
        self.feed.stop()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def on_websocket_message(self, message: dict[str, Any]) -> None:
        if message.get("type") == "user_order":
            self._apply_user_order(message.get("msg") or {})
        elif message.get("type") == "fill":
            self._apply_fill(message.get("msg") or {})
        self._wakeup.set()

    def has_window(self, window_key: str) -> bool:
        with self._lock:
            return any(
                batch.window_key == window_key and not batch.closed
                for batch in self._batches.values()
            )

    def active_batch_count(self) -> int:
        with self._lock:
            return sum(
                1 for batch in self._batches.values() if not batch.closed
            )

    def start_batch(
        self,
        *,
        window_key: str,
        mode: str,
        legs: tuple[ManagedLeg, ...],
        target_shares: int,
        predicted_high_f: float,
        target_notional_dollars: float = 0.0,
    ) -> HourlyBatch:
        if mode not in {"single", "adjacent"}:
            raise ValueError(f"Unsupported hourly mode: {mode}")
        if mode == "adjacent" and len(legs) != 2:
            raise ValueError("Adjacent mode requires exactly two legs")
        group = window_key.rsplit(":hour_", 1)[0]
        with self._lock:
            old = [
                batch
                for batch in self._batches.values()
                if batch.window_key.rsplit(":hour_", 1)[0] == group
                and not batch.closed
            ]
        for batch in old:
            self.close_batch(batch, "next_hour_model_output")

        now = time.time()
        minutes = float(
            self.trading.get("order_management_window_minutes", 40)
        )
        batch = HourlyBatch(
            batch_id=f"{window_key}:{mode}:{uuid.uuid4().hex[:10]}",
            window_key=window_key,
            mode=mode,
            legs=legs,
            target_shares=int(target_shares),
            target_notional_dollars=float(target_notional_dollars or 0.0),
            predicted_high_f=float(predicted_high_f),
            created_ts=now,
            expires_ts=now + max(1.0, minutes * 60),
            acquired=[0.0 for _ in legs],
            total_cost=[0.0 for _ in legs],
        )
        with self._lock:
            self._batches[batch.batch_id] = batch
        self._persist()
        self._refresh_subscriptions()
        self.event_callback(
            {
                "type": "batch_started",
                "batch_id": batch.batch_id,
                "window_key": window_key,
                "mode": mode,
                "legs": [leg.__dict__ for leg in legs],
                "target_shares": target_shares,
                "target_notional_dollars": target_notional_dollars,
                "expires_ts": batch.expires_ts,
            }
        )
        LOGGER.info(
            "Kalshi hourly batch started id=%s mode=%s legs=%s target=%s "
            "target_notional=%.4f window_minutes=%s",
            batch.batch_id,
            mode,
            [(leg.ticker, leg.outcome_side) for leg in legs],
            target_shares,
            float(target_notional_dollars or 0.0),
            minutes,
        )
        self._wakeup.set()
        return batch

    def close_batch(self, batch: HourlyBatch, reason: str) -> None:
        if batch.closed:
            return
        for order_id in list(batch.orders):
            self._cancel_order(batch, order_id, reason)
        batch.closed = True
        with self._lock:
            self._batches.pop(batch.batch_id, None)
        self._persist()
        self._refresh_subscriptions()
        self.event_callback(
            {
                "type": "batch_closed",
                "batch_id": batch.batch_id,
                "window_key": batch.window_key,
                "reason": reason,
                "acquired": batch.acquired,
                "average_prices": [
                    batch.average_price(index)
                    for index in range(len(batch.legs))
                ],
            }
        )
        LOGGER.info(
            "Kalshi hourly batch closed id=%s reason=%s acquired=%s",
            batch.batch_id,
            reason,
            batch.acquired,
        )

    def _refresh_subscriptions(self) -> None:
        with self._lock:
            tickers = sorted(
                {
                    leg.ticker
                    for batch in self._batches.values()
                    if not batch.closed
                    for leg in batch.legs
                }
            )
        self.feed.subscribe(tickers)

    def _loop(self) -> None:
        while self._running:
            self._wakeup.wait(timeout=1.0)
            self._wakeup.clear()
            with self._lock:
                batches = list(self._batches.values())
            for batch in batches:
                try:
                    self._manage(batch)
                except Exception:
                    LOGGER.exception(
                        "Kalshi hourly batch failed id=%s", batch.batch_id
                    )

    def _manage(self, batch: HourlyBatch) -> None:
        if batch.closed:
            return
        now = time.time()
        if now >= batch.expires_ts:
            self.close_batch(batch, "management_window_expired")
            return
        if now < batch.next_action_ts:
            return
        if not all(self.feed.has_book(leg.ticker) for leg in batch.legs):
            return
        if batch.mode == "single":
            self._manage_single(batch)
        else:
            self._manage_adjacent(batch)

    def _active_order_for_leg(
        self, batch: HourlyBatch, leg_index: int
    ) -> ManagedOrder | None:
        return next(
            (
                order
                for order in batch.orders.values()
                if order.leg_index == leg_index and order.remaining > 0
            ),
            None,
        )

    def _manage_single(self, batch: HourlyBatch) -> None:
        maximum = float(self.trading.get("max_buy_price", 0.85))
        minimum = float(self.trading.get("min_buy_price", 0.01))
        confidence_floor = float(
            self.trading.get("model_min_yes_price", 0.16)
        )
        leg = batch.legs[0]
        levels = self.feed.buy_levels(leg.ticker, leg.outcome_side)
        active = self._active_order_for_leg(batch, 0)
        if leg.outcome_side.upper() == "YES":
            if active is not None:
                self._cancel_order(
                    batch,
                    active.order_id,
                    "yes_confidence_floor_requires_live_offer",
                )
            if not levels:
                return
            if float(levels[0][0]) < confidence_floor:
                self.close_batch(batch, "yes_below_model_confidence_floor")
                return
        elif active is not None:
            return
        cost_budget = float(
            batch.target_notional_dollars
            if batch.target_notional_dollars > 0
            else self.trading.get("max_order_cost_dollars", 10.0)
        )
        budget_remaining = max(0.0, cost_budget - batch.total_cost[0])
        if budget_remaining < minimum:
            self.close_batch(batch, "cost_budget_filled")
            return
        if batch.target_notional_dollars > 0:
            remaining = int(math.floor((budget_remaining + 1e-9) / minimum))
        else:
            remaining = max(0, int(batch.target_shares - batch.acquired[0]))
        if remaining < 1:
            self.close_batch(batch, "target_filled")
            return
        available = executable_shares(levels, remaining, maximum)
        while available > 0:
            price = depth_price(levels, available, maximum)
            if (
                price is not None
                and available * price <= budget_remaining + 1e-9
            ):
                self._submit_orders(
                    batch,
                    [(0, available, price)],
                    "immediate_or_cancel",
                )
                batch.next_action_ts = time.time() + 0.5
                return
            available -= 1
        # A resting YES bid at the maximum could later execute against a price
        # below the confidence floor, so YES batches only act on live offers.
        if leg.outcome_side.upper() == "YES":
            return
        # NO legs are not model-confidence-gated and may rest at the maximum.
        resting_quantity = min(
            remaining,
            int(math.floor((budget_remaining + 1e-9) / maximum)),
        )
        if resting_quantity < 1:
            self.close_batch(batch, "cost_budget_filled")
            return
        self._submit_orders(
            batch,
            [(0, resting_quantity, maximum)],
            "good_till_canceled",
        )

    def _manage_adjacent(self, batch: HourlyBatch) -> None:
        confidence_floor = float(
            self.trading.get("model_min_yes_price", 0.16)
        )
        live_levels = [
            self.feed.buy_levels(leg.ticker, leg.outcome_side)
            for leg in batch.legs
        ]
        for leg, levels in zip(batch.legs, live_levels):
            if (
                leg.outcome_side.upper() == "YES"
                and levels
                and float(levels[0][0]) < confidence_floor
            ):
                self.close_batch(
                    batch, "adjacent_yes_below_model_confidence_floor"
                )
                return
        left, right = batch.acquired
        if abs(left - right) >= 0.5:
            richer = 0 if left > right else 1
            poorer = 1 - richer
            existing_repair = self._active_order_for_leg(batch, poorer)
            for order_id, order in list(batch.orders.items()):
                if (
                    existing_repair is not None
                    and order_id == existing_repair.order_id
                ):
                    continue
                self._cancel_order(batch, order_id, "adjacent_imbalance")
            deficit = int(abs(left - right))
            if deficit < 1:
                return
            if existing_repair is not None:
                return
            richer_price = batch.average_price(richer)
            repair_price = round(
                max(
                    float(self.trading.get("min_buy_price", 0.01)),
                    min(
                        float(self.trading.get("max_buy_price", 0.85)),
                        0.85 - richer_price,
                    ),
                ),
                4,
            )
            levels = live_levels[poorer]
            available = executable_shares(levels, deficit, repair_price)
            if available < 1:
                return
            live_price = depth_price(levels, available, repair_price)
            if live_price is None:
                return
            created = self._submit_orders(
                batch,
                [(poorer, available, live_price)],
                "immediate_or_cancel",
            )
            return

        if batch.repair_order_id:
            self._cancel_order(
                batch, batch.repair_order_id, "adjacent_balanced"
            )
            batch.repair_order_id = ""
        equal = min(left, right)
        remaining = max(0, int(batch.target_shares - equal))
        if remaining < 1:
            self.close_batch(batch, "target_filled")
            return
        # Balanced adjacent pairs never rest. Both live books must support
        # exactly the same contract count at acceptable prices.
        max_price = float(self.trading.get("max_buy_price", 0.85))
        max_total = float(
            self.trading.get("adjacent_yes_max_total_price", 0.90)
        )
        levels = live_levels
        cost_budget = float(
            self.trading.get("max_order_cost_dollars", 10.0)
        )
        remaining_budgets = [
            max(0.0, cost_budget - batch.total_cost[index])
            for index in range(2)
        ]
        common = min(
            remaining,
            executable_shares(levels[0], remaining, max_price),
            executable_shares(levels[1], remaining, max_price),
        )
        prices: tuple[float, float] | None = None
        while common >= 1:
            left_price = depth_price(levels[0], common, max_price)
            right_price = depth_price(levels[1], common, max_price)
            if (
                left_price is not None
                and right_price is not None
                and left_price + right_price < max_total - 1e-9
                and common * left_price
                <= remaining_budgets[0] + 1e-9
                and common * right_price
                <= remaining_budgets[1] + 1e-9
            ):
                left_repair_price = max(
                    float(self.trading.get("min_buy_price", 0.01)),
                    min(max_price, 0.85 - right_price),
                )
                right_repair_price = max(
                    float(self.trading.get("min_buy_price", 0.01)),
                    min(max_price, 0.85 - left_price),
                )
                if (
                    common * left_repair_price
                    <= remaining_budgets[0] + 1e-9
                    and common * right_repair_price
                    <= remaining_budgets[1] + 1e-9
                ):
                    prices = left_price, right_price
                    break
            common -= 1
        if common < 1 or prices is None:
            return
        self._submit_orders(
            batch,
            [(0, common, prices[0]), (1, common, prices[1])],
            "immediate_or_cancel",
        )
        batch.next_action_ts = time.time() + 1.0

    def _submit_orders(
        self,
        batch: HourlyBatch,
        requests: list[tuple[int, int, float]],
        time_in_force: str,
    ) -> list[ManagedOrder]:
        # Do not race the exchange at the end of the management window.
        # The REST expiration is second-granularity and the request itself
        # consumes part of the remaining lifetime.
        if (
            time_in_force == "good_till_canceled"
            and time.time() >= batch.expires_ts - 2.0
        ):
            self.close_batch(batch, "management_window_expiring")
            return []
        expiration = (
            math.ceil(batch.expires_ts)
            if time_in_force == "good_till_canceled"
            else None
        )
        bodies = []
        metadata = []
        for leg_index, quantity, price in requests:
            leg = batch.legs[leg_index]
            client_id = (
                f"weather-{batch.window_key.replace(':', '-')}-"
                f"{leg_index}-{uuid.uuid4().hex[:8]}"
            )[:64]
            bodies.append(
                self.client.order_body(
                    leg.ticker,
                    leg.outcome_side,
                    quantity,
                    price,
                    time_in_force=time_in_force,
                    subaccount=self.subaccount,
                    expiration_time=expiration,
                    client_order_id=client_id,
                )
            )
            metadata.append((leg_index, quantity, price))
        responses = (
            self.client.create_orders_batch(bodies)
            if len(bodies) > 1
            else [
                self.client.create_order(
                    bodies[0]["ticker"],
                    batch.legs[metadata[0][0]].outcome_side,
                    metadata[0][1],
                    metadata[0][2],
                    time_in_force=time_in_force,
                    subaccount=self.subaccount,
                    expiration_time=expiration,
                    client_order_id=bodies[0]["client_order_id"],
                )
            ]
        )
        created = []
        for response, (leg_index, requested, price) in zip(
            responses, metadata
        ):
            error = response.get("error")
            if error:
                LOGGER.error(
                    "Kalshi managed order rejected batch=%s leg=%s error=%s",
                    batch.batch_id,
                    leg_index,
                    error,
                )
                continue
            order_id = str(response.get("order_id") or "")
            filled = float(
                response.get("fill_count")
                or response.get("fill_count_fp")
                or 0
            )
            remaining = float(
                response.get("remaining_count")
                or response.get("remaining_count_fp")
                or max(0, requested - filled)
            )
            average_yes_scale = float(
                response.get("average_fill_price") or price
            )
            average = (
                1.0 - average_yes_scale
                if batch.legs[leg_index].outcome_side.upper() == "NO"
                and response.get("average_fill_price") is not None
                else average_yes_scale
            )
            if filled > 0:
                batch.acquired[leg_index] += filled
                batch.total_cost[leg_index] += filled * average
            order = ManagedOrder(
                order_id=order_id,
                leg_index=leg_index,
                requested=requested,
                price=price,
                filled=filled,
                remaining=remaining,
                time_in_force=time_in_force,
            )
            if (
                order_id
                and remaining > 0
                and time_in_force == "good_till_canceled"
            ):
                batch.orders[order_id] = order
            created.append(order)
            self.event_callback(
                {
                    "type": "order_submitted",
                    "batch_id": batch.batch_id,
                    "order_id": order_id,
                    "ticker": batch.legs[leg_index].ticker,
                    "outcome_side": batch.legs[leg_index].outcome_side,
                    "requested": requested,
                    "price": price,
                    "filled": filled,
                    "remaining": remaining,
                    "time_in_force": time_in_force,
                }
            )
            self._persist()
        self._persist()
        return created

    def _apply_user_order(self, payload: dict[str, Any]) -> None:
        order_id = str(payload.get("order_id") or "")
        if not order_id:
            return
        with self._lock:
            pair = next(
                (
                    (batch, batch.orders[order_id])
                    for batch in self._batches.values()
                    if order_id in batch.orders
                ),
                None,
            )
        if pair is None:
            return
        batch, order = pair
        total_filled = float(
            payload.get("fill_count_fp")
            or payload.get("fill_count")
            or order.filled
        )
        delta = max(0.0, total_filled - order.filled)
        if delta > 0:
            batch.acquired[order.leg_index] += delta
            batch.total_cost[order.leg_index] += delta * order.price
            order.filled = total_filled
        remaining_value = payload.get("remaining_count_fp")
        if remaining_value is None:
            remaining_value = payload.get("remaining_count")
        order.remaining = float(
            max(0.0, order.requested - order.filled)
            if remaining_value is None
            else remaining_value
        )
        status = str(payload.get("status") or "")
        if order.remaining <= 0 or status in {
            "canceled",
            "cancelled",
            "executed",
        }:
            batch.orders.pop(order_id, None)
        self.event_callback(
            {
                "type": "order_update",
                "batch_id": batch.batch_id,
                "window_key": batch.window_key,
                "order_id": order_id,
                "ticker": batch.legs[order.leg_index].ticker,
                "outcome_side": batch.legs[order.leg_index].outcome_side,
                "price": order.price,
                "status": status,
                "filled": order.filled,
                "delta_filled": delta,
                "remaining": order.remaining,
            }
        )
        self._persist()

    def _apply_fill(self, payload: dict[str, Any]) -> None:
        trade_id = str(payload.get("trade_id") or "")
        if trade_id and trade_id in self._seen_fill_ids:
            return
        order_id = str(payload.get("order_id") or "")
        if not order_id:
            return
        with self._lock:
            pair = next(
                (
                    (batch, batch.orders[order_id])
                    for batch in self._batches.values()
                    if order_id in batch.orders
                ),
                None,
            )
        if pair is None:
            return
        if trade_id:
            self._seen_fill_ids.add(trade_id)
        batch, order = pair
        reconciled = False
        # The create-order response may already contain this immediate fill.
        # Reconcile against Kalshi's cumulative order totals to avoid counting
        # the same fill once from REST and again from the websocket.
        try:
            current = self.client.get_order(order_id)
            self._apply_user_order(current)
            reconciled = True
        except Exception:
            LOGGER.exception(
                "Kalshi fill reconciliation failed order=%s", order_id
            )
        self.event_callback(
            {
                "type": "fill",
                "batch_id": batch.batch_id,
                "window_key": batch.window_key,
                "order_id": order_id,
                "trade_id": trade_id,
                "ticker": batch.legs[order.leg_index].ticker,
                "outcome_side": batch.legs[order.leg_index].outcome_side,
                "price": order.price,
                "quantity": float(
                    payload.get("count_fp") or payload.get("count") or 0
                ),
                "reconciled": reconciled,
            }
        )
        self._persist()

    def _cancel_order(
        self, batch: HourlyBatch, order_id: str, reason: str
    ) -> None:
        order = batch.orders.pop(order_id, None)
        if order is None:
            return
        try:
            self.client.cancel_order(order_id, self.subaccount)
        except Exception:
            LOGGER.exception(
                "Kalshi cancel failed batch=%s order=%s reason=%s",
                batch.batch_id,
                order_id,
                reason,
            )
        self.event_callback(
            {
                "type": "order_cancelled",
                "batch_id": batch.batch_id,
                "order_id": order_id,
                "reason": reason,
            }
        )
        self._persist()

    def _persist(self) -> None:
        if self.state_path is None:
            return
        with self._lock:
            rows = []
            for batch in self._batches.values():
                if batch.closed:
                    continue
                rows.append(
                    {
                        "batch_id": batch.batch_id,
                        "window_key": batch.window_key,
                        "mode": batch.mode,
                        "legs": [leg.__dict__ for leg in batch.legs],
                        "target_shares": batch.target_shares,
                        "target_notional_dollars": batch.target_notional_dollars,
                        "predicted_high_f": batch.predicted_high_f,
                        "created_ts": batch.created_ts,
                        "expires_ts": batch.expires_ts,
                        "acquired": batch.acquired,
                        "total_cost": batch.total_cost,
                        "repair_order_id": batch.repair_order_id,
                        "orders": [
                            order.__dict__ for order in batch.orders.values()
                        ],
                    }
                )
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(
            self.state_path.suffix + ".tmp"
        )
        temporary.write_text(
            json.dumps({"batches": rows}, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)

    def _recover(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            LOGGER.exception(
                "Unable to read Kalshi managed batch state=%s",
                self.state_path,
            )
            return
        now = time.time()
        recovered = []
        for row in payload.get("batches") or []:
            try:
                batch = HourlyBatch(
                    batch_id=str(row["batch_id"]),
                    window_key=str(row["window_key"]),
                    mode=str(row["mode"]),
                    legs=tuple(
                        ManagedLeg(
                            str(leg["ticker"]),
                            str(leg["outcome_side"]),
                        )
                        for leg in row["legs"]
                    ),
                    target_shares=int(row["target_shares"]),
                    target_notional_dollars=float(
                        row.get("target_notional_dollars") or 0.0
                    ),
                    predicted_high_f=float(row["predicted_high_f"]),
                    created_ts=float(row["created_ts"]),
                    expires_ts=float(row["expires_ts"]),
                    acquired=[float(value) for value in row["acquired"]],
                    total_cost=[
                        float(value) for value in row["total_cost"]
                    ],
                    repair_order_id=str(
                        row.get("repair_order_id") or ""
                    ),
                    orders={
                        str(order["order_id"]): ManagedOrder(
                            order_id=str(order["order_id"]),
                            leg_index=int(order["leg_index"]),
                            requested=int(order["requested"]),
                            price=float(order["price"]),
                            filled=float(order["filled"]),
                            remaining=float(order["remaining"]),
                            time_in_force=str(order["time_in_force"]),
                        )
                        for order in row.get("orders") or []
                    },
                )
            except (KeyError, TypeError, ValueError):
                LOGGER.exception("Invalid Kalshi managed batch state row=%s", row)
                continue
            with self._lock:
                self._batches[batch.batch_id] = batch
            if batch.expires_ts <= now:
                self.close_batch(batch, "recovered_expired")
                continue
            for order_id in list(batch.orders):
                try:
                    current = self.client.get_order(order_id)
                    self._apply_user_order(current)
                except Exception:
                    LOGGER.exception(
                        "Unable to reconcile recovered Kalshi order=%s",
                        order_id,
                    )
            recovered.append(batch.batch_id)
        if recovered:
            LOGGER.warning(
                "Recovered Kalshi managed batches=%s", recovered
            )
            self._refresh_subscriptions()
        self._persist()
