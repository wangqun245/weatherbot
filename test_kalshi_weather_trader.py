from __future__ import annotations

import base64
import json
import time
from datetime import date
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from kalshi_client import KalshiClient
from kalshi_execution import (
    HourlyBatch,
    KalshiHourlyExecutionManager,
    ManagedLeg,
    depth_price,
)
from kalshi_ws import KalshiWebSocketFeed
from kalshi_weather_trader import (
    contract_count_for_order,
    event_date_from_ticker,
    market_contains_temperature,
    select_order_plan,
)


def test_event_date_from_ticker() -> None:
    assert event_date_from_ticker("KXHIGHAUS-26JUN27") == date(2026, 6, 27)
    assert event_date_from_ticker("KXHIGHAUS-26JUN27-B98.5") == date(2026, 6, 27)
    assert event_date_from_ticker("bad") is None


def test_market_temperature_intervals() -> None:
    assert market_contains_temperature({"cap_strike": 94, "floor_strike": None}, 93)
    assert not market_contains_temperature({"cap_strike": 94, "floor_strike": None}, 94)
    assert market_contains_temperature({"floor_strike": 101, "cap_strike": None}, 102)
    assert not market_contains_temperature({"floor_strike": 101, "cap_strike": None}, 101)
    bracket = {"floor_strike": 96, "cap_strike": 97}
    assert market_contains_temperature(bracket, 96)
    assert market_contains_temperature(bracket, 97)
    assert not market_contains_temperature(bracket, 98)


def test_auth_signature_uses_method_and_path_without_query(monkeypatch) -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client = KalshiClient("https://example.test/trade-api/v2", api_key_id="key")
    client._private_key = private_key
    monkeypatch.setattr("kalshi_client.time.time", lambda: 1234.567)
    headers = client._auth_headers(
        "GET", "/portfolio/orders?status=resting&limit=10"
    )
    message = b"1234567GET/trade-api/v2/portfolio/orders"
    private_key.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    assert headers["KALSHI-ACCESS-KEY"] == "key"


def _config() -> dict:
    return {
        "trading": {
            "interval_snap_tolerance_f": 0.15,
            "adjacent_yes_max_total_price": 0.90,
        }
    }


def _markets() -> list[dict]:
    return [
        {
            "ticker": "LOW",
            "floor_strike": None,
            "cap_strike": 94,
            "yes_ask_dollars": "0.30",
            "no_ask_dollars": "0.80",
        },
        {
            "ticker": "MID",
            "floor_strike": 94,
            "cap_strike": 95,
            "yes_ask_dollars": "0.40",
            "no_ask_dollars": "0.60",
        },
        {
            "ticker": "HIGH",
            "floor_strike": 96,
            "cap_strike": 97,
            "yes_ask_dollars": "0.30",
            "no_ask_dollars": "0.20",
        },
        {
            "ticker": "TOP",
            "floor_strike": 97,
            "cap_strike": None,
            "yes_ask_dollars": "0.10",
            "no_ask_dollars": "0.90",
        },
    ]


def test_exact_interval_compares_target_yes_with_other_no() -> None:
    orders, reason = select_order_plan(_config(), _markets(), 94.4)
    assert len(orders) == 1
    assert orders[0]["market"]["ticker"] == "HIGH"
    assert orders[0]["side"] == "NO"
    assert orders[0]["price"] == 0.20
    assert reason.startswith("exact_interval_MID")


def test_prediction_within_point_15_compares_snapped_yes_with_other_no() -> None:
    orders, reason = select_order_plan(_config(), _markets(), 93.97)
    assert [(order["market"]["ticker"], order["side"]) for order in orders] == [
        ("HIGH", "NO")
    ]
    assert reason.startswith("boundary_snap_interval_MID")


def test_prediction_in_middle_gap_buys_adjacent_yes_pair_when_cheaper() -> None:
    markets = _markets()
    markets[0]["no_ask_dollars"] = "0.80"
    markets[3]["no_ask_dollars"] = "0.90"
    orders, reason = select_order_plan(_config(), markets, 95.5)
    assert [(order["market"]["ticker"], order["side"]) for order in orders] == [
        ("MID", "YES"),
        ("HIGH", "YES"),
    ]
    assert reason.startswith("adjacent_yes_pair")


def test_kalshi_no_order_uses_ask_and_yes_scale_price(monkeypatch) -> None:
    client = KalshiClient("https://example.test/trade-api/v2")
    captured = {}

    def fake_request(method, endpoint, **kwargs):
        captured.update({"method": method, "endpoint": endpoint, **kwargs})
        return {"order_id": "x"}

    monkeypatch.setattr(client, "request", fake_request)
    client.create_order("MARKET", "no", 10, 0.27)
    assert captured["json_body"]["side"] == "ask"
    assert captured["json_body"]["price"] == "0.7300"


def test_default_ten_contracts_respects_five_dollar_cap() -> None:
    trading = {"default_contracts": 10, "max_order_cost_dollars": 5}
    assert contract_count_for_order(0.29, trading) == 10
    assert contract_count_for_order(0.40, trading) == 10
    assert contract_count_for_order(0.75, trading) == 6
    assert contract_count_for_order(1.00, trading) == 5


def test_kalshi_fixed_shares_may_cost_less_than_five_dollars() -> None:
    body = KalshiClient.order_body(
        "KXTEST",
        "yes",
        10,
        0.29,
        time_in_force="immediate_or_cancel",
        subaccount=0,
    )
    assert body["count"] == "10.00"
    assert body["price"] == "0.2900"


def test_depth_price_requires_full_one_to_one_quantity() -> None:
    levels = [(0.20, 3), (0.25, 7)]
    assert depth_price(levels, 10, 0.85) == 0.25
    assert depth_price(levels, 11, 0.85) is None


class _FakeFeed:
    def __init__(self, levels):
        self.levels = levels

    def start(self):
        pass

    def stop(self):
        pass

    def subscribe(self, _tickers):
        pass

    def has_book(self, _ticker):
        return True

    def buy_levels(self, ticker, _side):
        return self.levels[ticker]


class _FakeClient:
    order_body = staticmethod(KalshiClient.order_body)

    def __init__(self):
        self.batches = []
        self.singles = []
        self.cancels = []

    def create_orders_batch(self, bodies):
        self.batches.append(bodies)
        return [
            {
                "order_id": f"batch-{index}",
                "fill_count": body["count"],
                "remaining_count": "0.00",
                "average_fill_price": body["price"],
            }
            for index, body in enumerate(bodies)
        ]

    def create_order(
        self,
        ticker,
        outcome_side,
        count,
        price,
        **kwargs,
    ):
        self.singles.append(
            {
                "ticker": ticker,
                "outcome_side": outcome_side,
                "count": count,
                "price": price,
                **kwargs,
            }
        )
        return {
            "order_id": "single-order",
            "fill_count": "0.00",
            "remaining_count": f"{count}.00",
        }

    def cancel_order(self, order_id, _subaccount=0):
        self.cancels.append(order_id)
        return {}


def _manager(client, feed):
    return KalshiHourlyExecutionManager(
        client=client,
        feed=feed,
        trading={
            "max_buy_price": 0.85,
            "min_buy_price": 0.01,
            "adjacent_yes_max_total_price": 0.90,
            "order_management_window_minutes": 40,
        },
        subaccount=0,
    )


def test_adjacent_batch_submits_equal_contract_counts() -> None:
    client = _FakeClient()
    feed = _FakeFeed({"LEFT": [(0.35, 10)], "RIGHT": [(0.45, 10)]})
    manager = _manager(client, feed)
    batch = HourlyBatch(
        batch_id="b",
        window_key="2026-06-28:hour_12",
        mode="adjacent",
        legs=(ManagedLeg("LEFT", "YES"), ManagedLeg("RIGHT", "YES")),
        target_shares=10,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[0, 0],
        total_cost=[0, 0],
    )
    manager._manage_adjacent(batch)
    assert len(client.batches) == 1
    assert [body["count"] for body in client.batches[0]] == [
        "10.00",
        "10.00",
    ]


def test_adjacent_total_equal_point_90_does_not_buy() -> None:
    client = _FakeClient()
    feed = _FakeFeed({"LEFT": [(0.40, 10)], "RIGHT": [(0.50, 10)]})
    manager = _manager(client, feed)
    batch = HourlyBatch(
        batch_id="b",
        window_key="2026-06-28:hour_12",
        mode="adjacent",
        legs=(ManagedLeg("LEFT", "YES"), ManagedLeg("RIGHT", "YES")),
        target_shares=10,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[0, 0],
        total_cost=[0, 0],
    )
    manager._manage_adjacent(batch)
    assert client.batches == []


def test_adjacent_pair_reduces_equal_size_to_keep_repair_under_five_dollars() -> None:
    client = _FakeClient()
    feed = _FakeFeed({"LEFT": [(0.30, 10)], "RIGHT": [(0.50, 10)]})
    manager = _manager(client, feed)
    batch = HourlyBatch(
        batch_id="b",
        window_key="2026-06-28:hour_12",
        mode="adjacent",
        legs=(ManagedLeg("LEFT", "YES"), ManagedLeg("RIGHT", "YES")),
        target_shares=10,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[0, 0],
        total_cost=[0, 0],
    )
    manager._manage_adjacent(batch)
    assert [body["count"] for body in client.batches[0]] == [
        "9.00",
        "9.00",
    ]


def test_adjacent_single_leg_fill_places_balance_repair() -> None:
    client = _FakeClient()
    feed = _FakeFeed({"LEFT": [], "RIGHT": []})
    manager = _manager(client, feed)
    batch = HourlyBatch(
        batch_id="b",
        window_key="2026-06-28:hour_12",
        mode="adjacent",
        legs=(ManagedLeg("LEFT", "YES"), ManagedLeg("RIGHT", "YES")),
        target_shares=5,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[5, 0],
        total_cost=[1.5, 0],
    )
    manager._manage_adjacent(batch)
    assert len(client.singles) == 1
    repair = client.singles[0]
    assert repair["ticker"] == "RIGHT"
    assert repair["count"] == 5
    assert repair["price"] == 0.55
    assert repair["time_in_force"] == "good_till_canceled"
    assert repair["expiration_time"] == int(batch.expires_ts)


def test_websocket_unified_yes_scale_builds_yes_and_no_asks() -> None:
    feed = KalshiWebSocketFeed(
        client=object(),
        url="wss://example.test",
        on_message=lambda _message: None,
    )
    feed._process(
        {
            "type": "orderbook_snapshot",
            "msg": {
                "market_ticker": "M",
                "yes_dollars_fp": [["0.4000", "8.00"]],
                "no_dollars_fp": [["0.5500", "7.00"]],
            },
        }
    )
    assert feed.buy_levels("M", "YES") == [(0.55, 7.0)]
    assert feed.buy_levels("M", "NO") == [(0.6, 8.0)]


def test_production_strategy_parameters_match_requested_policy() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    assert config["model"]["buy_start_hour"] == 12
    assert config["model"]["buy_end_hour"] == 16
    assert config["observations"]["station"] == "KAUS"
    assert config["observations"]["timezone"] == "America/Chicago"
    assert config["trading"]["max_buy_price"] == 0.85
    assert config["trading"]["interval_snap_tolerance_f"] == 0.15
    assert config["trading"]["adjacent_yes_max_total_price"] == 0.90
    assert config["trading"]["default_contracts"] == 10
    assert config["trading"]["max_order_cost_dollars"] == 5.00
    assert config["trading"]["order_management_window_minutes"] == 40
