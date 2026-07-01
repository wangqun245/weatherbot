from __future__ import annotations

import base64
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

import kalshi_ws
from kalshi_client import KalshiClient
from kalshi_execution import (
    HourlyBatch,
    KalshiHourlyExecutionManager,
    ManagedLeg,
    ManagedOrder,
    depth_price,
)
from kalshi_ws import KalshiWebSocketFeed
import kalshi_weather_trader as trader
from kalshi_weather_trader import (
    build_feature_row,
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


def test_websocket_auth_signature_uses_ws_path(monkeypatch) -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client = KalshiClient("https://example.test/trade-api/v2", api_key_id="key")
    client._private_key = private_key
    monkeypatch.setattr("kalshi_client.time.time", lambda: 1234.567)
    headers = client.websocket_auth_headers()
    message = b"1234567GET/trade-api/ws/v2"
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
    assert headers["Content-Type"] == "application/json"


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


class _FeatureModel:
    feature_name_ = [
        "temp_f_lag_1h",
        "metar_obs_max_temp_f_past_6h",
        "metar_obs_min_temp_f_past_6h",
        "metar_obs_latest_temp_f_past_6h",
        "metar_obs_temp_range_f_past_6h",
        "extra_metar_count_past_6h",
        "has_extra_metar_past_6h",
    ]


class _LowPredictionModel:
    feature_name_ = ["temp_f"]
    best_iteration_ = None

    def predict(self, rows, num_iteration=None):
        return [91.81379838220865]


def test_prediction_cannot_be_below_observed_local_day_high() -> None:
    prediction = trader.predict(
        _LowPredictionModel(),
        {
            "temp_f": 91.04,
            "_observed_local_day_high_f_so_far": 91.94,
        },
    )

    assert prediction == 91.94


def test_build_feature_row_adds_rolling_metar_context() -> None:
    config = {
        "observations": {
            "station": "KMIA",
            "timezone": "America/New_York",
            "regular_observation_minute": 53,
            "lag_tolerance_minutes": 30,
        },
        "model": {
            "buy_start_hour": 12,
            "buy_end_hour": 16,
        },
    }
    rows = [
        {
            "obsTime": "2026-06-29T05:53:00+00:00",
            "rawOb": "METAR KMIA 290553Z 09007KT 10SM FEW027 28/23 A3002 RMK AO2 SLP164 T02830228 10284 20278 $",
        },
        {
            "obsTime": "2026-06-29T10:53:00+00:00",
            "rawOb": "METAR KMIA 291053Z 11004KT 10SM SCT025 28/23 A3003 RMK AO2 SLP168 T02830233 $",
        },
        {
            "obsTime": "2026-06-29T11:53:00+00:00",
            "rawOb": "METAR KMIA 291153Z 11005KT 10SM FEW027 29/24 A3004 RMK AO2 SLP172 T02940239 10294 20283 $",
        },
        {
            "obsTime": "2026-06-29T12:53:00+00:00",
            "rawOb": "METAR KMIA 291253Z VRB04KT 10SM SCT028 31/24 A3006 RMK AO2 SLP178 T03110239 $",
        },
        {
            "obsTime": "2026-06-29T13:53:00+00:00",
            "rawOb": "METAR KMIA 291353Z 16004KT 10SM FEW036 31/24 A3007 RMK AO2 SLP183 T03110239 $",
        },
        {
            "obsTime": "2026-06-29T14:53:00+00:00",
            "rawOb": "METAR KMIA 291453Z 08007KT 10SM SCT035 SCT050 SCT250 32/24 A3008 RMK AO2 SLP185 T03220239 $",
        },
        {
            "obsTime": "2026-06-29T15:53:00+00:00",
            "rawOb": "METAR KMIA 291553Z 09007KT 10SM SCT034TCU BKN050 32/24 A3008 RMK AO2 SLP186 TCU E T03220239 $",
        },
        {
            "obsTime": "2026-06-29T16:53:00+00:00",
            "rawOb": "METAR KMIA 291653Z 09013G19KT 10SM SCT033TCU BKN046 BKN250 32/24 A3008 RMK AO2 SLP186 T03170239 $",
        },
    ]

    built = build_feature_row(config, _FeatureModel(), rows, date(2026, 6, 29))

    assert built is not None
    features, latest, latest_local = built
    assert latest.valid_utc == datetime(2026, 6, 29, 16, 53, tzinfo=timezone.utc)
    assert latest_local.hour == 12
    assert features["temp_f"] == 89.06
    assert features["metar_obs_count_past_6h"] == 7
    assert features["metar_obs_max_temp_f_past_6h"] == 89.96000000000001
    assert features["metar_obs_min_temp_f_past_6h"] == 82.94
    assert features["metar_obs_latest_temp_f_past_6h"] == 89.06
    assert features["metar_obs_temp_range_f_past_6h"] == 7.02000000000001
    assert features["_observed_local_day_high_f_so_far"] == 89.96000000000001
    assert features["extra_metar_count_past_6h"] == 0
    assert features["has_extra_metar_past_6h"] == 0
    assert features["asos_6h_max_temp_f"] == 84.91999999999999
    assert features["asos_6h_extrema_age_minutes"] == 300.0
    assert features["asos_previous_6h_max_temp_f"] == 83.12
    assert features["asos_previous_6h_extrema_age_minutes"] == 660.0


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


def test_partial_contract_plan_caps_cost_at_target_notional() -> None:
    trading = {
        "min_buy_price": 0.01,
        "max_buy_price": 0.85,
    }
    plan = trader.partial_contract_plan_for_notional(
        [(0.08, 50), (0.12, 200)],
        10.0,
        trading,
    )

    assert plan == {"contracts": 83.0, "price": 0.12, "cost": 9.96}


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


def test_single_manager_uses_remaining_notional_not_fixed_shares() -> None:
    client = _FakeClient()
    feed = _FakeFeed({"SINGLE": [(0.08, 200)]})
    manager = _manager(client, feed)
    batch = HourlyBatch(
        batch_id="b",
        window_key="2026-06-28:hour_12",
        mode="single",
        legs=(ManagedLeg("SINGLE", "YES"),),
        target_shares=10,
        target_notional_dollars=10.0,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[0],
        total_cost=[0],
    )

    manager._manage_single(batch)

    assert len(client.singles) == 1
    order = client.singles[0]
    assert order["ticker"] == "SINGLE"
    assert order["count"] == 125
    assert order["price"] == 0.08
    assert order["time_in_force"] == "immediate_or_cancel"


def test_run_cycle_single_interval_buys_partial_before_kalshi_manager(monkeypatch, tmp_path) -> None:
    target_day = date(2026, 6, 29)
    config = {
        "kalshi": {
            "series_ticker": "KXHIGHAUS",
            "subaccount": 0,
        },
        "market": {
            "target_date": target_day.isoformat(),
            "expected_rules_text": "",
        },
        "observations": {
            "station": "KAUS",
            "timezone": "America/Chicago",
        },
        "trading": {
            "live_enabled": True,
            "dry_run": False,
            "one_order_per_hour": True,
            "default_contracts": 10,
            "max_order_cost_dollars": 10.0,
            "min_buy_price": 0.01,
            "max_buy_price": 0.85,
            "interval_snap_tolerance_f": 0.15,
            "adjacent_yes_max_total_price": 0.90,
            "time_in_force": "immediate_or_cancel",
        },
        "outputs": {
            "state_json": str(tmp_path / "state.json"),
            "trades_jsonl": str(tmp_path / "trades.jsonl"),
            "order_events_jsonl": str(tmp_path / "order_events.jsonl"),
        },
    }
    market = {
        "ticker": "M98",
        "event_ticker": "KXHIGHAUS-26JUN29",
        "floor_strike": 98,
        "cap_strike": 99,
        "yes_ask_dollars": "0.08",
        "no_ask_dollars": "0.92",
        "rules_primary": "",
    }

    class Client:
        def __init__(self):
            self.orders = []

        def get_open_markets(self, _series):
            return [dict(market)]

        def get_orderbook(self, _ticker, depth=100):
            return {"yes_dollars": [], "no_dollars": [(0.92, 50)]}

        def create_order(self, ticker, outcome_side, count, price, **kwargs):
            self.orders.append(
                {
                    "ticker": ticker,
                    "outcome_side": outcome_side,
                    "count": count,
                    "price": price,
                    **kwargs,
                }
            )
            return {
                "order_id": "ioc-1",
                "fill_count": str(count),
                "remaining_count": "0.00",
                "average_fill_price": price,
            }

    class Manager:
        def __init__(self):
            self.started = []

        def has_window(self, _window_key):
            return False

        def start_batch(self, **kwargs):
            self.started.append(kwargs)
            return SimpleNamespace(
                batch_id="batch-1",
                legs=kwargs["legs"],
            )

    latest = trader.MetarRow(
        daily_high_f="98.0",
        station="KAUS",
        valid_utc=datetime(2026, 6, 29, 18, 53, tzinfo=timezone.utc),
        valid_text="2026-06-29T18:53:00+00:00",
        metar="KAUS 291853Z 16010KT 10SM 37/22 A2992",
    )
    monkeypatch.setattr(
        trader,
        "build_feature_row",
        lambda *_args, **_kwargs: (
            {},
            latest,
            datetime(2026, 6, 29, 13, 53, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(trader, "predict", lambda *_args, **_kwargs: 98.5)
    client = Client()
    manager = Manager()
    notifier = mock.Mock()

    trader.run_cycle(
        config,
        client,
        object(),
        execution_manager=manager,
        source_rows=[],
        notifier=notifier,
    )

    assert len(client.orders) == 1
    assert client.orders[0]["ticker"] == "M98"
    assert client.orders[0]["outcome_side"] == "yes"
    assert client.orders[0]["count"] == 50
    assert client.orders[0]["price"] == 0.08
    assert len(manager.started) == 1
    assert manager.started[0]["mode"] == "single"
    assert manager.started[0]["target_notional_dollars"] == 6.0
    messages = [call.args[0] for call in notifier.send.call_args_list]
    assert any(message.startswith("*Kalshi LIVE BUY FILLED*") for message in messages)
    assert any("Contracts: 50" in message for message in messages)
    events = [
        json.loads(line)
        for line in (tmp_path / "order_events.jsonl").read_text().splitlines()
    ]
    assert events[0]["source"] == "single_interval_immediate_ioc"
    assert events[0]["filled"] == 50


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


class _WsAuthClient:
    api_key_id = "key-dbc7"

    def websocket_auth_headers(self) -> dict[str, str]:
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": "123",
            "KALSHI-ACCESS-SIGNATURE": "sig",
            "Content-Type": "application/json",
        }


def test_websocket_connect_falls_back_and_promotes_url(monkeypatch) -> None:
    calls = []
    connected = object()

    def fake_create_connection(url, header, timeout):
        calls.append((url, header, timeout))
        if "external-api-ws" in url:
            raise RuntimeError("Handshake status 403 Forbidden")
        return connected

    monkeypatch.setattr(
        "kalshi_ws.websocket.create_connection",
        fake_create_connection,
    )
    feed = KalshiWebSocketFeed(
        client=_WsAuthClient(),
        url="wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        fallback_urls=["wss://api.elections.kalshi.com/trade-api/ws/v2"],
        on_message=lambda _message: None,
    )

    assert feed._connect() is connected
    assert [call[0] for call in calls] == [
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        "wss://api.elections.kalshi.com/trade-api/ws/v2",
    ]
    assert feed.url == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    assert feed.urls[0] == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    assert "Content-Type: application/json" in calls[1][1]


def test_websocket_idle_timeout_sends_ping_without_reconnect(monkeypatch) -> None:
    class IdleThenClosed:
        def __init__(self):
            self.calls = 0
            self.pings = 0
            self.timeout = None

        def settimeout(self, value):
            self.timeout = value

        def send(self, _payload):
            return None

        def recv(self):
            self.calls += 1
            if self.calls == 1:
                raise kalshi_ws.websocket.WebSocketTimeoutException("idle")
            return ""

        def ping(self):
            self.pings += 1

    ws = IdleThenClosed()
    feed = KalshiWebSocketFeed(
        client=_WsAuthClient(),
        url="wss://example.test",
        on_message=lambda _message: None,
        read_timeout_seconds=30,
    )
    feed._running = True
    monkeypatch.setattr(feed, "_connect", lambda: ws)
    monkeypatch.setattr(feed, "_subscription_commands", lambda _tickers: [])
    feed._tickers = {"M"}
    monkeypatch.setattr(feed, "reconnect_seconds", 0)

    def stop_after_close(_seconds):
        feed._running = False

    monkeypatch.setattr("kalshi_ws.time.sleep", stop_after_close)
    feed._run()

    assert ws.timeout == 30
    assert ws.pings == 1


def test_production_strategy_parameters_match_requested_policy() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    assert config["model"]["buy_start_hour"] == 12
    assert config["model"]["buy_end_hour"] == 16
    assert config["model"]["station_buy_hours"] == {
        "KATL": [14, 16],
        "KAUS": [14, 17],
        "KBOS": [14, 17],
        "KDCA": [14, 17],
        "KDFW": [16, 17],
        "KLAS": [13, 16],
        "KMDW": [14, 16],
        "KMIA": [12, 16],
        "KMSP": [14, 16],
        "KOKC": [14, 17],
        "KPHL": [14, 17],
        "KPHX": [14, 16],
        "KSAT": [14, 17],
        "KSEA": [14, 17],
        "KSFO": [14, 17],
    }
    assert (
        config["kalshi"]["websocket_url"]
        == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    )
    assert config["kalshi"]["websocket_fallback_urls"] == [
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    ]
    assert config["kalshi"]["websocket_read_timeout_seconds"] == 30
    assert config["observations"]["station"] == "KAUS"
    assert config["observations"]["timezone"] == "America/Chicago"
    assert config["trading"]["max_buy_price"] == 0.85
    assert config["trading"]["interval_snap_tolerance_f"] == 0.15
    assert config["trading"]["adjacent_yes_max_total_price"] == 0.90
    assert config["trading"]["default_contracts"] == 10
    assert config["trading"]["max_order_cost_dollars"] == 10.00
    assert config["trading"]["live_stations"] == ["KAUS", "KLAS", "KMIA"]
    assert config["trading"]["order_management_window_minutes"] == 40
    assert config["observations"]["lookback_hours"] == 24
    assert config["observations"]["awc_prefetch_hours"] == 24
    assert config["observations"]["awc_fallback_hours"] == 24
    assert config["observations"]["awc_max_attempts"] == 3
    assert config["observations"]["awc_retry_interval_seconds"] == 3
    assert config["observations"]["tgftp_start_delay_seconds"] == 60
    assert config["observations"]["tgftp_poll_interval_seconds"] == 2
    assert config["observations"]["tgftp_poll_timeout_seconds"] == 300


def test_station_buy_hours_use_station_overrides_and_default() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    expected = dict(config["model"]["station_buy_hours"])
    for station, hours in expected.items():
        item = json.loads(json.dumps(config))
        item["observations"]["station"] = station
        assert trader.station_buy_hours(item) == tuple(hours)

    config["observations"]["station"] = "KNYC"
    assert trader.station_buy_hours(config) == (12, 16)


def test_city_configs_only_enable_three_live_stations() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    city_configs = trader.configured_city_configs(config)

    assert len(city_configs) == 16
    live = {
        item["observations"]["station"]
        for item in city_configs
        if item["trading"]["live_enabled"]
        and not item["trading"]["dry_run"]
    }
    assert live == {"KAUS", "KLAS", "KMIA"}
    assert all(
        item["trading"]["dry_run"]
        for item in city_configs
        if item["observations"]["station"] not in live
    )


def test_kalshi_telegram_title_has_platform_prefix() -> None:
    notifier = mock.Mock()
    trader.notify_kalshi_trade(
        notifier,
        {
            "dry_run": False,
            "station": "KMIA",
            "prediction_f": 90.25,
            "orders": [{"outcome_side": "YES", "contracts": 10, "market_ticker": "M"}],
        },
    )

    message = notifier.send.call_args.args[0]
    assert message.startswith("*Kalshi LIVE TRADE*")


def test_kalshi_execution_fill_sends_telegram_notification() -> None:
    notifier = mock.Mock()
    trader.notify_kalshi_execution_event(
        notifier,
        {
            "type": "order_update",
            "window_key": "KMIA:2026-06-29:hour_13",
            "order_id": "order-1",
            "ticker": "KXHIGHMIA-26JUN29-B90",
            "outcome_side": "YES",
            "price": 0.08,
            "delta_filled": 25,
        },
    )

    message = notifier.send.call_args.args[0]
    assert message.startswith("*Kalshi LIVE BUY FILLED*")
    assert "Station: *KMIA*" in message
    assert "Contracts: 25" in message
    assert "Amount: $2.00" in message


def test_kalshi_managed_user_order_event_includes_fill_delta() -> None:
    events = []
    client = _FakeClient()
    feed = _FakeFeed({"M": []})
    manager = KalshiHourlyExecutionManager(
        client=client,
        feed=feed,
        trading={
            "max_buy_price": 0.85,
            "min_buy_price": 0.01,
            "adjacent_yes_max_total_price": 0.90,
            "order_management_window_minutes": 40,
        },
        subaccount=0,
        event_callback=events.append,
    )
    batch = HourlyBatch(
        batch_id="batch-1",
        window_key="KMIA:2026-06-29:hour_13",
        mode="single",
        legs=(ManagedLeg("M", "YES"),),
        target_shares=10,
        predicted_high_f=90,
        created_ts=time.time(),
        expires_ts=time.time() + 2400,
        acquired=[0],
        total_cost=[0],
        orders={
            "order-1": ManagedOrder(
                order_id="order-1",
                leg_index=0,
                requested=10,
                price=0.08,
                filled=0,
                remaining=10,
                time_in_force="good_till_canceled",
            )
        },
    )
    manager._batches[batch.batch_id] = batch

    manager._apply_user_order(
        {
            "order_id": "order-1",
            "fill_count": "4",
            "remaining_count": "6",
            "status": "resting",
        }
    )

    assert events[-1]["type"] == "order_update"
    assert events[-1]["window_key"] == "KMIA:2026-06-29:hour_13"
    assert events[-1]["ticker"] == "M"
    assert events[-1]["outcome_side"] == "YES"
    assert events[-1]["price"] == 0.08
    assert events[-1]["delta_filled"] == 4


def test_tgftp_request_is_no_cache_and_parses_latest_metar() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    response = mock.Mock()
    response.text = (
        "2026/06/28 18:53\n"
        "KAUS 281853Z 17012KT 10SM 35/22 A2990 RMK AO2 T03500222"
    )
    response.raise_for_status.return_value = None
    with mock.patch.object(trader.requests, "get", return_value=response) as get:
        observation = trader.fetch_tgftp_metar(config)

    assert observation["obs_dt"] == datetime(
        2026, 6, 28, 18, 53, tzinfo=timezone.utc
    )
    assert observation["raw_ob"].startswith("KAUS 281853Z")
    url = get.call_args.args[0]
    headers = get.call_args.kwargs["headers"]
    assert "/KAUS.TXT?nocache=" in url
    assert headers["Cache-Control"] == "no-cache, no-store, max-age=0"
    assert headers["Pragma"] == "no-cache"


def test_tgftp_merge_replaces_same_awc_timestamp() -> None:
    observation = {
        "obs_dt": datetime(2026, 6, 28, 18, 53, tzinfo=timezone.utc),
        "raw_ob": "KAUS 281853Z 17012KT 10SM 35/22 A2990",
    }
    rows = [
        {
            "obsTime": "2026-06-28T17:53:00+00:00",
            "rawOb": "KAUS 281753Z 17012KT 10SM 34/22 A2990",
        },
        {
            "obsTime": "2026-06-28T18:53:00+00:00",
            "rawOb": "KAUS 281853Z 17012KT 10SM 34/22 A2990",
        },
    ]

    merged = trader.merge_tgftp_metar(rows, observation)

    assert len(merged) == 2
    assert sum(row.get("_source") == "tgftp" for row in merged) == 1
    assert merged[-1]["rawOb"] == observation["raw_ob"]


def test_awc_history_stops_after_three_attempts() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    config["observations"]["awc_retry_interval_seconds"] = 0
    with mock.patch.object(
        trader,
        "fetch_metars",
        side_effect=[RuntimeError("one"), RuntimeError("two"), [{"rawOb": "ok"}]],
    ) as fetch:
        rows = trader.fetch_metars_with_retry(config, 11, "test")

    assert rows == [{"rawOb": "ok"}]
    assert fetch.call_count == 3
    fetch.assert_called_with(config, 11)
