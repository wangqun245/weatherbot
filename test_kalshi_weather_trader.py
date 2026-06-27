from __future__ import annotations

import base64
import json
from datetime import date
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from kalshi_client import KalshiClient
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


def test_prediction_within_point_15_snaps_to_adjacent_yes() -> None:
    orders, reason = select_order_plan(_config(), _markets(), 93.97)
    assert [(order["market"]["ticker"], order["side"]) for order in orders] == [
        ("MID", "YES")
    ]
    assert reason.startswith("boundary_snap_yes_MID")


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
    assert contract_count_for_order(0.40, trading) == 10
    assert contract_count_for_order(0.75, trading) == 6
    assert contract_count_for_order(1.00, trading) == 5


def test_production_strategy_parameters_match_requested_policy() -> None:
    config = json.loads(Path("kalshi_weather_config.json").read_text(encoding="utf-8"))
    assert config["model"]["buy_start_hour"] == 12
    assert config["model"]["buy_end_hour"] == 16
    assert config["trading"]["max_buy_price"] == 0.85
    assert config["trading"]["interval_snap_tolerance_f"] == 0.15
    assert config["trading"]["adjacent_yes_max_total_price"] == 0.90
    assert config["trading"]["default_contracts"] == 10
    assert config["trading"]["max_order_cost_dollars"] == 5.00
