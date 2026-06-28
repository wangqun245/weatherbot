from __future__ import annotations

import base64
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


class KalshiApiError(RuntimeError):
    pass


class KalshiClient:
    """Minimal Kalshi Trade API V2 client using official RSA-PSS authentication."""

    def __init__(
        self,
        base_url: str,
        api_key_id: str = "",
        private_key_path: str = "",
        timeout_seconds: float = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key_id = api_key_id.strip()
        self.private_key_path = private_key_path.strip()
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "weatherbot-kalshi/1.0"})
        self._private_key: Any = None

    def _load_private_key(self) -> Any:
        if self._private_key is not None:
            return self._private_key
        if not self.api_key_id:
            raise KalshiApiError("KALSHI_API_KEY_ID is required for private requests")
        if not self.private_key_path:
            raise KalshiApiError("KALSHI_PRIVATE_KEY_PATH is required for private requests")
        key_path = Path(self.private_key_path).expanduser()
        if not key_path.exists():
            raise KalshiApiError(f"Kalshi private key does not exist: {key_path}")
        self._private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None
        )
        return self._private_key

    def _auth_headers(self, method: str, endpoint: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        signing_path = "/trade-api/v2" + endpoint.split("?", 1)[0]
        return self._signed_headers(timestamp, method, signing_path)

    def _signed_headers(
        self, timestamp: str, method: str, signing_path: str
    ) -> dict[str, str]:
        message = f"{timestamp}{method.upper()}{signing_path}".encode("utf-8")
        signature = self._load_private_key().sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
        }

    def websocket_auth_headers(self) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        return self._signed_headers(
            timestamp, "GET", "/trade-api/ws/v2"
        )

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        private: bool = False,
    ) -> dict[str, Any]:
        method = method.upper()
        headers = self._auth_headers(method, endpoint) if private else {}
        response = self.session.request(
            method,
            self.base_url + endpoint,
            params=params,
            json=json_body,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            body = response.text[:1000]
            raise KalshiApiError(
                f"Kalshi {method} {endpoint} failed: HTTP {response.status_code}: {body}"
            )
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise KalshiApiError(f"Unexpected Kalshi response for {endpoint}: {payload!r}")
        return payload

    def get_series(self, series_ticker: str) -> dict[str, Any]:
        return self.request("GET", f"/series/{series_ticker}").get("series", {})

    def get_open_markets(self, series_ticker: str) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params: dict[str, Any] = {
                "series_ticker": series_ticker,
                "status": "open",
                "limit": 1000,
            }
            if cursor:
                params["cursor"] = cursor
            payload = self.request("GET", "/markets", params=params)
            markets.extend(payload.get("markets") or [])
            cursor = str(payload.get("cursor") or "")
            if not cursor:
                break
        return markets

    def get_orderbook(self, ticker: str, depth: int = 20) -> dict[str, Any]:
        return self.request(
            "GET", f"/markets/{ticker}/orderbook", params={"depth": depth}
        ).get("orderbook_fp", {})

    def get_balance(self) -> dict[str, Any]:
        return self.request("GET", "/portfolio/balance", private=True)

    def validate_credentials(self) -> dict[str, Any]:
        self._load_private_key()
        return self.get_balance()

    def get_positions(self, ticker: str = "") -> list[dict[str, Any]]:
        params = {"ticker": ticker} if ticker else None
        return self.request(
            "GET", "/portfolio/positions", params=params, private=True
        ).get("market_positions", [])

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self.request(
            "GET", f"/portfolio/orders/{order_id}", private=True
        ).get("order", {})

    def get_orders(
        self,
        *,
        status: str = "",
        ticker: str = "",
        subaccount: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": 1000}
        if status:
            params["status"] = status
        if ticker:
            params["ticker"] = ticker
        if subaccount is not None:
            params["subaccount"] = subaccount
        return self.request(
            "GET", "/portfolio/orders", params=params, private=True
        ).get("orders", [])

    @staticmethod
    def order_body(
        ticker: str,
        outcome_side: str,
        count: int,
        outcome_price_dollars: float,
        *,
        time_in_force: str,
        subaccount: int,
        expiration_time: int | None = None,
        client_order_id: str = "",
    ) -> dict[str, Any]:
        outcome_side = outcome_side.lower()
        if outcome_side not in {"yes", "no"}:
            raise ValueError("outcome_side must be 'yes' or 'no'")
        book_side = "bid" if outcome_side == "yes" else "ask"
        yes_scale_price = (
            outcome_price_dollars
            if outcome_side == "yes"
            else 1.0 - outcome_price_dollars
        )
        body: dict[str, Any] = {
            "ticker": ticker,
            "client_order_id": client_order_id or str(uuid.uuid4()),
            "side": book_side,
            "count": f"{int(count)}.00",
            "price": f"{yes_scale_price:.4f}",
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            "cancel_order_on_pause": True,
            "subaccount": int(subaccount),
        }
        if expiration_time is not None:
            if time_in_force != "good_till_canceled":
                raise ValueError(
                    "expiration_time requires good_till_canceled"
                )
            body["expiration_time"] = int(expiration_time)
        return body

    def create_order(
        self,
        ticker: str,
        outcome_side: str,
        count: int,
        outcome_price_dollars: float,
        *,
        time_in_force: str = "immediate_or_cancel",
        subaccount: int = 0,
        expiration_time: int | None = None,
        client_order_id: str = "",
    ) -> dict[str, Any]:
        body = self.order_body(
            ticker,
            outcome_side,
            count,
            outcome_price_dollars,
            time_in_force=time_in_force,
            subaccount=subaccount,
            expiration_time=expiration_time,
            client_order_id=client_order_id,
        )
        return self.request(
            "POST",
            "/portfolio/events/orders",
            json_body=body,
            private=True,
        )

    def create_orders_batch(
        self,
        orders: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return self.request(
            "POST",
            "/portfolio/events/orders/batched",
            json_body={"orders": orders},
            private=True,
        ).get("orders", [])

    def create_yes_buy(
        self,
        ticker: str,
        count: int,
        price_dollars: float,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return self.create_order(ticker, "yes", count, price_dollars, **kwargs)

    def cancel_order(self, order_id: str, subaccount: int = 0) -> dict[str, Any]:
        endpoint = f"/portfolio/events/orders/{order_id}"
        if subaccount:
            endpoint += "?" + urlencode({"subaccount": subaccount})
        return self.request("DELETE", endpoint, private=True)
