from executor import Executor, calculate_order_size
from polymarket_weather_paper_trader import LivePendingOrder, LiveTradingManager


TOKEN = "123456789"


def test_calculate_order_size_does_not_round_up_to_five_dollars() -> None:
    assert calculate_order_size(0.29, 2.90) == (10.0, 2.90)
    assert calculate_order_size(0.80, 5.00) == (6.0, 4.80)


def test_fixed_ten_share_buy_below_five_dollars_is_allowed() -> None:
    executor = Executor("", dry_run=True)
    executor._minimum_order_shares_cache[TOKEN] = 5.0
    result = executor.place_buy_order_shares(
        TOKEN, shares=10, price=0.29, neg_risk=True
    )
    assert result.success
    assert result.shares == 10
    assert result.amount_usd == 2.90


def test_fixed_share_buy_below_market_minimum_is_rejected() -> None:
    executor = Executor("", dry_run=True)
    executor._minimum_order_shares_cache[TOKEN] = 5.0
    result = executor.place_buy_order_shares(
        TOKEN, shares=4, price=0.29, neg_risk=True
    )
    assert not result.success
    assert "below market minimum 5 shares" in result.error


def test_fixed_share_buy_can_use_dynamic_max_buy_price_override() -> None:
    executor = Executor("", dry_run=True)
    executor._minimum_order_shares_cache[TOKEN] = 1.0

    rejected = executor.place_buy_order_shares(
        TOKEN, shares=10, price=0.99, neg_risk=True
    )
    assert not rejected.success
    assert "cap $0.90" in rejected.error

    accepted = executor.place_buy_order_shares(
        TOKEN,
        shares=10,
        price=0.99,
        neg_risk=True,
        max_buy_price=0.995,
    )
    assert accepted.success
    assert accepted.price == 0.99


def test_notional_buy_can_use_dynamic_max_buy_price_override() -> None:
    executor = Executor("", dry_run=True)
    executor._minimum_order_shares_cache[TOKEN] = 1.0

    rejected = executor.place_buy_order(
        TOKEN, amount_usd=10, price=0.99, neg_risk=True
    )
    assert not rejected.success
    assert "cap $0.90" in rejected.error

    accepted = executor.place_buy_order(
        TOKEN,
        amount_usd=10,
        price=0.99,
        neg_risk=True,
        max_buy_price=0.995,
    )
    assert accepted.success
    assert accepted.price == 0.99


class _BalanceClient:
    def __init__(self) -> None:
        self.balance_calls = 0
        self.update_calls = 0
        self.order_calls = 0

    def get_balance_allowance(self, _params):
        self.balance_calls += 1
        return {"balance": 10_000_000}

    def update_balance_allowance(self, _params):
        self.update_calls += 1

    def get_order(self, _order_id):
        self.order_calls += 1
        return {"status": "live", "size_matched": "0", "price": "0.29"}


def test_token_balance_uses_short_ttl_cache_without_refresh_endpoint() -> None:
    executor = Executor("", dry_run=False)
    client = _BalanceClient()
    executor.client = client
    executor._initialized = True
    for _ in range(100):
        assert executor._get_token_balance_optional(TOKEN) == 10.0
    assert client.balance_calls == 1
    assert client.update_calls == 0


def test_pending_buy_checks_order_before_balance_fallback() -> None:
    executor = Executor("", dry_run=False)
    client = _BalanceClient()
    executor.client = client
    executor._initialized = True
    executor.get_balance = lambda refresh=False: (_ for _ in ()).throw(
        AssertionError("balance endpoint must not be called")
    )
    executor._get_token_balance_optional = (
        lambda token_id, refresh=False: (_ for _ in ()).throw(
            AssertionError("token balance endpoint must not be called")
        )
    )
    assert (
        executor.check_pending_buy(
            "order", 0.29, 10, TOKEN, 100.0, 0.0
        )
        is None
    )
    assert client.order_calls == 1


def test_managed_hourly_order_skips_one_second_rest_poll() -> None:
    class FakeExecutor:
        def __init__(self):
            self.checks = 0

        def check_pending_buy(self, *_args):
            self.checks += 1
            return None

    manager = LiveTradingManager(
        {"trading": {"live_order_timeout_seconds": 20}}
    )
    manager.executor = FakeExecutor()
    pending = LivePendingOrder(
        kind="BUY",
        trade_id="trade",
        order_id="order",
        token_id=TOKEN,
        condition_id="condition",
        price=0.29,
        shares=10,
        created_ts=0,
    )
    manager._pending[pending.order_id] = pending
    manager._managed_order_ids.add(pending.order_id)
    manager.poll_pending_orders()
    assert manager.executor.checks == 0
