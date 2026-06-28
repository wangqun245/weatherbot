from executor import Executor, calculate_order_size


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
