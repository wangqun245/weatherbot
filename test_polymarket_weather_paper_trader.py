import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import polymarket_weather_paper_trader as bot


class ClobPricingTest(unittest.TestCase):
    def setUp(self):
        self.config = bot.default_config()
        self.market = bot.TemperatureMarket(
            event_id="event-1",
            market_id="market-1",
            condition_id="condition-1",
            city="Austin",
            kind="Highest",
            event_date="2026-06-07",
            event_title="Highest temperature in Austin on June 7",
            market_question="Will the highest temperature in Austin be between 90-91F on June 7?",
            polymarket_url="https://polymarket.com/event/example",
            yes_price=0.2,
            rule_min=90.0,
            rule_max=91.0,
            unit="F",
            raw_market_json='{"outcomes":["Yes","No"],"clobTokenIds":["yes-token","no-token"]}',
        )
        self.original_clob_get = bot.clob_get
        self.original_clob_post = bot.clob_post

    def test_model_awc_station_buy_hours_use_overrides_and_default(self):
        config = bot.default_config()

        expected = {
            "KATL": (14, 17),
            "KAUS": (14, 17),
            "KHOU": (14, 17),
            "KORD": (14, 17),
            "KDAL": (15, 17),
            "KBKF": (15, 17),
            "KLGA": (14, 17),
            "KLAX": (12, 16),
            "KSEA": (14, 17),
            "KSFO": (13, 16),
            "KMIA": (12, 16),
        }
        for station, hours in expected.items():
            self.assertEqual(hours, bot.model_awc_station_buy_hours(config, station))

    def test_model_awc_all_model_stations_are_live_by_default(self):
        config = bot.default_config()

        self.assertEqual(
            {"KATL", "KAUS", "KDAL", "KBKF", "KHOU", "KLAX", "KLGA", "KMIA", "KORD", "KSEA", "KSFO"},
            bot.model_awc_live_stations(config),
        )

    def test_production_model_awc_sizing_and_station_hours(self):
        config = bot.load_config("polymarket_weather_config.json")
        expected_hours = {
            "KATL": (14, 17),
            "KAUS": (13, 17),
            "KHOU": (14, 17),
            "KORD": (13, 16),
            "KDAL": (14, 17),
            "KBKF": (15, 17),
            "KLGA": (14, 17),
            "KLAX": (11, 15),
            "KSEA": (15, 17),
            "KSFO": (13, 16),
            "KMIA": (11, 16),
        }

        self.assertEqual(20.0, float(config["trading"]["buy_notional_usdc"]))
        self.assertEqual(
            20.0,
            float(config["trading"]["model_awc_adjacent_yes_shares"]),
        )
        self.assertEqual(
            expected_hours,
            {
                station: bot.model_awc_station_buy_hours(config, station)
                for station in expected_hours
            },
        )

    def test_model_awc_prediction_cannot_be_below_observed_high(self):
        model = SimpleNamespace(
            feature_name_=["temp_f"],
            best_iteration_=None,
            predict=lambda rows, num_iteration=None: [91.81379838220865],
        )
        features = {
            "temp_f": 91.04,
            "_observed_local_day_high_f_so_far": 91.94,
        }

        with mock.patch.object(bot, "model_awc_load_model", return_value=model):
            prediction = bot.model_awc_predict_high(self.config, features)

        self.assertEqual(91.94, prediction)

    def test_nonus_celsius_half_hour_schedule_and_snap_tolerance(self):
        config = bot.load_config("polymarket_weather_nonus_config.json")
        self.assertEqual((20, 50), bot.model_awc_station_observation_minutes(config, "EDDM"))
        self.assertEqual((0, 30), bot.model_awc_station_observation_minutes(config, "RJTT"))
        self.assertEqual((15, 18), bot.model_awc_station_buy_hours(config, "EDDM"))
        self.assertAlmostEqual(0.30625, bot.model_awc_interval_snap_tolerance(config, "C"))
        self.assertEqual(5.0, float(config["trading"]["buy_notional_usdc"]))
        self.assertEqual(5.0, float(config["trading"]["model_awc_adjacent_yes_shares"]))

    def tearDown(self):
        bot.clob_get = self.original_clob_get
        bot.clob_post = self.original_clob_post

    def test_best_buy_price_requires_executable_no_liquidity(self):
        def fake_clob_get(config, path, params=None):
            return {"bids": [], "asks": []}

        bot.clob_get = fake_clob_get

        self.assertIsNone(bot.best_buy_price(self.config, self.market, "NO"))

    def test_best_buy_price_uses_no_ask_when_available(self):
        def fake_clob_get(config, path, params=None):
            token_id = params["token_id"]
            if token_id == "no-token":
                return {"bids": [], "asks": [{"price": "0.42", "size": "100"}]}
            return {"bids": [], "asks": []}

        bot.clob_get = fake_clob_get

        self.assertEqual(0.42, bot.best_buy_price(self.config, self.market, "NO"))

    def test_best_buy_price_can_use_opposite_bid_complement(self):
        def fake_clob_get(config, path, params=None):
            token_id = params["token_id"]
            if token_id == "yes-token":
                return {"bids": [{"price": "0.63", "size": "100"}], "asks": []}
            return {"bids": [], "asks": []}

        bot.clob_get = fake_clob_get

        self.assertEqual(0.37, round(bot.best_buy_price(self.config, self.market, "NO"), 2))

    def test_best_buy_price_uses_exact_one_to_one_depth_without_extra_level(self):
        self.config["trading"]["buy_notional_usdc"] = 5.0
        self.config["trading"]["depth_price_notional_multiplier"] = 2.0
        self.config["trading"]["depth_price_extra_levels"] = 1

        def fake_clob_get(config, path, params=None):
            token_id = params["token_id"]
            if token_id == "no-token":
                return {
                    "bids": [],
                    "asks": [
                        {"price": "0.40", "size": "10"},   # $4 cumulative
                        {"price": "0.45", "size": "14"},   # $10.30 cumulative, then step one level higher
                        {"price": "0.47", "size": "100"},
                    ],
                }
            return {"bids": [], "asks": []}

        bot.clob_get = fake_clob_get

        # Production strategy intentionally ignores legacy extra-depth
        # settings and prices only the required 1:1 order quantity.
        self.assertEqual(0.45, bot.best_buy_price(self.config, self.market, "NO"))

    def test_clob_asset_sell_prices_batches_prices(self):
        captured = {}

        def fake_clob_post(config, path, payload):
            captured["path"] = path
            captured["payload"] = payload
            return {
                "asset-a": {"SELL": 0.42},
                "asset-b": {"SELL": "0.53"},
            }

        bot.clob_post = fake_clob_post

        prices = bot.clob_asset_sell_prices(self.config, ["asset-a", "asset-b", "asset-a"])

        self.assertEqual("/prices", captured["path"])
        self.assertEqual([{"token_id": "asset-a", "side": "SELL"}, {"token_id": "asset-b", "side": "SELL"}], captured["payload"])
        self.assertEqual({"asset-a": 0.42, "asset-b": 0.53}, prices)


class ModelAwcLiveSingleIntervalTest(unittest.TestCase):
    def setUp(self):
        self.config = bot.default_config()
        self.config["trading"]["live_trading_enabled"] = True
        self.config["trading"]["buy_notional_usdc"] = 10.0
        self.config["trading"]["model_awc_min_yes_price"] = 0.01
        self.config["outputs"]["trades_csv"] = "unused-trades.csv"
        self.config["outputs"]["settled_trades_csv"] = "unused-settled.csv"
        self.event = {
            "id": "event-1",
            "slug": "austin-high-june-29",
            "_parsed_kind": "Highest",
            "_parsed_event_date": "2026-06-29",
        }
        self.market = bot.TemperatureMarket(
            event_id="event-1",
            market_id="market-98-99",
            condition_id="condition-1",
            city="Austin",
            kind="Highest",
            event_date="2026-06-29",
            event_title="Highest temperature in Austin on June 29",
            market_question="Will the highest temperature in Austin be between 98-99F on June 29?",
            polymarket_url="https://polymarket.com/event/austin-high-june-29",
            yes_price=0.08,
            rule_min=98.0,
            rule_max=99.0,
            unit="F",
            raw_market_json='{"outcomes":["Yes","No"],"clobTokenIds":["yes-token","no-token"]}',
        )
        self.latest_utc = bot.datetime(2026, 6, 29, 18, 53, tzinfo=bot.timezone.utc)
        self.latest_local = self.latest_utc.astimezone(bot.ZoneInfo("America/Chicago"))
        self.latest_row = bot.FeatureMetarRow(
            daily_high_f="98.0",
            station="KAUS",
            valid_utc=self.latest_utc,
            valid_text="2026-06-29T18:53:00+00:00",
            metar="KAUS 291853Z 16010KT 10SM 37/22 A2992",
        )

    def _run_with_fake_live_trader(self, predicted_high_f, best_price=0.08, partial_now=None):
        fake_live_trader = SimpleNamespace(submissions=[], batches=[])

        def submit_buy_trade(
            config,
            cycle_id,
            market,
            wu_source,
            station,
            side,
            entry_price,
            observed_high,
            observed_low,
            reason,
            amount_usd=None,
            shares=None,
        ):
            fake_live_trader.submissions.append({
                "market_id": market.market_id,
                "side": side,
                "price": entry_price,
                "amount_usd": amount_usd,
                "shares": shares,
                "reason": reason,
            })
            return SimpleNamespace(shares=125.0)

        def start_model_awc_hourly_batch(*args, **kwargs):
            fake_live_trader.batches.append((args, kwargs))
            return "batch-1"

        fake_live_trader.submit_buy_trade = submit_buy_trade
        fake_live_trader.start_model_awc_hourly_batch = start_model_awc_hourly_batch

        with mock.patch.object(bot, "get_live_trader", return_value=fake_live_trader), \
            mock.patch.object(bot, "markets_for_event", return_value=[self.market]), \
            mock.patch.object(bot, "read_trades", return_value=[]), \
            mock.patch.object(bot, "write_csv"), \
            mock.patch.object(bot, "write_performance_reports"), \
            mock.patch.object(bot, "partial_buy_fillable_now", return_value=partial_now), \
            mock.patch.object(bot, "best_buy_price", return_value=best_price):
            trade = bot.process_model_awc_prediction(
                self.config,
                self.event,
                "Austin",
                "KAUS",
                predicted_high_f,
                self.latest_row,
                self.latest_local,
            )

        return fake_live_trader, trade

    def test_live_single_interval_buys_configured_notional_directly(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(98.51)

        self.assertIsNotNone(trade)
        self.assertEqual([], fake_live_trader.batches)
        self.assertEqual(1, len(fake_live_trader.submissions))
        submission = fake_live_trader.submissions[0]
        self.assertEqual("market-98-99", submission["market_id"])
        self.assertEqual("YES", submission["side"])
        self.assertEqual(0.08, submission["price"])
        self.assertIsNone(submission["amount_usd"])
        self.assertIsNone(submission["shares"])

    def test_live_single_interval_below_confidence_floor_skips_all_trading(self):
        self.config["trading"]["model_awc_min_yes_price"] = 0.16

        fake_live_trader, trade = self._run_with_fake_live_trader(
            98.51, best_price=0.15
        )

        self.assertIsNone(trade)
        self.assertEqual([], fake_live_trader.submissions)
        self.assertEqual([], fake_live_trader.batches)

    def test_live_boundary_snap_single_interval_buys_notional_directly(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(97.98)

        self.assertIsNotNone(trade)
        self.assertEqual([], fake_live_trader.batches)
        self.assertEqual(1, len(fake_live_trader.submissions))
        submission = fake_live_trader.submissions[0]
        self.assertEqual("market-98-99", submission["market_id"])
        self.assertEqual("YES", submission["side"])
        self.assertIsNone(submission["amount_usd"])
        self.assertIsNone(submission["shares"])
        self.assertIn("boundary_snap_yes_market-98-99", submission["reason"])

    def test_live_single_interval_insufficient_depth_starts_notional_manager(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(98.51, best_price=None)

        self.assertIsNone(trade)
        self.assertEqual([], fake_live_trader.submissions)
        self.assertEqual(1, len(fake_live_trader.batches))
        args, kwargs = fake_live_trader.batches[0]
        self.assertEqual("Austin", args[0])
        self.assertEqual("KAUS", args[1])
        self.assertEqual((self.market,), args[4])
        self.assertEqual(("YES",), args[5])
        self.assertEqual(0.0, args[6])
        self.assertEqual("single", args[8])
        self.assertEqual(10.0, kwargs["target_notional_usd"])

    def test_live_single_interval_price_above_cap_starts_resting_notional_manager(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(98.51, best_price=0.96)

        self.assertIsNone(trade)
        self.assertEqual([], fake_live_trader.submissions)
        self.assertEqual(1, len(fake_live_trader.batches))
        args, kwargs = fake_live_trader.batches[0]
        self.assertEqual((self.market,), args[4])
        self.assertEqual(("YES",), args[5])
        self.assertEqual("single", args[8])
        self.assertEqual(10.0, kwargs["target_notional_usd"])

    def test_live_boundary_snap_price_above_cap_starts_resting_notional_manager(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(97.98, best_price=0.96)

        self.assertIsNone(trade)
        self.assertEqual([], fake_live_trader.submissions)
        self.assertEqual(1, len(fake_live_trader.batches))
        args, kwargs = fake_live_trader.batches[0]
        self.assertEqual((self.market,), args[4])
        self.assertEqual(("YES",), args[5])
        self.assertEqual(10.0, kwargs["target_notional_usd"])

    def test_live_single_interval_buys_partial_before_notional_manager(self):
        fake_live_trader, trade = self._run_with_fake_live_trader(
            98.51,
            best_price=None,
            partial_now={"price": 0.08, "shares": 50.0, "amount_usd": 4.0},
        )

        self.assertIsNotNone(trade)
        self.assertEqual(1, len(fake_live_trader.submissions))
        submission = fake_live_trader.submissions[0]
        self.assertEqual("market-98-99", submission["market_id"])
        self.assertEqual("YES", submission["side"])
        self.assertEqual(0.08, submission["price"])
        self.assertEqual(4.0, submission["amount_usd"])
        self.assertEqual(50.0, submission["shares"])
        self.assertEqual(1, len(fake_live_trader.batches))
        _args, kwargs = fake_live_trader.batches[0]
        self.assertEqual(6.0, kwargs["target_notional_usd"])

    def test_live_single_manager_buys_websocket_offer_by_notional(self):
        manager = bot.LiveTradingManager(self.config)
        manager.market_feed = SimpleNamespace(
            get_price=lambda token: SimpleNamespace(best_ask=0.08, ask_size=200.0)
        )
        submissions = []

        def submit_buy_trade(
            config,
            cycle_id,
            market,
            wu_source,
            station,
            side,
            entry_price,
            observed_high,
            observed_low,
            reason,
            amount_usd=None,
            shares=None,
            notify_submitted=True,
        ):
            submissions.append({
                "price": entry_price,
                "amount_usd": amount_usd,
                "shares": shares,
                "reason": reason,
                "notify_submitted": notify_submitted,
            })
            return SimpleNamespace(live_buy_order_id="order-1")

        manager.submit_buy_trade = submit_buy_trade
        batch = bot.ModelAwcHourlyBatch(
            batch_id="Austin:KAUS:2026-06-29:hour_13:single",
            city="Austin",
            station="KAUS",
            event_date="2026-06-29",
            local_hour=13,
            mode="single",
            markets=(self.market,),
            sides=("YES",),
            token_ids=("yes-token",),
            target_shares=0.0,
            target_notional_usd=10.0,
            predicted_high_f=98.51,
            cycle_id="cycle-1",
            reason="model_awc_managed_single_hour_13",
            baseline_balances={"yes-token": 0.0},
            acquired_shares={"yes-token": 0.0},
            acquired_cost_usd={"yes-token": 0.0},
            average_prices={"yes-token": 0.0},
            open_order_ids={},
            expires_ts=bot.time.time() + 60,
        )

        with mock.patch.object(bot, "read_trades", return_value=[]), \
            mock.patch.object(bot, "write_csv"), \
            mock.patch.object(bot, "write_performance_reports"):
            manager._manage_single_hourly_batch(batch)

        self.assertEqual(1, len(submissions))
        self.assertEqual(0.08, submissions[0]["price"])
        self.assertEqual(10.0, submissions[0]["amount_usd"])
        self.assertEqual(125.0, submissions[0]["shares"])
        self.assertFalse(submissions[0]["notify_submitted"])
        self.assertEqual({"yes-token": "order-1"}, batch.open_order_ids)

    def test_live_single_manager_closes_when_yes_falls_below_confidence_floor(self):
        self.config["trading"]["model_awc_min_yes_price"] = 0.16
        manager = bot.LiveTradingManager(self.config)
        manager.market_feed = SimpleNamespace(
            get_price=lambda token: SimpleNamespace(best_ask=0.15, ask_size=200.0)
        )
        manager._close_hourly_batch = mock.Mock()
        manager._submit_batch_order = mock.Mock()
        batch = bot.ModelAwcHourlyBatch(
            batch_id="Austin:KAUS:2026-06-29:hour_13:single",
            city="Austin",
            station="KAUS",
            event_date="2026-06-29",
            local_hour=13,
            mode="single",
            markets=(self.market,),
            sides=("YES",),
            token_ids=("yes-token",),
            target_shares=0.0,
            target_notional_usd=10.0,
            predicted_high_f=98.51,
            cycle_id="cycle-1",
            reason="model_awc_managed_single_hour_13",
            baseline_balances={"yes-token": 0.0},
            acquired_shares={"yes-token": 0.0},
            acquired_cost_usd={"yes-token": 0.0},
            average_prices={"yes-token": 0.0},
            open_order_ids={},
            expires_ts=bot.time.time() + 60,
        )

        manager._manage_single_hourly_batch(batch)

        manager._close_hourly_batch.assert_called_once_with(
            batch, "yes_below_confidence_floor"
        )
        manager._submit_batch_order.assert_not_called()

    def test_managed_balance_does_not_regress_confirmed_websocket_fills(self):
        manager = bot.LiveTradingManager(self.config)
        manager.executor = SimpleNamespace(
            _get_token_balance_optional=lambda _token, refresh=False: 0.0
        )
        batch = SimpleNamespace(
            acquired_shares={"yes-token": 15.0},
            baseline_balances={"yes-token": 0.0},
        )

        self.assertEqual(15.0, manager._batch_token_balance(batch, "yes-token"))

    def test_adjacent_repair_never_chases_beyond_per_leg_target(self):
        manager = bot.LiveTradingManager(self.config)
        submissions = []
        manager._submit_batch_order = lambda batch, leg_index, shares, price, reason: submissions.append(
            (leg_index, shares, price, reason)
        )
        manager._cancel_batch_order = lambda *_args, **_kwargs: None
        batch = SimpleNamespace(
            batch_id="Chicago:KORD:2026-06-30:hour_12:adjacent",
            token_ids=("left", "right"),
            acquired_shares={"left": 18.0, "right": 0.0},
            average_prices={"left": 0.66, "right": 0.19},
            open_order_ids={},
            target_shares=10.0,
            reason="model_awc_managed_adjacent_hour_12",
            repair_token_id="",
            next_action_ts=0.0,
        )

        manager._manage_adjacent_hourly_batch(batch)

        self.assertEqual(1, len(submissions))
        self.assertEqual(10, submissions[0][1])


class ModelAwcLiveAdjacentSelectionTest(unittest.TestCase):
    def setUp(self):
        self.config = bot.default_config()
        self.config["trading"]["live_trading_enabled"] = True
        self.config["trading"]["model_awc_adjacent_yes_shares"] = 10.0
        self.config["outputs"]["trades_csv"] = "unused-trades.csv"
        self.config["outputs"]["settled_trades_csv"] = "unused-settled.csv"
        self.event = {
            "id": "event-miami",
            "slug": "miami-high-july-3",
            "_parsed_kind": "Highest",
            "_parsed_event_date": "2026-07-03",
        }
        self.markets = [
            self._market("market-86-87", 86, 87),
            self._market("market-88-89", 88, 89),
            self._market("market-90-91", 90, 91),
        ]
        self.latest_utc = bot.datetime(2026, 7, 3, 17, 53, tzinfo=bot.timezone.utc)
        self.latest_local = self.latest_utc.astimezone(bot.ZoneInfo("America/New_York"))
        self.latest_row = bot.FeatureMetarRow(
            daily_high_f="90.0",
            station="KMIA",
            valid_utc=self.latest_utc,
            valid_text="2026-07-03T17:53:00+00:00",
            metar="KMIA 031753Z 18006KT 10SM 31/23 A3005",
        )

    def _market(self, market_id, low, high):
        return bot.TemperatureMarket(
            event_id="event-miami",
            market_id=market_id,
            condition_id=f"condition-{market_id}",
            city="Miami",
            kind="Highest",
            event_date="2026-07-03",
            event_title="Highest temperature in Miami on July 3",
            market_question=f"Will Miami be between {low}-{high}F?",
            polymarket_url="https://polymarket.com/event/miami-high-july-3",
            yes_price=0.4,
            rule_min=float(low),
            rule_max=float(high),
            unit="F",
            raw_market_json=(
                '{"outcomes":["Yes","No"],'
                f'"clobTokenIds":["yes-{market_id}","no-{market_id}"]}}'
            ),
        )

    def _run(self, prices, trades=None):
        fake_live_trader = SimpleNamespace(batches=[], submissions=[])

        def start_model_awc_hourly_batch(*args, **kwargs):
            fake_live_trader.batches.append((args, kwargs))
            return "batch-1"

        fake_live_trader.start_model_awc_hourly_batch = start_model_awc_hourly_batch

        def submit_buy_trade(
            _config,
            _cycle_id,
            market,
            _wu_source,
            _station,
            side,
            entry_price,
            _observed_high,
            _observed_low,
            reason,
            amount_usd=None,
            shares=None,
        ):
            fake_live_trader.submissions.append({
                "market_id": market.market_id,
                "side": side,
                "price": entry_price,
                "amount_usd": amount_usd,
                "shares": shares,
                "reason": reason,
            })
            return SimpleNamespace(shares=shares or 0.0)

        fake_live_trader.submit_buy_trade = submit_buy_trade

        def best_price(_config, market, side, **_kwargs):
            return prices.get((market.market_id, side))

        with mock.patch.object(bot, "get_live_trader", return_value=fake_live_trader), \
            mock.patch.object(bot, "markets_for_event", return_value=self.markets), \
            mock.patch.object(bot, "read_trades", return_value=trades or []), \
            mock.patch.object(bot, "best_buy_price", side_effect=best_price), \
            mock.patch.object(bot, "write_csv"), \
            mock.patch.object(bot, "write_performance_reports"):
            result = bot.process_model_awc_prediction(
                self.config, self.event, "Miami", "KMIA", 89.7,
                self.latest_row, self.latest_local,
            )
        return fake_live_trader, result

    def test_live_adjacent_existing_yes_only_manages_missing_yes_leg(self):
        held = SimpleNamespace(
            status="OPEN",
            strategy=self.config["trading"]["strategy_name"],
            city="Miami",
            kind="Highest",
            event_date="2026-07-03",
            position_side="YES",
            market_id="market-90-91",
            shares=11.0,
            yes_price=0.69,
            total_cost_usdc=7.59,
            cycle_id="20260703T165541:model_awc_high:Miami:KMIA:2026-07-03:hour_12",
        )
        prices = {
            ("market-88-89", "YES"): 0.30,
            ("market-90-91", "YES"): 0.59,
        }

        manager, result = self._run(prices, [held])

        self.assertIsNone(result)
        self.assertEqual(1, len(manager.batches))
        args, _kwargs = manager.batches[0]
        self.assertEqual(("market-88-89",), tuple(m.market_id for m in args[4]))
        self.assertEqual(("YES",), args[5])
        self.assertEqual(10.0, args[6])
        self.assertEqual("single", args[8])

    def test_live_adjacent_cheaper_no_wins_before_existing_yes_check(self):
        held = SimpleNamespace(
            status="OPEN",
            strategy=self.config["trading"]["strategy_name"],
            city="Miami",
            kind="Highest",
            event_date="2026-07-03",
            position_side="YES",
            market_id="market-90-91",
            shares=11.0,
            yes_price=0.69,
            total_cost_usdc=7.59,
            cycle_id="20260703T165541:model_awc_high:Miami:KMIA:2026-07-03:hour_12",
        )
        prices = {
            ("market-88-89", "YES"): 0.40,
            ("market-90-91", "YES"): 0.45,
            ("market-86-87", "NO"): 0.20,
        }

        manager, result = self._run(prices, [held])

        self.assertIsNone(result)
        args, _kwargs = manager.batches[0]
        self.assertEqual(("market-86-87",), tuple(m.market_id for m in args[4]))
        self.assertEqual(("NO",), args[5])
        self.assertEqual(10.0, args[6])

    def test_live_adjacent_chooses_cheaper_non_adjacent_no_fixed_shares(self):
        prices = {
            ("market-88-89", "YES"): 0.45,
            ("market-90-91", "YES"): 0.44,
            ("market-86-87", "NO"): 0.20,
        }

        manager, result = self._run(prices)

        self.assertIsNone(result)
        args, _kwargs = manager.batches[0]
        self.assertEqual(("market-86-87",), tuple(m.market_id for m in args[4]))
        self.assertEqual(("NO",), args[5])
        self.assertEqual(10.0, args[6])
        self.assertEqual("single", args[8])

    def test_live_adjacent_chooses_two_yes_legs_when_combination_is_cheaper(self):
        prices = {
            ("market-88-89", "YES"): 0.30,
            ("market-90-91", "YES"): 0.35,
            ("market-86-87", "NO"): 0.80,
        }

        manager, result = self._run(prices)

        self.assertIsNone(result)
        args, _kwargs = manager.batches[0]
        self.assertEqual(
            ("market-88-89", "market-90-91"),
            tuple(m.market_id for m in args[4]),
        )
        self.assertEqual(("YES", "YES"), args[5])
        self.assertEqual(10.0, args[6])
        self.assertEqual("adjacent", args[8])

    def test_live_adjacent_one_yes_below_floor_treats_other_as_single(self):
        self.config["trading"]["model_awc_min_yes_price"] = 0.16
        prices = {
            ("market-88-89", "YES"): 0.15,
            ("market-90-91", "YES"): 0.35,
            ("market-86-87", "NO"): 0.80,
        }

        manager, result = self._run(prices)

        self.assertIsNotNone(result)
        self.assertEqual([], manager.batches)
        self.assertEqual(1, len(manager.submissions))
        self.assertEqual("market-90-91", manager.submissions[0]["market_id"])
        self.assertEqual("YES", manager.submissions[0]["side"])
        self.assertEqual(0.35, manager.submissions[0]["price"])

    def test_live_adjacent_one_yes_below_floor_can_choose_cheaper_other_no(self):
        self.config["trading"]["model_awc_min_yes_price"] = 0.16
        prices = {
            ("market-88-89", "YES"): 0.15,
            ("market-90-91", "YES"): 0.35,
            ("market-86-87", "NO"): 0.20,
        }

        manager, result = self._run(prices)

        self.assertIsNotNone(result)
        self.assertEqual([], manager.batches)
        self.assertEqual(1, len(manager.submissions))
        self.assertEqual("market-86-87", manager.submissions[0]["market_id"])
        self.assertEqual("NO", manager.submissions[0]["side"])
        self.assertEqual(0.20, manager.submissions[0]["price"])

    def test_live_adjacent_both_yes_below_floor_skips_all_trading(self):
        self.config["trading"]["model_awc_min_yes_price"] = 0.16
        prices = {
            ("market-88-89", "YES"): 0.15,
            ("market-90-91", "YES"): 0.12,
            ("market-86-87", "NO"): 0.20,
        }

        manager, result = self._run(prices)

        self.assertIsNone(result)
        self.assertEqual([], manager.batches)
        self.assertEqual([], manager.submissions)

    def test_live_adjacent_skips_yes_when_total_exceeds_threshold_and_no_is_not_cheaper(self):
        prices = {
            ("market-88-89", "YES"): 0.48,
            ("market-90-91", "YES"): 0.47,
            ("market-86-87", "NO"): 0.97,
        }

        manager, result = self._run(prices)

        self.assertIsNone(result)
        self.assertEqual([], manager.batches)


class MetarMomentumTest(unittest.TestCase):
    def test_websocket_message_prices_extracts_nested_changes(self):
        rows = bot.websocket_message_prices({
            "event_type": "price_change",
            "changes": [
                {"asset_id": "asset-1", "price": "0.42"},
                {"asset_id": "asset-2", "best_bid": "0.31", "best_ask": "0.33"},
            ],
        })

        self.assertNotIn(("asset-1", 0.42, "price"), rows)
        self.assertIn(("asset-2", 0.31, "best_bid"), rows)
        self.assertIn(("asset-2", 0.33, "best_ask"), rows)

    def test_momentum_target_price_moves_fraction_of_remaining_path_to_one(self):
        self.assertEqual(0.86, round(bot.momentum_target_price(0.8, 0.3), 2))
        self.assertEqual(0.37, round(bot.momentum_target_price(0.1, 0.3), 2))

    def test_directional_price_change_fraction_uses_remaining_upside_for_rises(self):
        self.assertEqual(0.03, round(bot.directional_price_change_fraction(0.8, 0.806), 2))
        self.assertLess(bot.directional_price_change_fraction(0.8, 0.805), 0.03)

    def test_directional_price_change_fraction_uses_self_price_for_drops(self):
        self.assertEqual(0.03, round(bot.directional_price_change_fraction(0.8, 0.776), 2))
        self.assertLess(bot.directional_price_change_fraction(0.8, 0.777), 0.03)

    def test_station_for_event_uses_configured_city_station_without_network(self):
        config = bot.default_config()
        config["events"].setdefault("city_stations", {})["Shanghai"] = "ZSPD"
        original = bot.extract_wunderground_source

        def fail_if_called(config, event_url):
            raise AssertionError("network should not be called")

        bot.extract_wunderground_source = fail_if_called
        try:
            self.assertEqual("ZSPD", bot.station_for_event(config, "Shanghai", "https://polymarket.com/event/example"))
        finally:
            bot.extract_wunderground_source = original

    def test_station_for_event_handles_page_timeout(self):
        config = bot.default_config()
        config["events"]["city_stations"] = {}
        original = bot.extract_wunderground_source

        def timeout(config, event_url):
            raise bot.requests.exceptions.ReadTimeout("timed out")

        bot.extract_wunderground_source = timeout
        try:
            self.assertEqual("", bot.station_for_event(config, "Shanghai", "https://polymarket.com/event/example"))
        finally:
            bot.extract_wunderground_source = original

    def test_outer_boundary_market_uses_highest_high_and_lowest_low(self):
        markets = [
            bot.TemperatureMarket("event-1", "low", "", "Austin", "Highest", "2026-06-08", "", "low", "", 0.1, None, 79.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "mid", "", "Austin", "Highest", "2026-06-08", "", "mid", "", 0.1, 80.0, 81.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "high", "", "Austin", "Highest", "2026-06-08", "", "high", "", 0.1, 82.0, None, "F", False, "{}"),
        ]

        self.assertEqual("high", bot.outer_boundary_market(markets, "F", "Highest").market_id)
        self.assertEqual("low", bot.outer_boundary_market(markets, "F", "Lowest").market_id)

    def test_websocket_relevant_markets_keep_highest_current_and_above(self):
        markets = [
            bot.TemperatureMarket("event-1", "below", "", "Austin", "Highest", "2026-06-08", "", "below", "", 0.1, 84.0, 85.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "current", "", "Austin", "Highest", "2026-06-08", "", "current", "", 0.1, 86.0, 87.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "above", "", "Austin", "Highest", "2026-06-08", "", "above", "", 0.1, 88.0, 89.0, "F", False, "{}"),
        ]

        relevant = bot.websocket_relevant_markets_for_observed_extreme(markets, "Highest", "F", 86.0, 78.0)

        self.assertEqual(["current", "above"], [m.market_id for m in relevant])

    def test_websocket_relevant_markets_keep_lowest_current_and_below(self):
        markets = [
            bot.TemperatureMarket("event-1", "below", "", "Austin", "Lowest", "2026-06-08", "", "below", "", 0.1, 74.0, 75.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "current", "", "Austin", "Lowest", "2026-06-08", "", "current", "", 0.1, 76.0, 77.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "above", "", "Austin", "Lowest", "2026-06-08", "", "above", "", 0.1, 78.0, 79.0, "F", False, "{}"),
        ]

        relevant = bot.websocket_relevant_markets_for_observed_extreme(markets, "Lowest", "F", 90.0, 77.0)

        self.assertEqual(["below", "current"], [m.market_id for m in relevant])

    def test_websocket_relevant_markets_fallback_all_without_observation(self):
        markets = [
            bot.TemperatureMarket("event-1", "one", "", "Austin", "Highest", "2026-06-08", "", "one", "", 0.1, 84.0, 85.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "two", "", "Austin", "Highest", "2026-06-08", "", "two", "", 0.1, 86.0, 87.0, "F", False, "{}"),
        ]

        relevant = bot.websocket_relevant_markets_for_observed_extreme(markets, "Highest", "F", None, None)

        self.assertEqual(["one", "two"], [m.market_id for m in relevant])

    def test_impossible_markets_for_highest_prioritize_nearest_below_observed_high(self):
        markets = [
            bot.TemperatureMarket("event-1", "far", "", "Austin", "Highest", "2026-06-08", "", "far", "", 0.1, None, 81.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "middle", "", "Austin", "Highest", "2026-06-08", "", "middle", "", 0.1, 88.0, 89.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "nearest", "", "Austin", "Highest", "2026-06-08", "", "nearest", "", 0.1, 92.0, 93.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "possible", "", "Austin", "Highest", "2026-06-08", "", "possible", "", 0.1, 94.0, 95.0, "F", False, "{}"),
        ]

        ordered = bot.deterministic_impossible_markets_by_proximity(markets, "Highest", 94.0, None, "F")

        self.assertEqual(["nearest", "middle", "far"], [m.market_id for m in ordered])

    def test_impossible_markets_for_lowest_prioritize_nearest_above_observed_low(self):
        markets = [
            bot.TemperatureMarket("event-1", "possible", "", "Austin", "Lowest", "2026-06-08", "", "possible", "", 0.1, 68.0, 69.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "nearest", "", "Austin", "Lowest", "2026-06-08", "", "nearest", "", 0.1, 71.0, 72.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "middle", "", "Austin", "Lowest", "2026-06-08", "", "middle", "", 0.1, 73.0, 74.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "far", "", "Austin", "Lowest", "2026-06-08", "", "far", "", 0.1, 80.0, None, "F", False, "{}"),
        ]

        ordered = bot.deterministic_impossible_markets_by_proximity(markets, "Lowest", None, 70.0, "F")

        self.assertEqual(["nearest", "middle", "far"], [m.market_id for m in ordered])

    def test_adjacent_no_momentum_market_uses_nearest_not_yet_impossible_bucket(self):
        high_markets = [
            bot.TemperatureMarket("event-1", "below", "", "Austin", "Highest", "2026-06-08", "", "below", "", 0.1, 78.0, 79.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "current", "", "Austin", "Highest", "2026-06-08", "", "current", "", 0.1, 80.0, 81.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "above", "", "Austin", "Highest", "2026-06-08", "", "above", "", 0.1, 82.0, 83.0, "F", False, "{}"),
        ]
        low_markets = [
            bot.TemperatureMarket("event-1", "below", "", "Austin", "Lowest", "2026-06-08", "", "below", "", 0.1, 68.0, 69.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "current", "", "Austin", "Lowest", "2026-06-08", "", "current", "", 0.1, 70.0, 71.0, "F", False, "{}"),
            bot.TemperatureMarket("event-1", "above", "", "Austin", "Lowest", "2026-06-08", "", "above", "", 0.1, 72.0, 73.0, "F", False, "{}"),
        ]

        high_candidate = bot.adjacent_no_momentum_market(high_markets, "Highest", 79.0, None, "F")
        low_candidate = bot.adjacent_no_momentum_market(low_markets, "Lowest", None, 72.0, "F")

        self.assertEqual("below", high_candidate.market_id)
        self.assertEqual("above", low_candidate.market_id)

    def test_parse_tgftp_metar_uses_observation_line_and_exact_temperature(self):
        row = bot.parse_tgftp_metar(
            "2026/06/08 17:53\n"
            "KAUS 081753Z 16015G25KT 10SM SCT017 SCT037 31/24 A2994 RMK AO2 T03110239"
        )

        self.assertIsNotNone(row)
        self.assertEqual("2026-06-08T17:53:00+00:00", row["obs_dt"].isoformat())
        self.assertEqual(31.1, row["temp_c"])

    def test_tgftp_observation_changed_uses_timestamp_and_temperature(self):
        previous = {
            "obs_dt": bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc),
            "temp_c": 31.1,
            "raw_ob": "old",
        }
        same = {
            "obs_dt": bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc),
            "temp_c": 31.1,
            "raw_ob": "same",
        }
        newer_time = {
            "obs_dt": bot.datetime(2026, 6, 8, 18, 53, tzinfo=bot.timezone.utc),
            "temp_c": 31.1,
            "raw_ob": "new-time",
        }
        newer_temp = {
            "obs_dt": bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc),
            "temp_c": 32.0,
            "raw_ob": "new-temp",
        }

        self.assertFalse(bot.tgftp_observation_changed(previous, same))
        self.assertTrue(bot.tgftp_observation_changed(previous, newer_time))
        self.assertTrue(bot.tgftp_observation_changed(previous, newer_temp))

    def test_tgftp_request_uses_cache_buster_and_no_cache_headers(self):
        response = mock.Mock()
        response.text = (
            "2026/06/08 17:53\n"
            "KAUS 081753Z 16015KT 10SM 31/24 A2994 RMK AO2 T03110239"
        )
        response.raise_for_status.return_value = None
        with mock.patch.object(bot.requests, "get", return_value=response) as get:
            row = bot.tgftp_metar_observation("kaus")

        self.assertEqual(31.1, row["temp_c"])
        url = get.call_args.args[0]
        headers = get.call_args.kwargs["headers"]
        self.assertRegex(url, r"/KAUS\.TXT\?nocache=\d+$")
        self.assertEqual("no-cache, no-store, max-age=0", headers["Cache-Control"])
        self.assertEqual("no-cache", headers["Pragma"])

    def test_merge_tgftp_replaces_delayed_awc_row_at_same_observation_time(self):
        old = {
            "obsTime": "2026-06-08T17:53:00+00:00",
            "rawOb": "KAUS 081753Z 16015KT 10SM 30/24 A2994",
            "temp": 30.0,
        }
        context = {
            "obsTime": "2026-06-08T16:53:00+00:00",
            "rawOb": "KAUS 081653Z 16015KT 10SM 29/24 A2994",
            "temp": 29.0,
        }
        obs = {
            "obs_dt": bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc),
            "temp_c": 31.1,
            "raw_ob": "KAUS 081753Z 16015KT 10SM 31/24 A2994 RMK AO2 T03110239",
        }

        merged = bot.merge_tgftp_into_aviation_rows([context, old], obs)

        self.assertEqual(2, len(merged))
        self.assertEqual(1, sum(row.get("_source") == "tgftp" for row in merged))
        self.assertEqual(obs["raw_ob"], merged[-1]["rawOb"])

    def test_model_awc_history_retries_at_most_three_times(self):
        config = bot.default_config()
        config["trading"]["model_awc_awc_max_attempts"] = 3
        config["trading"]["model_awc_awc_retry_interval_seconds"] = 0
        with mock.patch.object(
            bot,
            "aviation_metar_observations",
            side_effect=[RuntimeError("one"), RuntimeError("two"), [{"rawOb": "third"}]],
        ) as fetch:
            rows = bot.model_awc_fetch_history_with_retry(config, "KSEA", 10, "test")

        self.assertEqual([{"rawOb": "third"}], rows)
        self.assertEqual(3, fetch.call_count)
        fetch.assert_called_with("KSEA", 10)

    def test_station_report_window_uses_expected_next_obs_time(self):
        config = bot.default_config()
        bot.STATION_REPORT_TIMING.clear()
        latest = bot.datetime(2026, 6, 8, 16, 53, tzinfo=bot.timezone.utc)
        state = bot.update_station_report_timing("Austin", "KAUS", latest, 3600, 24)

        inside, _ = bot.in_station_report_window(config, "Austin", "KAUS", bot.datetime(2026, 6, 8, 17, 56, 29, tzinfo=bot.timezone.utc))
        outside, _ = bot.in_station_report_window(config, "Austin", "KAUS", bot.datetime(2026, 6, 8, 17, 56, 30, tzinfo=bot.timezone.utc))

        self.assertEqual("2026-06-08T17:53:00+00:00", state["expected_next_obs_utc"])
        self.assertTrue(inside)
        self.assertFalse(outside)

    def test_weather_record_window_stays_active_for_five_minutes(self):
        config = bot.default_config()
        bot.STATION_REPORT_TIMING.clear()
        bot.WEATHER_RECORD_ACTIVE_WINDOWS.clear()
        latest = bot.datetime(2026, 6, 8, 16, 53, tzinfo=bot.timezone.utc)
        bot.update_station_report_timing("Austin", "KAUS", latest, 3600, 24)

        at_start, _ = bot.in_station_weather_record_window(
            config, "Austin", "KAUS", bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc)
        )
        before_end, _ = bot.in_station_weather_record_window(
            config, "Austin", "KAUS", bot.datetime(2026, 6, 8, 17, 57, 59, tzinfo=bot.timezone.utc)
        )
        at_end, _ = bot.in_station_weather_record_window(
            config, "Austin", "KAUS", bot.datetime(2026, 6, 8, 17, 58, tzinfo=bot.timezone.utc)
        )

        self.assertTrue(at_start)
        self.assertTrue(before_end)
        self.assertFalse(at_end)

    def test_weather_record_window_survives_next_observation_time_refresh(self):
        config = bot.default_config()
        bot.STATION_REPORT_TIMING.clear()
        bot.WEATHER_RECORD_ACTIVE_WINDOWS.clear()
        expected = bot.datetime(2026, 6, 8, 17, 53, tzinfo=bot.timezone.utc)
        bot.update_station_report_timing(
            "Austin", "KAUS", bot.datetime(2026, 6, 8, 16, 53, tzinfo=bot.timezone.utc), 3600, 24
        )
        bot.in_station_weather_record_window(config, "Austin", "KAUS", expected)
        bot.update_station_report_timing("Austin", "KAUS", expected, 3600, 24)

        still_active, _ = bot.in_station_weather_record_window(
            config, "Austin", "KAUS", expected + bot.timedelta(seconds=299)
        )

        self.assertTrue(still_active)

    def test_price_record_payload_includes_millisecond_timestamp(self):
        payload = bot.price_record_payload(
            "websocket_tick",
            {"city": "Austin", "station": "KAUS", "market_id": "m1", "position_side": "NO"},
            "asset-1",
            0.42,
            0.40,
            "price",
            {"expected_next_obs_utc": "2026-06-08T17:53:00+00:00"},
        )

        self.assertEqual("websocket_tick", payload["event_type"])
        self.assertEqual("asset-1", payload["asset_id"])
        self.assertIsInstance(payload["captured_at_epoch_ms"], int)
        self.assertIn(".", payload["captured_at_utc"])

    def test_window_snapshot_seeds_momentum_base_from_previous_price_for_trigger_asset(self):
        config = bot.default_config()
        bot.STATION_REPORT_TIMING.clear()
        bot.PRICE_RECORDING_WINDOWS.clear()
        bot.PRICE_MOMENTUM_WINDOWS.clear()
        latest = bot.datetime.now(bot.timezone.utc) - bot.timedelta(hours=1, seconds=1)
        bot.update_station_report_timing("Austin", "KAUS", latest, 3600, 24)
        with tempfile.TemporaryDirectory() as tmp:
            config["outputs"]["price_window_ticks_jsonl"] = str(Path(tmp) / "ticks.jsonl")
            assets = {
                "trigger-asset": {"city": "Austin", "station": "KAUS", "market_id": "m1", "position_side": "NO", "last_price": 0.99},
                "other-asset": {"city": "Austin", "station": "KAUS", "market_id": "m2", "position_side": "YES", "last_price": 0.40},
            }

            bot.record_price_window_tick(config, assets, assets["trigger-asset"], "trigger-asset", 0.99, 0.30, "price")

        self.assertNotIn("trigger-asset:price", bot.PRICE_MOMENTUM_WINDOWS)
        self.assertEqual(0.40, bot.PRICE_MOMENTUM_WINDOWS["other-asset:last_price"]["base_price"])

    def test_best_bid_has_independent_momentum_window_baseline(self):
        config = bot.default_config()
        bot.STATION_REPORT_TIMING.clear()
        bot.PRICE_RECORDING_WINDOWS.clear()
        bot.PRICE_MOMENTUM_WINDOWS.clear()
        latest = bot.datetime.now(bot.timezone.utc) - bot.timedelta(hours=1, seconds=1)
        bot.update_station_report_timing("Houston", "KHOU", latest, 3600, 24)
        with tempfile.TemporaryDirectory() as tmp:
            config["outputs"]["price_window_ticks_jsonl"] = str(Path(tmp) / "ticks.jsonl")
            assets = {
                "asset-1": {
                    "city": "Houston",
                    "station": "KHOU",
                    "market_id": "m1",
                    "position_side": "NO",
                    "last_price": 0.53,
                    "last_prices_by_field": {"price": 0.53, "best_bid": 0.56},
                },
            }

            bot.record_price_window_tick(config, assets, assets["asset-1"], "asset-1", 0.67, 0.56, "best_bid")

        self.assertEqual(0.56, bot.PRICE_MOMENTUM_WINDOWS["asset-1:best_bid"]["base_price"])
        self.assertNotIn("asset-1:price", bot.PRICE_MOMENTUM_WINDOWS)

    def test_websocket_message_prices_extracts_best_bid_ask_event(self):
        rows = bot.websocket_message_prices({
            "event_type": "best_bid_ask",
            "asset_id": "asset-1",
            "best_bid": "0.67",
            "best_ask": "0.73",
            "timestamp": "1781100000000",
        })

        self.assertIn(("asset-1", 0.67, "best_bid"), rows)
        self.assertIn(("asset-1", 0.73, "best_ask"), rows)

    def test_websocket_message_prices_maps_last_trade_price(self):
        rows = bot.websocket_message_prices({
            "event_type": "last_trade_price",
            "asset_id": "asset-1",
            "price": "0.52",
        })

        self.assertEqual([("asset-1", 0.52, "last_price")], rows)

    def test_websocket_message_prices_extracts_book_top_of_book(self):
        rows = bot.websocket_message_prices({
            "event_type": "book",
            "asset_id": "asset-1",
            "bids": [{"price": "0.60", "size": "4"}, {"price": "0.62", "size": "1"}],
            "asks": [{"price": "0.72", "size": "2"}, {"price": "0.70", "size": "8"}],
        })

        self.assertIn(("asset-1", 0.62, "best_bid"), rows)
        self.assertIn(("asset-1", 0.70, "best_ask"), rows)


class WeatherRecordTest(unittest.TestCase):
    def tearDown(self):
        bot.WEATHER_RECORD_POINTS_BY_STATION_EVENT.clear()
        while not bot.WEATHER_RECORD_UPDATES.empty():
            bot.WEATHER_RECORD_UPDATES.get_nowait()

    def test_parse_weather_record_timestamp_accepts_epoch_milliseconds(self):
        parsed = bot.parse_weather_record_timestamp(1781278848236)

        self.assertIsNotNone(parsed)
        self.assertEqual("2026-06-12T15:40:48.236000+00:00", parsed.isoformat())

    def test_normalize_weather_record_websocket_payload_matches_api_shape(self):
        rows = bot.normalize_weather_record_payload({
            "value": [{
                "StationCode": "kaus",
                "TimeStamp": 1781278848236,
                "Temperature": 31.0,
            }]
        })

        self.assertEqual(1, len(rows))
        self.assertEqual("KAUS", rows[0]["station"])
        self.assertEqual(31.0, rows[0]["temp_f"])
        self.assertEqual("F", rows[0]["source_unit"])
        self.assertEqual("2026-06-12T15:40:48.236000+00:00", rows[0]["obs_dt"].isoformat())

    def test_normalize_weather_record_websocket_accepts_single_row(self):
        rows = bot.normalize_weather_record_payload({
            "stationCode": "khou",
            "timestamp": "2026-06-12T16:00:00Z",
            "temperature": 32,
        })

        self.assertEqual(1, len(rows))
        self.assertEqual("KHOU", rows[0]["station"])
        self.assertEqual(32.0, rows[0]["temp_f"])

    def test_normalize_weather_record_converts_explicit_celsius_to_fahrenheit(self):
        rows = bot.normalize_weather_record_payload({
            "StationCode": "KAUS",
            "TimeStamp": "2026-06-12T16:00:00Z",
            "Temperature": 31,
            "TemperatureUnit": "C",
        })

        self.assertEqual(1, len(rows))
        self.assertAlmostEqual(87.8, rows[0]["temp_f"])
        self.assertEqual("C", rows[0]["source_unit"])

    def test_normalize_weather_record_does_not_reconvert_fahrenheit(self):
        rows = bot.normalize_weather_record_payload({
            "StationCode": "KAUS",
            "TimeStamp": "2026-06-12T16:00:00Z",
            "Temperature": "88°F",
        })

        self.assertEqual(88.0, rows[0]["temp_f"])
        self.assertEqual("F", rows[0]["source_unit"])

    def test_weather_record_observed_extremes_accumulates_station_event_day_points(self):
        config = bot.default_config()
        rows = [
            {
                "station": "KAUS",
                "obs_dt": bot.datetime(2026, 6, 12, 13, 0, tzinfo=bot.timezone.utc),
                "temp_c": 29.0,
                "raw": {},
            },
            {
                "station": "KAUS",
                "obs_dt": bot.datetime(2026, 6, 12, 14, 0, tzinfo=bot.timezone.utc),
                "temp_c": 31.0,
                "raw": {},
            },
        ]

        high, low, latest_dt, points = bot.weather_record_observed_extremes(
            config, "KAUS", "Austin", "2026-06-12", "F", rows
        )

        self.assertEqual(88, high)
        self.assertEqual(84, low)
        self.assertEqual("2026-06-12T09:00:00-05:00", latest_dt.isoformat())
        self.assertEqual(2, len(points))

    def test_weather_record_observed_extremes_uses_websocket_fahrenheit_directly(self):
        config = bot.default_config()
        rows = bot.normalize_weather_record_payload({
            "StationCode": "KAUS",
            "TimeStamp": "2026-06-12T14:00:00Z",
            "Temperature": 88,
        })

        high, low, _, _ = bot.weather_record_observed_extremes(
            config, "KAUS", "Austin", "2026-06-12", "F", rows
        )

        self.assertEqual(88, high)
        self.assertEqual(88, low)

    def test_raw_websocket_record_does_not_require_parsed_price_rows(self):
        config = bot.default_config()
        bot.PRICE_RECORDING_WINDOWS.clear()
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "raw.jsonl"
            config["outputs"]["price_window_raw_jsonl"] = str(raw_path)
            bot.PRICE_RECORDING_WINDOWS["Austin:KAUS:2026-06-08T17:53:00+00:00"] = {
                "record_until_monotonic": bot.time.monotonic() + 60,
                "started_at_utc": "2026-06-08T17:53:00.000+00:00",
                "record_seconds": 300,
            }

            bot.record_raw_price_window_message(config, {}, '{"event_type":"heartbeat"}', [], 1780941180.123)

            rows = raw_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(1, len(rows))
        self.assertEqual('{"event_type":"heartbeat"}', rows[0])

    def test_report_schedule_ignores_special_short_interval_minutes(self):
        times = [
            bot.datetime(2026, 6, 8, hour, 53, tzinfo=bot.timezone.utc)
            for hour in range(3, 24)
        ] + [
            bot.datetime(2026, 6, 9, hour, 53, tzinfo=bot.timezone.utc)
            for hour in range(0, 3)
        ] + [
            bot.datetime(2026, 6, 9, 2, 56, tzinfo=bot.timezone.utc)
        ]

        latest, interval, count, minutes, expected = bot.infer_report_schedule_from_times(times, 24)

        self.assertEqual("2026-06-09T02:56:00+00:00", latest.isoformat())
        self.assertEqual(3600, interval)
        self.assertEqual(25, count)
        self.assertEqual([53], minutes)
        self.assertEqual("2026-06-09T03:53:00+00:00", expected.isoformat())

    def test_report_schedule_supports_twice_hourly_minutes(self):
        times = []
        for hour in range(24):
            times.append(bot.datetime(2026, 6, 8, hour, 23, tzinfo=bot.timezone.utc))
            times.append(bot.datetime(2026, 6, 8, hour, 53, tzinfo=bot.timezone.utc))
        times.append(bot.datetime(2026, 6, 8, 12, 31, tzinfo=bot.timezone.utc))

        latest, interval, count, minutes, expected = bot.infer_report_schedule_from_times(times, 24)

        self.assertEqual(1800, interval)
        self.assertEqual(49, count)
        self.assertEqual([23, 53], minutes)
        self.assertEqual("2026-06-09T00:23:00+00:00", expected.isoformat())


class TelegramNotificationTest(unittest.TestCase):
    def setUp(self):
        self.config = bot.default_config()
        self.market = bot.TemperatureMarket(
            event_id="event-1",
            market_id="market-1",
            condition_id="condition-1",
            city="Austin",
            kind="Highest",
            event_date="2026-06-08",
            event_title="Highest temperature in Austin on June 8",
            market_question="Will the highest temperature in Austin be between 90-91F on June 8?",
            polymarket_url="https://polymarket.com/event/example",
            yes_price=0.4,
            rule_min=90.0,
            rule_max=91.0,
            unit="F",
            raw_market_json='{"outcomes":["Yes","No"],"clobTokenIds":["yes-token","no-token"]}',
        )
        self.messages = []
        self.original_get_telegram_notifier = bot.get_telegram_notifier
        self.original_best_sell_price = bot.best_sell_price
        self.original_best_buy_price = bot.best_buy_price

        class FakeNotifier:
            enabled = True

            def __init__(self, messages):
                self.messages = messages

            def send(self, message, silent=False):
                self.messages.append(message)

        fake = FakeNotifier(self.messages)
        bot.get_telegram_notifier = lambda config: fake
        bot.best_sell_price = lambda config, market, side: 0.55
        bot.best_buy_price = lambda config, market, side: 0.90

    def tearDown(self):
        bot.get_telegram_notifier = self.original_get_telegram_notifier
        bot.best_sell_price = self.original_best_sell_price
        bot.best_buy_price = self.original_best_buy_price

    def test_paper_buy_and_sell_send_telegram_notifications(self):
        trade = bot.make_trade(self.config, "cycle-1", self.market, "", "KAUS", "YES", 0.4, 91.0, 78.0, "unit_test_buy")

        bot.notify_trade(self.config, trade, "BUY", "FILLED", "unit_test_buy")
        self.assertEqual(1, len(self.messages))
        self.assertIn("*PAPER BUY FILLED*", self.messages[0])
        self.assertIn("Austin Highest 2026-06-08", self.messages[0])

        self.assertTrue(bot.close_trade(self.config, trade, self.market, "unit_test_sell"))
        self.assertEqual(2, len(self.messages))
        self.assertIn("*PAPER SELL CLOSED*", self.messages[1])
        self.assertIn("P&L:", self.messages[1])

    def test_no_exit_buys_yes_hedge_when_better_than_selling_no(self):
        trade = bot.make_trade(self.config, "cycle-1", self.market, "", "KAUS", "NO", 0.4, 92.0, 78.0, "unit_test_buy")
        bot.best_sell_price = lambda config, market, side: 0.30
        bot.best_buy_price = lambda config, market, side: 0.60

        self.assertTrue(bot.close_trade(self.config, trade, self.market, "unit_test_invalidated"))

        self.assertEqual("HEDGED", trade.status)
        self.assertEqual("buy_yes_hedge", trade.exit_action)
        self.assertEqual(0.60, trade.exit_yes_price)
        self.assertGreater(trade.exit_hedge_cost_usdc, 0)

    def test_hedged_no_unwinds_yes_when_no_becomes_impossible_again(self):
        trade = bot.make_trade(self.config, "cycle-1", self.market, "", "KAUS", "NO", 0.4, 92.0, 78.0, "unit_test_buy")
        trade.status = "HEDGED"
        trade.exit_action = "buy_yes_hedge"
        trade.exit_hedge_cost_usdc = 7.5
        bot.best_sell_price = lambda config, market, side: 0.70 if side == "YES" else 0.30

        self.assertTrue(
            bot.unwind_yes_hedge_if_no_impossible(
                self.config,
                trade,
                self.market,
                92.0,
                78.0,
                "F",
                "unit_test_unwind",
            )
        )

        self.assertEqual("OPEN", trade.status)
        self.assertEqual("sell_yes_hedge", trade.exit_action)
        self.assertEqual(0.70, trade.exit_yes_price)
        self.assertEqual("yes_hedge_unwound_no_impossible", trade.settlement_source)


if __name__ == "__main__":
    unittest.main()
