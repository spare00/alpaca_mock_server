from datetime import date, datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import mock_server
from mock_server import MockState, _mock_chart_series, _order_payload, _strategy_from_client_order_id
from mock_server import _resolve_replay_cache_dir


class MockServerTests(unittest.TestCase):
    def test_replay_cache_defaults_to_tmp_and_can_be_disabled(self):
        self.assertEqual(_resolve_replay_cache_dir(""), "/tmp/alpaca_mock_replay_cache")
        self.assertIsNone(_resolve_replay_cache_dir("off"))
        self.assertEqual(_resolve_replay_cache_dir("/private/tmp/custom"), "/private/tmp/custom")

    def test_market_fill_uses_quote_side(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_quote("HPE", "33.88", "34.09", "2026-05-14T13:36:00Z")

        self.assertEqual(state.fill_price("HPE", "buy", now), 34.09)
        self.assertEqual(state.fill_price("HPE", "sell", now), 33.88)

    def test_market_fill_falls_back_to_mid_when_quote_side_missing(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("HPE", "34.00", "2026-05-14T13:36:00Z")

        self.assertEqual(state.fill_price("HPE", "buy", now), 34.0)
        self.assertEqual(state.fill_price("HPE", "sell", now), 34.0)

    def test_replay_sell_fill_rejects_quote_far_below_latest_bar(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "41.6687", "41.70", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "sell", now), 44.85)

    def test_replay_buy_fill_rejects_quote_far_above_latest_bar(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "48.90", "49.00", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "buy", now), 45.11)

    def test_replay_fill_keeps_quote_inside_bar_guard(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 11))
        now = datetime(2026, 5, 11, 13, 31, 50, tzinfo=timezone.utc)
        state.remember_market_data(
            {
                "bars": {
                    "B": [
                        {
                            "o": 44.95,
                            "h": 45.11,
                            "l": 44.85,
                            "c": 45.07,
                            "vw": 45.038282,
                            "t": "2026-05-11T13:31:00Z",
                        }
                    ]
                }
            }
        )
        state.remember_market_quote("B", "44.80", "45.16", "2026-05-11T13:31:50Z")

        self.assertEqual(state.fill_price("B", "sell", now), 44.8)
        self.assertEqual(state.fill_price("B", "buy", now), 45.16)

    def test_replay_clock_uses_speed_multiplier(self):
        wall_start = datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc)
        wall_now = datetime(2026, 5, 26, 0, 0, 10, tzinfo=timezone.utc)
        with patch.object(mock_server, "_utc_now", side_effect=[wall_start, wall_now]):
            state = MockState(
                "100000",
                True,
                alpaca_historical_et_date=date(2026, 5, 11),
                replay_speed=3.0,
            )

            self.assertEqual(state.replay_now_utc(), datetime(2026, 5, 11, 13, 30, 30, tzinfo=timezone.utc))

    def test_replay_clock_can_snap_to_fixed_step(self):
        wall_start = datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc)
        wall_now = datetime(2026, 5, 26, 0, 0, 11, tzinfo=timezone.utc)
        with patch.object(mock_server, "_utc_now", side_effect=[wall_start, wall_now]):
            state = MockState(
                "100000",
                True,
                alpaca_historical_et_date=date(2026, 5, 11),
                replay_speed=3.0,
                replay_step_seconds=30.0,
            )

            self.assertEqual(state.replay_now_utc(), datetime(2026, 5, 11, 13, 30, 30, tzinfo=timezone.utc))

    def test_replay_clock_advances_to_next_weekday_session(self):
        wall_start = datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc)
        # 6.5 regular-session hours at 1x speed -> Tue 09:30 ET
        wall_now = wall_start + timedelta(hours=6, minutes=30)
        with patch.object(mock_server, "_utc_now", side_effect=[wall_start, wall_now, wall_now]):
            state = MockState(
                "100000",
                True,
                alpaca_historical_et_date=date(2026, 5, 18),
                alpaca_historical_et_end_date=date(2026, 5, 22),
                replay_speed=1.0,
            )

            replay_now = state.replay_now_utc()
            self.assertEqual(replay_now, datetime(2026, 5, 19, 13, 30, tzinfo=timezone.utc))
            self.assertEqual(replay_now.astimezone(mock_server._NY).date(), date(2026, 5, 19))

    def test_strategy_is_parsed_from_bk_client_order_id(self):
        self.assertEqual(
            _strategy_from_client_order_id("bk-si-uber-1778508121000-b-deadbeef"),
            "steady_intraday",
        )
        self.assertEqual(
            _strategy_from_client_order_id("bk-mei-cifr-1778508440000-s-deadbeef"),
            "macd_early_impulse",
        )
        self.assertEqual(_strategy_from_client_order_id("codex-uber-1778508121000-buy-deadbeef"), "")

    def test_chart_series_filters_trade_events_by_strategy(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("UBER", "75.00", "2026-05-14T13:36:00Z")
        state.record_tracked_symbols(["UBER"])

        steady_order = _order_payload(
            "11111111-1111-4111-8111-111111111111",
            {
                "symbol": "UBER",
                "qty": "1",
                "side": "buy",
                "client_order_id": "bk-si-uber-1778508121000-b-deadbeef",
            },
            "filled",
            "1",
            "75.00",
            now,
        )
        macd_order = _order_payload(
            "22222222-2222-4222-8222-222222222222",
            {
                "symbol": "UBER",
                "qty": "1",
                "side": "sell",
                "client_order_id": "bk-mei-uber-1778508181000-s-feedface",
            },
            "filled",
            "1",
            "75.10",
            now,
        )
        state.apply_fill(steady_order)
        state.apply_fill(macd_order)

        code, body = _mock_chart_series(
            state,
            {"symbols": ["UBER"], "minutes": ["60"], "timeframe": ["1Min"], "strategy": ["steady_intraday"]},
        )

        self.assertEqual(code, 200)
        self.assertEqual(body["trade_strategies"], ["macd_early_impulse", "steady_intraday"])
        self.assertEqual(body["symbols_with_trades"], ["UBER"])
        self.assertEqual(body["trade_counts_by_symbol"], {"UBER": 1})
        self.assertEqual(len(body["trade_events"]), 1)
        self.assertEqual(body["trade_events"][0]["strategy"], "steady_intraday")
        self.assertEqual(body["trade_events"][0]["qty"], "1")
        self.assertEqual(body["trade_events"][0]["fill_stage"], "entry")

    def test_trade_events_mark_partial_and_full_sell_quantities(self):
        state = MockState("100000", True)
        now = datetime(2026, 5, 14, 13, 36, tzinfo=timezone.utc)
        state.remember_market_price("BB", "10.00", "2026-05-14T13:36:00Z")
        state.record_tracked_symbols(["BB"])

        buy_order = _order_payload(
            "11111111-1111-4111-8111-111111111111",
            {"symbol": "BB", "qty": "10", "side": "buy"},
            "filled",
            "10",
            "10.00",
            now,
        )
        partial_sell = _order_payload(
            "22222222-2222-4222-8222-222222222222",
            {"symbol": "BB", "qty": "4", "side": "sell"},
            "filled",
            "4",
            "10.50",
            now,
        )
        full_sell = _order_payload(
            "33333333-3333-4333-8333-333333333333",
            {"symbol": "BB", "qty": "6", "side": "sell"},
            "filled",
            "6",
            "11.00",
            now,
        )

        state.apply_fill(buy_order)
        state.apply_fill(partial_sell)
        state.apply_fill(full_sell)

        code, body = _mock_chart_series(
            state,
            {"symbols": ["BB"], "minutes": ["60"], "timeframe": ["1Min"]},
        )

        self.assertEqual(code, 200)
        events = body["trade_events"]
        self.assertEqual([ev["qty"] for ev in events], ["10", "4", "6"])
        self.assertEqual([ev["fill_stage"] for ev in events], ["entry", "partial", "full"])


class ReplayTradeQuotePairingTests(unittest.TestCase):
    def test_quote_row_at_or_before_rejects_stale_nbbo(self):
        rows = [
            (datetime(2026, 5, 6, 18, 9, 4, tzinfo=timezone.utc), {"bp": 70.5, "ap": 70.52, "t": "2026-05-06T18:09:04Z"}),
            (datetime(2026, 5, 6, 18, 9, 34, tzinfo=timezone.utc), {"bp": 70.59, "ap": 70.6, "t": "2026-05-06T18:09:34Z"}),
        ]
        trade_dt = datetime(2026, 5, 6, 18, 9, 35, tzinfo=timezone.utc)
        fresh = mock_server._quote_row_at_or_before(rows, trade_dt, 2.0)
        stale = mock_server._quote_row_at_or_before(rows, trade_dt, 0.5)
        self.assertEqual(fresh, rows[1][1])
        self.assertIsNone(stale)

    def test_replay_paired_messages_emit_quote_before_trade(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 6))
        trade_body = {
            "trades": {
                "KRE": [
                    {"t": "2026-05-06T18:09:35.100Z", "p": 70.55, "s": 100},
                    {"t": "2026-05-06T18:09:35.200Z", "p": 70.56, "s": 120},
                ]
            }
        }
        quote_range_body = {
            "quotes": {
                "KRE": [
                    {"t": "2026-05-06T18:09:34.606Z", "bp": 70.59, "ap": 70.6, "bs": 100, "as": 100},
                    {"t": "2026-05-06T18:09:35.050Z", "bp": 70.54, "ap": 70.55, "bs": 100, "as": 100},
                ]
            }
        }
        messages, emitted = mock_server._ws_replay_paired_trade_quote_messages(
            state,
            ["KRE"],
            trade_body,
            quote_range_body,
        )
        self.assertEqual(emitted, {"KRE"})
        self.assertEqual(len(messages), 3)
        self.assertEqual([msg["T"] for msg in messages], ["q", "t", "t"])
        sorted_messages = sorted(messages, key=mock_server._stream_message_sort_key)
        self.assertEqual([msg["T"] for msg in sorted_messages], ["q", "t", "t"])

    def test_replay_paired_messages_skip_trades_without_fresh_quote(self):
        state = MockState("100000", True, alpaca_historical_et_date=date(2026, 5, 6))
        trade_body = {
            "trades": {
                "BAC": [
                    {"t": "2026-05-08T19:20:40.055Z", "p": 51.3, "s": 100},
                ]
            }
        }
        quote_range_body = {
            "quotes": {
                "BAC": [
                    {"t": "2026-05-08T19:20:09.000Z", "bp": 51.21, "ap": 51.22, "bs": 100, "as": 100},
                ]
            }
        }
        messages, emitted = mock_server._ws_replay_paired_trade_quote_messages(
            state,
            ["BAC"],
            trade_body,
            quote_range_body,
            max_quote_lag_seconds=2.0,
        )
        self.assertEqual(messages, [])
        self.assertEqual(emitted, set())


if __name__ == "__main__":
    unittest.main()
