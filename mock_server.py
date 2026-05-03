#!/usr/bin/env python3
"""
Alpaca REST mock scoped to what ``stocktrader/main.py`` can hit (via alpaca-py).

``main()`` builds a market-data stream and an executor. Only those paths pull
REST against this mock:

**Trading** (``http://127.0.0.1:<trading-port>``) — used only when
``EXECUTION_MODE=alpaca_paper`` (``AlpacaPaperExecutor`` in ``execution.py``):

- ``GET /v2/clock`` — regular session gate
- ``GET /v2/account`` — startup cash sync
- ``GET /v2/positions`` — startup reconcile
- ``GET /v2/orders`` — startup cancel of open orders for watched symbols
- ``GET /v2/orders/{uuid}`` — fill polling after submit
- ``POST /v2/orders`` — buy / sell
- ``DELETE /v2/orders/{uuid}`` — cancel after timeout

**Market data** (``http://127.0.0.1:<data-port>``):

- ``GET /v2/stocks/quotes/latest`` — ``AlpacaRestPollingStream`` each poll, and
  ``AlpacaPaperExecutor._fresh_entry_price`` before a buy when stream mode.
  Each call returns a **new** quote row (monotonic ``t``, 3–10 bps spread). In the
  first ~10% of simulated session, quotes add sustained upward drift plus
  non-negative micro-jitter so opening_impulse-style quote windows see consecutive
  buying pressure; later session uses mild asymmetric noise only.
- ``GET /v2/stocks/bars`` — ``AlpacaRestPollingStream`` only when
  ``ALPACA_MARKET_DATA_MODE=rest`` (not used on the default stream path)

Stream mode still uses Alpaca WebSockets for live bars/quotes; this mock does
not implement WS.

**Intraday simulation** (optional): pass ``--scenario`` to a JSON file (see
``samples/intc_may01_chart_scenario.json``). Two clocks:

- **minute** (default when a scenario is loaded): a **simulated session time**
  counter (in *session minutes*, float) advances by ``--minutes-per-bars-tick``
  (default 1) on each ``GET /v2/stocks/bars``, never less than one requested bar
  span. For a 1 Hz REST loop with sub-minute bars, use timeframe ``1Sec`` and
  ``--seconds-per-bars-tick 1`` so
  each poll advances one *session second* and OHLC spans one second on the
  curve. Optional JSON ``session_minutes`` (default 390) is the length of one
  synthetic RTH day on the curve before wrap.

- **wall**: ``u`` from wall time modulo ``--sim-cycle-seconds`` (legacy).

With **minute** clock + ``--scenario``, bar/quote timestamps and ``GET /v2/clock``
``timestamp`` follow a synthetic US/Eastern RTH day (09:30 + simulated session
minute) so clients with ``regular_market_only`` accept the feed off-hours.
Override the calendar day with ``--session-date YYYY-MM-DD`` (ET).

Run::

    python mock_server.py --scenario samples/intc_may01_chart_scenario.json
    python mock_server.py --scenario samples/intc_may01_chart_scenario.json --sim-clock wall --sim-cycle-seconds 3600
    python mock_server.py --scenario samples/intc_may01_chart_scenario.json --access-log
    python mock_server.py --scenario samples/intc_may01_chart_scenario.json --seconds-per-bars-tick 1

Point stocktrader at the mock (see ``config.py`` / ``alpaca_client.py``)::

    ALPACA_TRADING_BASE_URL=http://127.0.0.1:19901
    ALPACA_DATA_BASE_URL=http://127.0.0.1:19902
    ALPACA_API_KEY=test
    ALPACA_SECRET_KEY=test
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import threading
import time as time_module
import uuid
from collections import defaultdict
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

_NY = ZoneInfo("America/New_York")

log = logging.getLogger("alpaca_mock")

# Paths reachable from stocktrader main.py (see module docstring).
_MAIN_TRADING_GET_PATHS = frozenset({"/v2/clock", "/v2/account", "/v2/positions", "/v2/orders"})
_MAIN_TRADING_ORDER_UUID = re.compile(r"^/v2/orders/([0-9a-f-]{36})$", re.I)
_MAIN_DATA_PATHS = frozenset({"/v2/stocks/bars", "/v2/stocks/quotes/latest"})
_TERMINAL_ORDER_STATUSES = frozenset({"filled", "canceled", "expired", "rejected", "done_for_day"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _decimal_value(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _decimal_str(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _order_matches_status_filter(order_status: str, status_filter: str) -> bool:
    status = order_status.lower()
    requested = status_filter.lower()
    if requested == "all":
        return True
    if requested == "closed":
        return status in _TERMINAL_ORDER_STATUSES
    if requested == "open":
        return status not in _TERMINAL_ORDER_STATUSES
    return status == requested


def _default_et_session_date() -> date:
    """Most recent weekday (Mon–Fri) in America/New_York for synthetic RTH."""
    now_et = datetime.now(_NY)
    d = now_et.date()
    wd = d.weekday()
    if wd >= 5:
        d = d - timedelta(days=wd - 4)
    return d


def _et_open_datetime(d: date) -> datetime:
    return datetime.combine(d, time_of_day(9, 30), tzinfo=_NY)


def _utc_from_session_fminute(et_day: date, session_minute: float, session_len: float) -> datetime:
    """Map synthetic session minute (may be negative; wraps) to UTC instant."""
    m = float(session_minute) % float(session_len)
    return (_et_open_datetime(et_day) + timedelta(minutes=m)).astimezone(timezone.utc)


def _utc_from_session_minute_no_wrap(et_day: date, session_minute: float) -> datetime:
    return (_et_open_datetime(et_day) + timedelta(minutes=float(session_minute))).astimezone(timezone.utc)


def _timeframe_seconds(timeframe: str) -> int | None:
    """Bar step in seconds (Alpaca-style: 1Min, 5Min, 1Sec, …). Unknown → 60."""
    m = re.match(r"^(\d+)(Sec|Min|Hour|Day|Week|Month)$", timeframe or "", re.I)
    if not m:
        return 60
    n, unit = int(m.group(1)), m.group(2).title()
    mult = {"Sec": 1, "Min": 60, "Hour": 3600, "Day": 86400, "Week": 604800, "Month": 2592000}
    return n * mult[unit]


class MockState:
    def __init__(
        self,
        starting_cash: float,
        market_open: bool,
        *,
        sim_anchor_epoch: float | None = None,
        sim_cycle_seconds: float = 3600.0,
        scenario_points: dict[str, list[tuple[float, float]]] | None = None,
        sim_clock_mode: str = "wall",
        session_minutes_total: int = 390,
        minutes_per_bars_tick: float = 1.0,
        synthetic_rth_et_date: date | None = None,
    ) -> None:
        self.starting_cash = starting_cash
        self.cash = _decimal_value(starting_cash)
        self.market_open = market_open
        self.orders: dict[str, dict[str, Any]] = {}
        self.positions: dict[str, dict[str, Decimal]] = {}
        self.account_lock = threading.Lock()
        self.mock_prices: dict[str, float] = defaultdict(lambda: 100.0)
        self.sim_anchor_epoch = float(
            sim_anchor_epoch if sim_anchor_epoch is not None else time_module.time()
        )
        self.sim_cycle_seconds = max(60.0, float(sim_cycle_seconds))
        self.scenario_points: dict[str, list[tuple[float, float]]] = scenario_points or {}
        self.sim_clock_mode = sim_clock_mode
        self.session_minutes_total = max(1, int(session_minutes_total))
        self.minutes_per_bars_tick = max(1e-15, float(minutes_per_bars_tick))
        self.sim_session_minutes: float = 0.0
        self.sim_lock = threading.Lock()
        self.synthetic_rth_et_date = synthetic_rth_et_date
        self.quote_tick_index: int = 0
        self.quote_lock = threading.Lock()
        self.quote_last_emit_utc: datetime | None = None

    def next_quote(self, symbol: str) -> tuple[str, float, float]:
        """Next synthetic quote: monotonic UTC ``t``, mid, half-spread width (bps-sized)."""
        sym = symbol.upper()
        with self.sim_lock:
            sm = float(self.sim_session_minutes)

        with self.quote_lock:
            self.quote_tick_index += 1
            tick = self.quote_tick_index
            if self.synthetic_rth_et_date is not None:
                candidate = _utc_from_session_minute_no_wrap(
                    self.synthetic_rth_et_date,
                    sm,
                ) + timedelta(seconds=tick)
            else:
                candidate = _utc_now()
            if self.quote_last_emit_utc is not None:
                candidate = max(candidate, self.quote_last_emit_utc + timedelta(microseconds=1))
            dt = candidate
            self.quote_last_emit_utc = dt

        base_price = self.mid_price(sym, dt)
        session_len = float(self.session_minutes_total)
        session_phase = (sm % session_len) / session_len if session_len > 0 else 0.0
        if session_phase < 0.1:
            drift = 0.0015
            noise = (tick % 2) * 0.0002
        else:
            drift = 0.0
            noise = ((tick % 3) - 1) * 0.0005
        price = base_price * (1.0 + drift + noise)
        spread_bps = 0.0003 + (tick % 8) * 0.0001
        spread = max(0.01, price * spread_bps)
        return _iso(dt), float(price), float(spread)

    def virtual_u(self, at: datetime) -> float:
        t = _as_utc_timestamp(at)
        elapsed = t - self.sim_anchor_epoch
        u = (elapsed % self.sim_cycle_seconds) / self.sim_cycle_seconds
        return min(1.0, max(0.0, u))

    def mid_price(self, symbol: str, at: datetime) -> float:
        sym = symbol.upper()
        pts = self.scenario_points.get(sym)
        if pts:
            if self.sim_clock_mode == "minute":
                with self.sim_lock:
                    sm = float(self.sim_session_minutes)
                return _mid_from_session_minute(pts, sm, float(self.session_minutes_total))
            return _interp_points(pts, self.virtual_u(at))
        return float(self.mock_prices[sym])

    def account_payload(self) -> dict[str, Any]:
        with self.account_lock:
            cash = self.cash
            market_value = Decimal("0")
            for sym, pos in self.positions.items():
                qty = pos["qty"]
                px = _decimal_value(self.mid_price(sym, _utc_now()))
                market_value += qty * px
            equity = cash + market_value
            buying_power = cash
        c = _money(cash)
        e = _money(equity)
        bp = _money(buying_power)
        return {
            "id": str(uuid.uuid4()),
            "account_number": "MOCK-0001",
            "status": "ACTIVE",
            "currency": "USD",
            "cash": c,
            "buying_power": bp,
            "portfolio_value": e,
            "equity": e,
            "pattern_day_trader": False,
            "trading_blocked": False,
            "transfers_blocked": False,
            "account_blocked": False,
            "multiplier": "1",
            "shorting_enabled": True,
        }

    def position_payloads(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with self.account_lock:
            snapshot = [(sym, pos["qty"], pos["avg_entry_price"]) for sym, pos in self.positions.items()]
        for sym, qty, avg_entry in snapshot:
            if qty == 0:
                continue
            current = _decimal_value(self.mid_price(sym, _utc_now()))
            market_value = qty * current
            cost_basis = qty * avg_entry
            unrealized = market_value - cost_basis
            out.append(
                {
                    "asset_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, sym)),
                    "symbol": sym,
                    "exchange": "NASDAQ",
                    "asset_class": "us_equity",
                    "asset_marginable": True,
                    "qty": _decimal_str(qty),
                    "avg_entry_price": _decimal_str(avg_entry),
                    "side": "long" if qty >= 0 else "short",
                    "market_value": _money(market_value),
                    "cost_basis": _money(cost_basis),
                    "unrealized_pl": _money(unrealized),
                    "unrealized_plpc": _decimal_str(unrealized / cost_basis) if cost_basis else "0",
                    "current_price": _decimal_str(current),
                    "lastday_price": _decimal_str(current),
                    "change_today": "0",
                }
            )
        return out

    def apply_fill(self, order: dict[str, Any]) -> None:
        sym = str(order.get("symbol") or "").upper()
        if not sym:
            return
        qty = _decimal_value(order.get("filled_qty") or order.get("qty"), "0")
        px = _decimal_value(order.get("filled_avg_price"), "0")
        if qty <= 0 or px <= 0:
            return
        side = str(order.get("side") or "buy").lower()
        signed_qty = qty if side == "buy" else -qty
        with self.account_lock:
            self.cash -= signed_qty * px
            pos = self.positions.get(sym)
            if pos is None:
                self.positions[sym] = {"qty": signed_qty, "avg_entry_price": px}
                return
            old_qty = pos["qty"]
            old_avg = pos["avg_entry_price"]
            new_qty = old_qty + signed_qty
            if new_qty == 0:
                self.positions.pop(sym, None)
                return
            if old_qty == 0 or (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
                pos["avg_entry_price"] = ((old_qty * old_avg) + (signed_qty * px)) / new_qty
            pos["qty"] = new_qty


def _as_utc_timestamp(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


def _mid_from_session_minute(
    points: list[tuple[float, float]], session_minute: float, session_len: float
) -> float:
    """Map simulated RTH minute index to scenario u in [0,1)."""
    if session_len <= 0:
        return _interp_points(points, 0.0)
    m = session_minute % session_len
    u = m / session_len
    return _interp_points(points, u)


def _interp_points(points: list[tuple[float, float]], u: float) -> float:
    pts = sorted(points, key=lambda x: x[0])
    if not pts:
        return 100.0
    u = u % 1.0
    if u <= pts[0][0]:
        return float(pts[0][1])
    for i in range(len(pts) - 1):
        u0, p0 = pts[i]
        u1, p1 = pts[i + 1]
        if u0 <= u <= u1:
            if abs(u1 - u0) < 1e-12:
                return float(p0)
            return float(p0 + (p1 - p0) * (u - u0) / (u1 - u0))
    return float(pts[-1][1])


def _load_scenario(path: str) -> tuple[dict[str, list[tuple[float, float]]], float | None, int]:
    """Returns (symbol -> [(u, price), ...], cycle_seconds or None, session_minutes)."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    cycle = raw.get("cycle_seconds")
    session_minutes = int(raw.get("session_minutes", 390))
    symbols_block = raw.get("symbols") or {}
    out: dict[str, list[tuple[float, float]]] = {}
    for sym, spec in symbols_block.items():
        sym_u = sym.strip().upper()
        pairs = spec.get("points") or []
        parsed: list[tuple[float, float]] = []
        for row in pairs:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                parsed.append((float(row[0]), float(row[1])))
        if parsed:
            out[sym_u] = parsed
    return out, float(cycle) if cycle is not None else None, session_minutes


def _synthetic_bars(
    symbols: list[str],
    timeframe: str,
    start: datetime | None,
    end: datetime | None,
    limit: int | None,
    state: MockState,
) -> dict[str, list[dict[str, Any]]]:
    step = _timeframe_seconds(timeframe) or 60
    end_dt = end or _utc_now()
    max_points = min(limit or 500, 500) if limit else 1000
    if start:
        start_dt = start
    elif limit:
        n = max(1, min(limit, 500))
        start_dt = end_dt - timedelta(seconds=step * (n - 1))
    else:
        start_dt = end_dt - timedelta(seconds=step * 30)

    if state.sim_clock_mode == "minute":
        return _synthetic_bars_minute_clock(
            symbols, timeframe, start_dt, end_dt, limit, max_points, step, state
        )

    out: dict[str, list[dict[str, Any]]] = {s: [] for s in symbols}
    for sym in symbols:
        t = start_dt
        i = 0
        while t <= end_dt and i < max_points:
            t_next = t + timedelta(seconds=step)
            o = state.mid_price(sym, t)
            c = state.mid_price(sym, min(t_next, end_dt))
            hi = max(o, c) * 1.001
            lo = min(o, c) * 0.999
            vol = 800.0 + 40.0 * abs(c - o) * 1000.0
            if i > 0 and abs(c - o) > 0.03:
                vol += 5000.0
            out[sym].append(
                {
                    "t": _iso(t),
                    "o": o,
                    "h": hi,
                    "l": lo,
                    "c": c,
                    "v": vol,
                    "n": 50.0,
                    "vw": (o + c) / 2,
                }
            )
            t = t_next
            i += 1
        if limit and len(out[sym]) > limit:
            out[sym] = out[sym][-limit:]
    return out


def _synthetic_bars_minute_clock(
    symbols: list[str],
    _timeframe: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int | None,
    max_points: int,
    step: int,
    state: MockState,
) -> dict[str, list[dict[str, Any]]]:
    """Session-clock bars: each row spans ``step`` seconds on the session timeline (step/60 session minutes)."""
    session_len = float(state.session_minutes_total)
    span_session_minutes = max(float(step) / 60.0, 1e-15)
    times: list[datetime] = []
    t = start_dt
    i = 0
    while t <= end_dt and i < max_points:
        times.append(t)
        t += timedelta(seconds=step)
        i += 1
    if limit and len(times) > limit:
        times = times[-limit:]

    out: dict[str, list[dict[str, Any]]] = {s: [] for s in symbols}
    with state.sim_lock:
        cur = float(state.sim_session_minutes)
        n = len(times)
        for sym in symbols:
            pts = state.scenario_points.get(sym)
            for j, t_open in enumerate(times):
                start_m = cur - (n - 1 - j) * span_session_minutes
                if start_m < 0 or start_m >= session_len:
                    continue
                if pts:
                    o = _mid_from_session_minute(pts, start_m, session_len)
                    c = _mid_from_session_minute(pts, start_m + span_session_minutes, session_len)
                else:
                    px = float(state.mock_prices[sym])
                    o = c = px
                hi = max(o, c) * 1.001
                lo = min(o, c) * 0.999
                vol = 800.0 + 40.0 * abs(c - o) * 1000.0
                if abs(c - o) > 0.03:
                    vol += 5000.0
                if state.synthetic_rth_et_date is not None:
                    t_bar = _utc_from_session_minute_no_wrap(state.synthetic_rth_et_date, float(start_m))
                    t_iso = _iso(t_bar)
                else:
                    t_iso = _iso(t_open)
                out[sym].append(
                    {
                        "t": t_iso,
                        "o": o,
                        "h": hi,
                        "l": lo,
                        "c": c,
                        "v": vol,
                        "n": 50.0,
                        "vw": (o + c) / 2,
                    }
                )
        state.sim_session_minutes += max(state.minutes_per_bars_tick, span_session_minutes)
    return out


def _order_payload(
    order_id: str,
    body: dict[str, Any],
    status: str,
    filled_qty: str | None,
    filled_avg: str | None,
    now: datetime,
) -> dict[str, Any]:
    sym = (body.get("symbol") or "").upper()
    side = (body.get("side") or "buy").lower()
    qty = str(body.get("qty") or body.get("quantity") or "1")
    client_oid = body.get("client_order_id") or f"mock-{order_id[:8]}"
    ts = _iso(now)
    asset_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, sym or "UNKNOWN"))
    payload: dict[str, Any] = {
        "id": order_id,
        "client_order_id": client_oid,
        "created_at": ts,
        "updated_at": ts,
        "submitted_at": ts,
        "asset_id": asset_id,
        "symbol": sym,
        "asset_class": "us_equity",
        "qty": qty,
        "filled_qty": filled_qty if filled_qty is not None else "0",
        "filled_avg_price": filled_avg,
        "type": "market",
        "side": side,
        "time_in_force": (body.get("time_in_force") or "day").lower(),
        "status": status,
        "order_class": "simple",
        "extended_hours": bool(body.get("extended_hours", False)),
    }
    if status == "filled":
        payload["filled_at"] = ts
        payload["filled_qty"] = filled_qty or qty
        payload["filled_avg_price"] = filled_avg or "100.0"
    return payload


def _setup_access_logging() -> None:
    if log.handlers:
        return
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [alpaca_mock] %(message)s", datefmt="%H:%M:%S"))
    log.addHandler(h)
    log.setLevel(logging.INFO)


def _access_line(server: str, command: str, raw_path: str, code: int, detail: str) -> None:
    parsed = urlparse(raw_path)
    q = f"?{parsed.query}" if parsed.query else ""
    log.info("%s | %s %s%s -> %d | %s", server, command, parsed.path, q, code, detail)


def _summary_trading(path: str, code: int, body: Any, *, empty: bool = False) -> str:
    if empty or code == 204:
        return "no body"
    if code >= 400:
        if isinstance(body, dict):
            return (body.get("message") or str(body))[:220]
        return str(body)[:220]
    if path == "/v2/clock" and isinstance(body, dict):
        return f"is_open={body.get('is_open')} timestamp={body.get('timestamp')}"
    if path == "/v2/account" and isinstance(body, dict):
        return f"cash={body.get('cash')} buying_power={body.get('buying_power')} equity={body.get('equity')}"
    if path == "/v2/positions" and isinstance(body, list):
        return f"positions={len(body)}"
    if path == "/v2/orders" and isinstance(body, list):
        return f"open_orders={len(body)}"
    if path.startswith("/v2/orders/") and isinstance(body, dict):
        oid = str(body.get("id", ""))[:8]
        return (
            f"order id={oid}… symbol={body.get('symbol')} status={body.get('status')} "
            f"filled_qty={body.get('filled_qty')} filled_avg={body.get('filled_avg_price')}"
        )
    if path == "/v2/orders" and isinstance(body, dict) and body.get("id"):
        return (
            f"order_accept symbol={body.get('symbol')} side={body.get('side')} status={body.get('status')} "
            f"qty={body.get('qty')} filled_qty={body.get('filled_qty')} filled_avg={body.get('filled_avg_price')}"
        )
    if isinstance(body, dict):
        return f"keys={list(body.keys())[:8]}"
    if isinstance(body, list):
        return f"list n={len(body)}"
    return str(type(body).__name__)


def _summary_data(path: str, code: int, body: Any, state: MockState) -> str:
    if code >= 400:
        if isinstance(body, dict):
            return (body.get("message") or str(body))[:220]
        return str(body)[:220]
    if path == "/v1/mock/status" and isinstance(body, dict):
        return (
            f"sim_session_minutes={body.get('sim_session_minutes')} "
            f"synthetic_ts={body.get('synthetic_timestamp_utc')} clock={body.get('sim_clock_mode')}"
        )
    if path == "/v2/stocks/bars" and isinstance(body, dict):
        bars = body.get("bars") or {}
        parts: list[str] = []
        for sym in sorted(bars.keys()):
            rows = bars[sym]
            if not rows:
                parts.append(f"{sym}=0bars")
                continue
            last = rows[-1]
            parts.append(
                f"{sym} n={len(rows)} last_t={last.get('t')} o={last.get('o')} c={last.get('c')} v={last.get('v'):.0f}"
                if last.get("v") is not None
                else f"{sym} n={len(rows)} last_t={last.get('t')} c={last.get('c')}"
            )
        sm = f" sim_session_minutes_after_tick={state.sim_session_minutes:.4f}"
        return "; ".join(parts) + sm
    if path == "/v2/stocks/quotes/latest" and isinstance(body, dict):
        q = body.get("quotes") or {}
        parts = []
        for sym in sorted(q.keys()):
            row = q[sym]
            mid = (float(row.get("bp", 0)) + float(row.get("ap", 0))) / 2.0
            parts.append(f"{sym} t={row.get('t')} mid≈{mid:.4f} bp={row.get('bp')} ap={row.get('ap')}")
        return "; ".join(parts)
    if isinstance(body, dict):
        return f"keys={list(body.keys())[:10]}"
    return str(type(body).__name__)


class TradingHandler(BaseHTTPRequestHandler):
    state: MockState
    log_verbose: bool = False
    access_log: bool = False

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.log_verbose:
            super().log_message(fmt, *args)

    def _send(self, code: int, body: Any | None, content_type: str = "application/json") -> None:
        data = b""
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if data:
            self.wfile.write(data)
        if self.access_log:
            _access_line("trading", self.command, self.path, code, _summary_trading(urlparse(self.path).path, code, body))

    def _send_empty(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()
        if self.access_log:
            _access_line("trading", self.command, self.path, code, _summary_trading(urlparse(self.path).path, code, None, empty=True))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path not in _MAIN_TRADING_GET_PATHS and not _MAIN_TRADING_ORDER_UUID.match(path):
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {path}; this mock only implements main.py trading GET routes "
                        f"(clock, account, positions, orders)"
                    )
                },
            )
            return

        if path == "/v2/clock":
            if self.state.synthetic_rth_et_date is not None:
                now = _utc_from_session_fminute(
                    self.state.synthetic_rth_et_date,
                    self.state.sim_session_minutes,
                    float(self.state.session_minutes_total),
                )
            else:
                now = _utc_now()
            self._send(
                200,
                {
                    "timestamp": _iso(now),
                    "is_open": self.state.market_open,
                    "next_open": _iso(now - timedelta(hours=1)),
                    "next_close": _iso(now + timedelta(hours=6)),
                },
            )
            return

        if path == "/v2/account":
            self._send(200, self.state.account_payload())
            return

        if path == "/v2/positions":
            self._send(200, self.state.position_payloads())
            return

        if path == "/v2/orders":
            status_filter = ((qs.get("status") or ["open"])[0] or "open").lower()
            out = []
            for o in self.state.orders.values():
                order_status = str(o.get("status") or "").lower()
                if _order_matches_status_filter(order_status, status_filter):
                    out.append(o)
            self._send(200, out)
            return

        m = _MAIN_TRADING_ORDER_UUID.match(path)
        if m:
            oid = m.group(1).lower()
            o = self.state.orders.get(oid)
            if not o:
                self._send(404, {"code": 40410000, "message": "order not found"})
                return
            self._send(200, o)
            return

        raise AssertionError(f"unreachable routing for {path}")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/v2/orders":
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {parsed.path}; this mock only implements "
                        "main.py POST /v2/orders (market orders)"
                    )
                },
            )
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send(400, {"code": 40010001, "message": "invalid json"})
            return

        now = _utc_now()
        oid = str(uuid.uuid4()).lower()
        sym = (body.get("symbol") or "").upper()
        px = f"{self.state.mid_price(sym, now):.4f}"

        if self.state.market_open and (body.get("type") or "market").lower() == "market":
            st = "filled"
            o = _order_payload(oid, body, st, None, px, now)
            self.state.apply_fill(o)
        else:
            st = "accepted"
            o = _order_payload(oid, body, st, "0", None, now)

        self.state.orders[oid] = o
        self._send(200, o)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        m = _MAIN_TRADING_ORDER_UUID.match(parsed.path)
        if not m:
            self._send(
                404,
                {"message": f"unknown path {parsed.path}; main.py only DELETE /v2/orders/{{uuid}}"},
            )
            return
        oid = m.group(1).lower()
        o = self.state.orders.get(oid)
        if not o:
            self._send(404, {"code": 40410000, "message": "order not found"})
            return
        now = _utc_now()
        o["status"] = "canceled"
        o["canceled_at"] = _iso(now)
        o["updated_at"] = _iso(now)
        self._send_empty(204)


class DataHandler(BaseHTTPRequestHandler):
    state: MockState
    log_verbose: bool = False
    access_log: bool = False

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.log_verbose:
            super().log_message(fmt, *args)

    def _send(self, code: int, body: Any) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        if self.access_log:
            _access_line(
                "data",
                self.command,
                self.path,
                code,
                _summary_data(urlparse(self.path).path, code, body, self.state),
            )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/v1/mock/status":
            st = self.state
            syn: str | None = None
            if st.synthetic_rth_et_date is not None:
                syn = _iso(
                    _utc_from_session_fminute(
                        st.synthetic_rth_et_date,
                        st.sim_session_minutes,
                        float(st.session_minutes_total),
                    )
                )
            self._send(
                200,
                {
                    "sim_session_minutes": st.sim_session_minutes,
                    "sim_clock_mode": st.sim_clock_mode,
                    "session_minutes_total": st.session_minutes_total,
                    "synthetic_rth_et_date": str(st.synthetic_rth_et_date)
                    if st.synthetic_rth_et_date
                    else None,
                    "synthetic_timestamp_utc": syn,
                    "market_open_flag": st.market_open,
                    "quote_tick_index": st.quote_tick_index,
                },
            )
            return

        if path not in _MAIN_DATA_PATHS:
            self._send(
                404,
                {
                    "message": (
                        f"unknown path {path}; this mock only implements main.py data GET "
                        "/v2/stocks/bars (rest mode) and /v2/stocks/quotes/latest"
                    )
                },
            )
            return

        if path == "/v2/stocks/bars":
            symbols = (qs.get("symbols") or [""])[0].split(",")
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            timeframe = (qs.get("timeframe") or ["1Min"])[0]
            start = _parse_iso((qs.get("start") or [None])[0])
            end = _parse_iso((qs.get("end") or [None])[0])
            limit_raw = (qs.get("limit") or [None])[0]
            limit = int(limit_raw) if limit_raw and limit_raw.isdigit() else None
            bars = _synthetic_bars(symbols, timeframe, start, end, limit, self.state)
            self._send(200, {"bars": bars, "next_page_token": None})
            return

        if path == "/v2/stocks/quotes/latest":
            symbols = (qs.get("symbols") or [""])[0].split(",")
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            quotes: dict[str, dict[str, Any]] = {}
            for sym in symbols:
                t, mid, spread = self.state.next_quote(sym)
                quotes[sym] = {
                    "t": t,
                    "bp": mid - spread / 2,
                    "ap": mid + spread / 2,
                    "bs": 100.0,
                    "as": 100.0,
                }
            self._send(200, {"quotes": quotes})
            return

        raise AssertionError(f"unreachable routing for {path}")


def _run(host: str, port: int, handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server


def main() -> None:
    p = argparse.ArgumentParser(
        description="Alpaca REST mock: routes used by stocktrader main.py (alpaca-py layout).",
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--trading-port", type=int, default=19901)
    p.add_argument("--data-port", type=int, default=19902)
    p.add_argument("--cash", type=float, default=25_000.0, help="Account cash / buying_power in mock /account")
    p.add_argument("--market-closed", action="store_true", help="Report clock.is_open=false")
    p.add_argument("--price", action="append", default=[], metavar="SYM=PRICE", help="Mock mid price (repeatable)")
    p.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="JSON file with intraday curves (see samples/intc_may01_chart_scenario.json)",
    )
    p.add_argument(
        "--sim-cycle-seconds",
        type=float,
        default=None,
        help="Seconds for one full u=[0,1] replay (overrides scenario JSON if set)",
    )
    p.add_argument(
        "--sim-anchor-epoch",
        type=float,
        default=None,
        help="Unix epoch anchor for simulation clock (default: server start)",
    )
    p.add_argument(
        "--sim-clock",
        choices=("minute", "wall", "auto"),
        default="auto",
        help="minute=advance one session minute per bars GET (default with --scenario); wall=time-based u",
    )
    p.add_argument(
        "--minutes-per-bars-tick",
        type=float,
        default=1.0,
        help="Simulated session minutes to advance after each GET /v2/stocks/bars (minute clock only; fractional ok)",
    )
    p.add_argument(
        "--seconds-per-bars-tick",
        type=float,
        default=None,
        metavar="SEC",
        help="Sets --minutes-per-bars-tick to SEC/60 (e.g. 1 with 1Sec bars = one session-second per poll)",
    )
    p.add_argument(
        "--session-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="America/New_York calendar date for synthetic RTH timestamps (minute clock + --scenario only; default: last weekday ET)",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Stdlib HTTP request line logging (Apache-style)")
    p.add_argument(
        "--access-log",
        action="store_true",
        help="Log each HTTP response: trading|data, method, path, status, summary (clock, bars, quotes, orders, …)",
    )
    args = p.parse_args()
    if args.seconds_per_bars_tick is not None:
        if args.seconds_per_bars_tick < 0:
            p.error("--seconds-per-bars-tick must be >= 0")
        args.minutes_per_bars_tick = args.seconds_per_bars_tick / 60.0

    scenario_points: dict[str, list[tuple[float, float]]] | None = None
    file_cycle: float | None = None
    session_minutes_total = 390
    if args.scenario:
        scenario_points, file_cycle, session_minutes_total = _load_scenario(args.scenario)
    cycle = args.sim_cycle_seconds if args.sim_cycle_seconds is not None else (file_cycle or 3600.0)

    if args.sim_clock == "auto":
        sim_clock_mode = "minute" if scenario_points else "wall"
    else:
        sim_clock_mode = args.sim_clock

    syn_et_date: date | None = None
    if scenario_points and sim_clock_mode == "minute":
        if args.session_date:
            try:
                syn_et_date = date.fromisoformat(args.session_date.strip())
            except ValueError:
                p.error("--session-date must be YYYY-MM-DD")
        else:
            syn_et_date = _default_et_session_date()

    state = MockState(
        args.cash,
        market_open=not args.market_closed,
        sim_anchor_epoch=args.sim_anchor_epoch,
        sim_cycle_seconds=cycle,
        scenario_points=scenario_points,
        sim_clock_mode=sim_clock_mode,
        session_minutes_total=session_minutes_total,
        minutes_per_bars_tick=args.minutes_per_bars_tick,
        synthetic_rth_et_date=syn_et_date,
    )
    for item in args.price:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        state.mock_prices[k.strip().upper()] = float(v)

    access_log = args.access_log
    if access_log:
        _setup_access_logging()

    TradingHandler.state = state
    TradingHandler.log_verbose = args.verbose
    TradingHandler.access_log = access_log
    DataHandler.state = state
    DataHandler.log_verbose = args.verbose
    DataHandler.access_log = access_log

    t_srv: ThreadingHTTPServer | None = None
    d_srv: ThreadingHTTPServer | None = None
    try:
        t_srv = _run(args.host, args.trading_port, TradingHandler)
        d_srv = _run(args.host, args.data_port, DataHandler)
    except Exception:
        if t_srv is not None:
            t_srv.shutdown()
            t_srv.server_close()
        raise

    sim_line = ""
    if scenario_points:
        sim_line = (
            f"  Intraday scenario: {args.scenario} | symbols={','.join(sorted(scenario_points))} | "
            f"clock={sim_clock_mode}"
            + (f" | session_minutes={session_minutes_total}" if sim_clock_mode == "minute" else f" | cycle={cycle:.0f}s")
            + (f" | synthetic RTH ET date={syn_et_date}" if syn_et_date else "")
            + "\n"
        )
    print(
        f"Alpaca mock (main.py REST surface) listening:\n"
        f"  Trading: http://{args.host}:{args.trading_port}\n"
        f"           GET  /v2/clock /v2/account /v2/positions /v2/orders /v2/orders/{{id}}\n"
        f"           POST /v2/orders   DELETE /v2/orders/{{id}}\n"
        f"  Data:    http://{args.host}:{args.data_port}\n"
        f"           GET /v2/stocks/quotes/latest   GET /v2/stocks/bars (rest mode only)\n"
        f"           GET /v1/mock/status (simulation snapshot; not Alpaca)\n"
        f"{sim_line}"
        f"Set ALPACA_TRADING_BASE_URL and ALPACA_DATA_BASE_URL (EXECUTION_MODE / ALPACA_MARKET_DATA_MODE as needed).\n"
        f"  Response logging: {'on (--access-log)' if access_log else 'off (add --access-log to log each reply)'}",
        flush=True,
    )
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down.", flush=True)
    finally:
        if t_srv is not None:
            t_srv.shutdown()
            t_srv.server_close()
        if d_srv is not None:
            d_srv.shutdown()
            d_srv.server_close()


if __name__ == "__main__":
    main()
