"""
Forward market-data REST calls to Alpaca's real Data API while remapping
request timestamps onto a replay calendar day (America/New_York).

Used when ``mock_server`` is started with ``--alpaca-date`` so stocktrader can
point ``ALPACA_DATA_BASE_URL`` at the mock and still receive historical SIP/IEX
bars and quotes from Alpaca for that session date.

For **start** and **end** on ``GET /v2/stocks/bars``, infer the client's implied
last US/Eastern **session** calendar day from ``end`` (exclusive midnight at the
start of the next day → through the prior calendar day). Slide ``start`` and
``end`` by ``(replay target - implied)`` days so the mock forwards the same
relative window to Alpaca as if the client had anchored on ``target`` instead
of wall-clock "today" (no ``--as-of-date`` required on stocktrader).
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time as time_of_day, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

_NY = ZoneInfo("America/New_York")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso_utc(s: str | None) -> datetime | None:
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


def snap_datetime_to_target_et_date(dt_utc: datetime, target: date) -> datetime:
    """Preserve US/Eastern wall-clock time-of-day, move calendar date to ``target``."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    et = dt_utc.astimezone(_NY)
    t = et.time()
    try:
        combined = datetime.combine(target, t, tzinfo=_NY)
    except ValueError:
        combined = datetime.combine(target, time_of_day(12, 0), tzinfo=_NY)
    return combined.astimezone(timezone.utc)


def _first(qs: dict[str, list[str]], key: str) -> str | None:
    vals = qs.get(key)
    if not vals or vals[0] is None or vals[0] == "":
        return None
    return vals[0]


def timeframe_to_seconds(timeframe: str) -> int:
    m = re.match(r"^(\d+)(Sec|Min|Hour|Day|Week|Month)$", timeframe or "", re.I)
    if not m:
        return 60
    n, unit = int(m.group(1)), m.group(2).title()
    mult = {"Sec": 1, "Min": 60, "Hour": 3600, "Day": 86400, "Week": 604800, "Month": 2592000}
    return n * mult[unit]


def _et_open_utc(d: date) -> datetime:
    return datetime.combine(d, time_of_day(9, 30), tzinfo=_NY).astimezone(timezone.utc)


def _et_close_utc(d: date) -> datetime:
    return datetime.combine(d, time_of_day(16, 0), tzinfo=_NY).astimezone(timezone.utc)


def _implied_last_session_date_et(end_utc: datetime) -> date:
    """
    Calendar day in US/Eastern treated as the last session covered by ``end``.

    If ``end`` is exactly midnight ET, treat it as an *exclusive* upper bound at
    the start of that calendar day (typical daily-bar ``select_market_universe``
    pattern), so the last session day is the prior calendar date.
    """
    et = end_utc.astimezone(_NY)
    if et.time() == time_of_day(0, 0, 0, 0):
        return et.date() - timedelta(days=1)
    return et.date()


def _replay_quote_window(snapped_end: datetime, target: date) -> tuple[datetime, datetime]:
    """
    Build a valid [start, end) window on ``target`` for historical quote fetches.

    Wall-clock snaps before the cash open can make ``snapped_end`` fall before
    09:30 ET, which previously produced start > end and empty upstream results
    for every symbol (502 on /v2/stocks/quotes/latest replay).
    """
    open_u = _et_open_utc(target)
    close_u = _et_close_utc(target)
    eff_end = snapped_end
    if eff_end <= open_u:
        eff_end = close_u
    elif eff_end > close_u:
        eff_end = close_u
    start_win = max(open_u, eff_end - timedelta(hours=9))
    if start_win >= eff_end:
        start_win = open_u
        eff_end = min(close_u, open_u + timedelta(hours=7, minutes=30))
    return start_win, eff_end


def _flatten_upstream_params(
    parsed_qs: dict[str, list[str]],
    target: date,
    replay_now_utc: datetime | None = None,
) -> dict[str, str]:
    """Build query dict for Alpaca ``GET /v2/stocks/bars`` from the mock client's query string."""
    out = {}
    for key in ("symbols", "timeframe", "feed", "limit", "adjustment", "asof", "currency", "page_token", "sort"):
        v = _first(parsed_qs, key)
        if v is not None:
            out[key] = v
    if "feed" not in out:
        out["feed"] = "iex"

    start_s = _first(parsed_qs, "start")
    end_s = _first(parsed_qs, "end")
    limit_s = _first(parsed_qs, "limit")
    tf = out.get("timeframe") or "1Min"
    step = timeframe_to_seconds(tf)
    now_utc = _utc_now()
    replay_now_utc = replay_now_utc or snap_datetime_to_target_et_date(now_utc, target)

    su_full = parse_iso_utc(start_s) if start_s else None
    eu_full = parse_iso_utc(end_s) if end_s else None

    if su_full and eu_full and eu_full > su_full:
        if timeframe_to_seconds(tf) < 86400:
            span = eu_full - su_full
            out["start"] = _iso_z(replay_now_utc - span)
            out["end"] = _iso_z(replay_now_utc)
        else:
            implied = _implied_last_session_date_et(eu_full)
            shift_days = (target - implied).days
            out["start"] = _iso_z(su_full + timedelta(days=shift_days))
            out["end"] = _iso_z(eu_full + timedelta(days=shift_days))
    else:
        if start_s:
            su = parse_iso_utc(start_s)
            if su:
                out["start"] = _iso_z(snap_datetime_to_target_et_date(su, target))
        if end_s:
            eu = parse_iso_utc(end_s)
            if eu:
                out["end"] = _iso_z(snap_datetime_to_target_et_date(eu, target))

    if "start" in out and "end" not in out:
        end_src = parse_iso_utc(end_s) if end_s else now_utc
        out["end"] = _iso_z(snap_datetime_to_target_et_date(end_src, target))

    if "end" in out and "start" not in out:
        end_dt = parse_iso_utc(out["end"]) or snap_datetime_to_target_et_date(now_utc, target)
        out["start"] = _iso_z(end_dt - timedelta(seconds=step * 100))

    if "start" not in out and "end" not in out:
        end_snap = replay_now_utc
        if limit_s and limit_s.isdigit():
            lim = max(1, min(int(limit_s), 10_000))
            start_snap = end_snap - timedelta(seconds=step * lim)
        else:
            start_snap = end_snap - timedelta(minutes=15)
        out["start"] = _iso_z(start_snap)
        out["end"] = _iso_z(end_snap)

    if "start" in out and "end" in out:
        s_dt = parse_iso_utc(out["start"])
        e_dt = parse_iso_utc(out["end"])
        if s_dt and e_dt and s_dt > e_dt:
            out["start"], out["end"] = out["end"], out["start"]

    return out


def flatten_passthrough_params(parsed_qs: dict[str, list[str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, vals in parsed_qs.items():
        if not vals:
            continue
        first = vals[0]
        if first is not None and first != "":
            out[key] = first
    if "feed" not in out:
        out["feed"] = "iex"
    return out


def upstream_get_json(
    base_url: str,
    path: str,
    params: dict[str, str],
    api_key: str,
    secret_key: str,
    timeout: float = 60.0,
) -> tuple[int, Any | None, str]:
    """GET ``path`` (e.g. ``/v2/stocks/bars``) on data host; returns (status, json_or_none, error_text)."""
    root = base_url.rstrip("/")
    q = urllib.parse.urlencode(params)
    url = f"{root}{path}?{q}"
    req = urllib.request.Request(
        url,
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            status = int(resp.getcode() or 200)
            try:
                return status, json.loads(raw) if raw else None, ""
            except json.JSONDecodeError:
                return status, None, "invalid json from upstream"
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        try:
            parsed = json.loads(body) if body else None
        except json.JSONDecodeError:
            parsed = {"message": body[:500] or str(exc)}
        return int(exc.code), parsed, body[:500]
    except urllib.error.URLError as exc:
        return 502, None, str(exc.reason or exc)


def proxy_stock_bars(
    parsed_qs: dict[str, list[str]],
    target: date,
    base_url: str,
    api_key: str,
    secret_key: str,
    replay_now_utc: datetime | None = None,
) -> tuple[int, Any]:
    params = _flatten_upstream_params(parsed_qs, target, replay_now_utc)
    if not params.get("symbols"):
        return 400, {"message": "missing symbols"}
    status, body, err = upstream_get_json(base_url, "/v2/stocks/bars", params, api_key, secret_key)
    if body is None:
        return status, {"message": err or "upstream error"}
    return status, body


def _rows_for_symbol(quotes: dict[str, Any], sym: str) -> list[Any]:
    raw = quotes.get(sym)
    if isinstance(raw, list) and raw:
        return raw
    sup = sym.upper()
    for key, val in quotes.items():
        if str(key).upper() == sup and isinstance(val, list):
            return val
    return []


def proxy_quotes_latest(
    parsed_qs: dict[str, list[str]],
    target: date,
    base_url: str,
    api_key: str,
    secret_key: str,
    replay_now_utc: datetime | None = None,
) -> tuple[int, Any]:
    symbols_raw = (_first(parsed_qs, "symbols") or "").split(",")
    symbols = [s.strip().upper() for s in symbols_raw if s.strip()]
    if not symbols:
        return 400, {"message": "missing symbols"}
    feed = _first(parsed_qs, "feed") or "iex"
    end_snap = replay_now_utc or snap_datetime_to_target_et_date(_utc_now(), target)
    start_win, eff_end = _replay_quote_window(end_snap, target)
    out: dict[str, dict[str, Any]] = {}
    # One upstream round-trip per chunk (not per symbol); otherwise universe-scale
    # clients issue tens of thousands of sequential requests and appear hung.
    chunk_size = 50
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i : i + chunk_size]
        params = {
            "symbols": ",".join(chunk),
            "start": _iso_z(start_win),
            "end": _iso_z(eff_end),
            "limit": "10000",
            "sort": "desc",
            "feed": feed,
        }
        status, body, _ = upstream_get_json(
            base_url, "/v2/stocks/quotes", params, api_key, secret_key, timeout=90.0
        )
        if status != 200 or not isinstance(body, dict):
            continue
        quotes = body.get("quotes") or {}
        for sym in chunk:
            rows = _rows_for_symbol(quotes, sym)
            if not rows:
                continue
            row0 = rows[0] if isinstance(rows[0], dict) else None
            if row0:
                out[sym] = row0
    # Partial or empty is valid: clients (e.g. select_market_universe) treat missing
    # symbols as no quote rather than failing the whole batch.
    return 200, {"quotes": out}


def replay_session_minutes(target: date, now_utc: datetime | None = None) -> float:
    """Minutes since 09:30 US/Eastern on ``target`` for a wall-snapped instant (for /v1/mock/status)."""
    now_utc = now_utc or _utc_now()
    snapped = snap_datetime_to_target_et_date(now_utc, target).astimezone(_NY)
    open_et = datetime.combine(target, time_of_day(9, 30), tzinfo=_NY)
    close_et = datetime.combine(target, time_of_day(16, 0), tzinfo=_NY)
    if snapped < open_et:
        return 0.0
    if snapped > close_et:
        return (close_et - open_et).total_seconds() / 60.0
    return max(0.0, (snapped - open_et).total_seconds() / 60.0)
