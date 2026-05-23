# alpaca_mock_server

Local HTTP mock of the Alpaca REST endpoints used by **stocktrader** (`stocktrader/main.py` via alpaca-py). WebSocket market data is not implemented here.

## `.env` file

You do not need to `export` variables: place a **`.env`** file next to `mock_server.py` (or in the current working directory when you start the server). The server loads it **before** parsing CLI flags, using `setdefault` so existing shell variables still win.

Optional: `python mock_server.py --env-file /path/to/other.env`

See **`env.example`** for supported variable names (`ALPACA_MOCK_*`, `ALPACA_UPSTREAM_*`).

## Modes

**Alpaca historical replay (`--alpaca-date` or `ALPACA_MOCK_ALPACA_DATE` in `.env`)**  
The data port proxies `GET /v2/stocks/bars` and `GET /v2/stocks/quotes/latest` to Alpaca’s Data API, snapping runtime request times onto that US/Eastern calendar day. By default the replay clock starts at `09:30` New York time and advances with server runtime; override the start time with `--alpaca-time HH:MM` or `ALPACA_MOCK_ALPACA_TIME=HH:MM` (for example `--alpaca-date 2026-05-13 --alpaca-time 09:35`). Requires `ALPACA_UPSTREAM_API_KEY` and `ALPACA_UPSTREAM_SECRET_KEY` (real keys; not the dummy `test` keys used by stocktrader toward the mock). For `quotes/latest`, historical upstream data can omit a symbol or return unusable bid/ask; the mock then fills that symbol with a tight synthetic NBBO from the **last known mid** (from upstream bars/quotes once seen, else optional `--price` / default `100`), so REST clients always receive a valid quote row per requested ticker.

For **bars**, requests that send `end` and `limit` without `start` are mapped upstream using the same **limit-sized** window as live Alpaca (not a fixed small bar count), so clients like stocktrader do not need different warmup parameters under replay.

**Local synthetic (default)**  
Without `--alpaca-date`, bars and quotes are generated locally from `--price SYM=PRICE` (repeatable) and default mids (`100` per symbol).

**Trading** is always local (paper mock): clock, account, positions, orders.

### Pure replay vs optional knobs

For **“only replay the target date and answer stocktrader’s REST calls”**, you need **`--alpaca-date`** (or `ALPACA_MOCK_ALPACA_DATE`) and **upstream** `ALPACA_UPSTREAM_API_KEY` / `ALPACA_UPSTREAM_SECRET_KEY`. You do **not** need **`--price`** or **`ALPACA_MOCK_PRICE`**; mids come from Alpaca bars/quotes as requests flow.

Other flags/env vars are **not** used to fake market prices in normal replay, but they can change behavior at the edges:

| Item | Role |
|------|------|
| `--alpaca-time` / `ALPACA_MOCK_ALPACA_TIME` | Where the **replay clock** starts on that ET date (default `09:30`). Configuration, not a price override. |
| `--cash` / `ALPACA_MOCK_CASH` | Starting balance for the **local paper ledger** only (`GET /v2/account`). Unrelated to SIP/IEX replay. |
| `--market-closed` / `ALPACA_MOCK_MARKET_CLOSED` | Forces **`GET /v2/clock` → `is_open=false`**. Use only if you intentionally want the mock to report a closed session. |
| `X-Alpaca-Mock-Replay: passthrough` (header) | On bars/quotes, forwards the request to Alpaca **without** replay date remapping. Escape hatch, not default replay. |
| `GET /v2/assets` without upstream keys | Returns the built-in **`mock_asset_universe.txt`** list instead of calling Alpaca. |

## Run

For **historical replay**, the important flags are **`--alpaca-date`** (which US/Eastern calendar day to replay) and **`--alpaca-time`** (where the replay clock starts on that day; default `09:30` if omitted). Upstream Alpaca credentials must be set (`ALPACA_UPSTREAM_API_KEY` / `ALPACA_UPSTREAM_SECRET_KEY` in `.env` or the matching CLI flags).

```bash
# Replay May 13, 2026 from 9:35am ET (most common: set date + time, rest from .env)
python mock_server.py --alpaca-date 2026-05-13 --alpaca-time 09:35 --access-log

python mock_server.py --access-log
python mock_server.py --price INTC=35.5 --access-log
```

Add **`-v`** for stdlib-style HTTP request lines.

## Point stocktrader at the mock

Set (see stocktrader `config.py` / `alpaca_client.py`):

- `ALPACA_TRADING_BASE_URL=http://127.0.0.1:19901`
- `ALPACA_DATA_BASE_URL=http://127.0.0.1:19902`
- `ALPACA_API_KEY=test`
- `ALPACA_SECRET_KEY=test`
- `REPLAY_MARKET_DATA=true` when you want stocktrader’s replay timing tied to the mock clock instead of wall clock (see stocktrader docs).

## What is implemented

**Trading** (`127.0.0.1:<trading-port>`) when `EXECUTION_MODE=alpaca_paper`:

- `GET /v2/clock`, `/v2/account`, `/v2/positions`, `/v2/orders`
- `GET /v2/assets` — with upstream keys set, proxies to Alpaca; otherwise returns a built-in active US equity list from `mock_asset_universe.txt`
- `GET /v2/orders/{uuid}`, `POST /v2/orders`, `DELETE /v2/orders/{uuid}`

**Market data** (`127.0.0.1:<data-port>`):

- `GET /v2/stocks/quotes/latest`
- `GET /v2/stocks/bars` (when `ALPACA_MARKET_DATA_MODE=rest`)
- `GET /chart` — browser chart (Chart.js) polling `GET /v1/mock/chart-series`; see **Browser chart** below
- Optional passthrough: send header `X-Alpaca-Mock-Replay: passthrough` on bars or quotes requests to forward query params to Alpaca without replay date remapping (still requires upstream keys)

Diagnostics: `GET http://127.0.0.1:<data-port>/v1/mock/status` returns `data_mode` (`local_synthetic` or `alpaca_replay`), `sim_session_minutes` (meaningful in replay mode), `market_open_flag`, `quote_tick_index`, `tracked_symbol_count` / `tracked_symbols_sample` (up to 200 tickers seen on data and trading routes above), and replay fields when applicable.

## Browser chart (`GET /chart`)

The page polls `GET /v1/mock/chart-series`, which uses the same bar resolution rules as `GET /v2/stocks/bars` (replay clock and upstream proxy when configured). Bar times are labeled in **US/Eastern**.

**Query parameters**

| Parameter | Role |
|-----------|------|
| `minutes` | Lookback window length (clamped **5–1440**; default **120**) |
| `timeframe` | Bar timeframe (default **1Min**) |
| `poll` | Poll interval in **milliseconds** (clamped **5000–120000**; default 5000; values under 5s are raised to 5s) |
| `symbols` | Optional comma-separated list. If omitted, symbol **chips** are built from **tracked** tickers (see below). If set, the chip list is fixed to that list; one symbol is charted at a time (click a chip to switch). |
| `strategy` | Optional display filter for fill markers/counts, for example `strategy=steady_intraday`. Bars and mock trading behavior are unchanged. |

**Tracked symbols (chip strip without `symbols=`)**  
The mock records tickers from `GET /v2/stocks/bars` and `GET /v2/stocks/quotes/latest` (`symbols=` query), and from trading routes that expose symbols (`GET /v2/positions`, `GET /v2/orders`, order lookup, `POST /v2/orders`). The chart strip is sorted tracked symbols, **not** wired to stocktrader’s strategy JSON. For chart payloads the strip is capped at **100** symbols; if more are tracked, the JSON includes `chart_symbol_strip_total` and the UI notes that the strip is partial.

**Fills on the chart**  
Buy/sell fills from the mock executor are returned as `trade_events` and drawn as **scatter markers** at fill price (green ▲ buy, red ▼ sell). Fill tooltips show execution quantity, and sell fills are marked partial/full when the mock position state can classify them. The bar window expands to include fills outside the `minutes` lookback. Symbol chips with fill history use a **green border** and show a fill count (e.g. `RDW ·1`). JSON fields: `symbols_with_trades`, `trade_counts_by_symbol`.

When stocktrader sends `client_order_id` values with the `bk-<strategy-prefix>-...` format, the mock decodes the strategy and exposes strategy chips on `/chart`. Selecting a strategy only filters the displayed fills and chip counts; it does not affect bars, replay time, fills, positions, or any trading route.

Example: `http://127.0.0.1:19902/chart?minutes=60&timeframe=1Min&poll=5000&strategy=steady_intraday`

## Requirements

Requires Python 3.9+ (stdlib only for the server; `--alpaca-date` uses `urllib` to call Alpaca’s HTTPS data API).
