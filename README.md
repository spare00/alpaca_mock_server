# alpaca_mock_server

Local HTTP mock of the Alpaca REST endpoints used by **stocktrader** (`stocktrader/main.py` via alpaca-py). WebSocket market data is not implemented here.

## `.env` file

You do not need to `export` variables: place a **`.env`** file next to `mock_server.py` (or in the current working directory when you start the server). The server loads it **before** parsing CLI flags, using `setdefault` so existing shell variables still win.

Optional: `python mock_server.py --env-file /path/to/other.env`

See **`env.example`** for supported variable names (`ALPACA_MOCK_*`, `ALPACA_UPSTREAM_*`).

## Modes

**Alpaca historical replay (`--alpaca-date` or `ALPACA_MOCK_ALPACA_DATE` in `.env`)**  
The data port proxies `GET /v2/stocks/bars` and `GET /v2/stocks/quotes/latest` to Alpaca’s Data API, snapping runtime request times onto that US/Eastern calendar day. By default the replay clock is pinned to `09:30` New York time; override it with `--alpaca-time HH:MM` or `ALPACA_MOCK_ALPACA_TIME=HH:MM` (for example `--alpaca-date 2024-05-13 --alpaca-time 09:35`). Requires `ALPACA_UPSTREAM_API_KEY` and `ALPACA_UPSTREAM_SECRET_KEY` (real keys; not the dummy `test` keys used by stocktrader toward the mock).

**Local synthetic (default)**  
Without `--alpaca-date`, bars and quotes are generated locally from `--price SYM=PRICE` (repeatable) and default mids (`100` per symbol).

**Trading** is always local (paper mock): clock, account, positions, orders.

## Run

```bash
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
- `GET /v2/orders/{uuid}`, `POST /v2/orders`, `DELETE /v2/orders/{uuid}`

**Market data** (`127.0.0.1:<data-port>`):

- `GET /v2/stocks/quotes/latest`
- `GET /v2/stocks/bars` (when `ALPACA_MARKET_DATA_MODE=rest`)

Diagnostics: `GET http://127.0.0.1:<data-port>/v1/mock/status` returns `data_mode` (`local_synthetic` or `alpaca_replay`), `sim_session_minutes` (meaningful in replay mode), `market_open_flag`, `quote_tick_index`, and replay fields when applicable.

Requires Python 3.9+ (stdlib only for the server; `--alpaca-date` uses `urllib` to call Alpaca’s HTTPS data API).
