# TradoWix Market Data Client

Python asyncio client for streaming live market data AND fetching historical OHLCV candles from TradoWix.

## Run & Operate

```bash
# Fetch historical candle data (all symbols)
TRADOWIX_TOKEN=your_token python fetch_history.py

# Fetch specific symbols
TRADOWIX_TOKEN=your_token python fetch_history.py EURUSD GBPUSD USDJPY

# Stream live ticks + build candles in real-time
TRADOWIX_TOKEN=your_token python example.py

# Quick 20-second live WebSocket test
TRADOWIX_TOKEN=your_token python test_live.py
```

## Stack

- Python 3.11+
- `asyncio` + `websockets` (v16)
- `aiofiles` for async file I/O
- `urllib` for REST API (stdlib, no extra deps)
- No external frameworks — pure stdlib + two deps

## Where things live

- `market_data_client/` — main package
  - `client.py`      — `MarketDataClient` (connect, subscribe, tick stream, candle aggregation, auto-reconnect)
  - `auth.py`        — `TokenAuthHandler` (token handshake)
  - `config.py`      — `ClientConfig` (env-aware dataclass)
  - `models.py`      — `Tick`, `Candle`, `TickAggregator`, `Instrument`, `ServerMessage`
  - `csv_export.py`  — `CsvExporter` (buffered, ticks + candles, one file per symbol)
  - `reconnect.py`   — `ReconnectPolicy` (exponential backoff + jitter)
  - `historical.py`  — `HistoricalDataClient` (REST API, historical OHLCV candles)
- `fetch_history.py` — fetch & save historical candles to CSV (new)
- `example.py`       — full live streaming demo
- `test_live.py`     — live 20-second WebSocket test
- `requirements.txt`
- `data/`            — CSV output directory (created at runtime)

---

## CONFIRMED: Historical Candle REST Endpoint

**Discovered by deep-scanning JS bundles + live HTTP testing.**

```
GET https://tradowix.com/api/chart/candles
    ?symbol=EURUSD
    &timeframe=60          ← SECONDS (not minutes! only 60 works)
    &count=200             ← max 200 per page
    &offset=0              ← pagination offset
Cookie: oauth_session_token=<TOKEN>
```

### Response format
```json
{
  "candles": [
    {
      "symbol":    "EURUSD",
      "timeframe": 60,
      "o": 1.16285,
      "h": 1.16288,
      "l": 1.16275,
      "c": 1.16287,
      "t": 1779357000000,
      "isClosed":  true
    }
  ],
  "hasMore": false,
  "total":   200
}
```

### Confirmed limits (live-tested)
| Parameter       | Value |
|-----------------|-------|
| Max candles     | 200 (server rolling window, ~3.3 hours of 1-min bars) |
| Only timeframe  | `60` (seconds) — 300, 3600, 86400 return empty |
| Pagination      | `offset=N` works |
| Timestamps      | `t` field is milliseconds epoch |

### Available symbols (confirmed with live data)
```
EURUSD  GBPUSD  USDJPY  USDCHF  AUDUSD  USDCAD
EURGBP  EURJPY  GBPJPY  ETHUSD  EURUSD-OTC  GBPUSD-OTC
```

---

## Confirmed TradoWix WebSocket Protocol

**Server**: `wss://api.tradowix.com/ws`

| Direction | Message |
|-----------|---------|
| Server→Client | `{"type":"authRequired"}` — server demands auth on connect |
| Client→Server | `{"type":"authenticate","token":"<oauth_session_token>"}` |
| Server→Client | `{"type":"authenticated","data":{"userId":"..."}}` — auth OK |
| Client→Server | `{"type":"subscribe","symbols":["EURUSD"],"timeframe":1}` — timeframe is **integer minutes** |
| Server→Client | `{"type":"subscribed","data":{"subscribed":["EURUSD"],"failed":[],"activeSymbol":"EURUSD"}}` |
| Server→Client | `{"type":"quote","data":{"symbol":"EURUSD","price":1.15977,"timestamp":1779368485738}}` — live tick |
| Server→Client | `{"type":"instruments","data":[{id, symbol, precision, isOTC, isOpen, ...}]}` — pushed on connect |
| Server→Client | `{"type":"balanceUpdate","data":{"balance":{demoBalance, realBalance, ...}}}` — pushed on connect |
| Server→Client | `{"type":"timeSync","timestamp":...}` — server clock sync (periodic) |
| Server→Client | `{"type":"pong"}` — response to ping |
| Client→Server | `{"type":"ping"}` — keepalive |

**Key facts discovered by live testing:**
- `timeframe` must be an **integer** (minutes: 1, 5, 15, 60, ...) — NOT a string like "1m"
- `symbols` must be an **array** — not `symbol` (singular)
- WebSocket streams **price ticks** (`quote` messages) only — no candle data via WS
- Historical candles come from the REST endpoint above
- Candles can also be built **client-side** using `TickAggregator` for real-time construction
- 116 symbols available including Forex, Crypto, OTC pairs

## Architecture

- All I/O is async — no blocking calls in any hot path
- Auth is a separate handshake phase before recv loop starts
- `HistoricalDataClient` uses urllib in a thread executor for async-friendly REST calls
- `TickAggregator` aggregates incoming ticks into OHLCV candles per symbol/timeframe
- CSV exporter buffers rows and flushes in batches; always flushed on clean disconnect
- Auto-reconnect rebuilds all subscriptions transparently after reconnect

## Gotchas

- `TRADOWIX_TOKEN` is the `oauth_session_token` cookie value from your TradoWix session
- The server sends `authRequired` immediately on connect — the client handles it automatically
- WebSocket `timeframe` is in **minutes** as an integer (1 = 1 min, 60 = 1 hour)
- REST `timeframe` is in **SECONDS** — always use `60` for 1-minute candles
- Server keeps a rolling window of ~200 1-minute candles (~3.3 hours of history)
- If the REST endpoint times out, wait 5-10 minutes (Cloudflare rate limiting) and retry
- `reconnect_max_attempts=0` means unlimited retries (default)

## User preferences

_Populate as you build — explicit user instructions worth remembering across sessions._
