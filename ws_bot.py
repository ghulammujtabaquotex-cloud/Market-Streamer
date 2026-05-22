#!/usr/bin/env python3
"""
TradoWix WebSocket Data Bot
============================
Connects to wss://api.tradowix.com/ws, subscribes to ALL open instruments,
builds real-time candles from live ticks, merges with REST history,
and serves a zero-gap JSON API on http://localhost:8765

Setup:
    export TRADOWIX_TOKEN="your_oauth_session_token"
    python ws_bot.py

Endpoints:
    GET /status                              → WS connection info + subscribed pairs
    GET /instruments                         → all instruments list
    GET /candles?symbol=EURUSD&limit=200     → merged REST+live candles (latest included)
    GET /ticks?symbol=EURUSD                 → latest tick price for a symbol
    GET /candles/all                         → current live candle for every subscribed pair
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from typing import Optional
from aiohttp import web
import websockets
from websockets.exceptions import ConnectionClosed

# ── Config ────────────────────────────────────────────────────────────────────

TOKEN      = os.environ.get("TRADOWIX_TOKEN", "")
WS_URL     = "wss://api.tradowix.com/ws"
REST_URL   = "https://tradowix.com/api/chart/candles"
API_HOST   = "0.0.0.0"
API_PORT   = int(os.environ.get("API_PORT", "8765"))
LOG_LEVEL  = os.environ.get("LOG_LEVEL", "INFO")
TIMEFRAME  = 1          # 1-minute candles
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ws_bot")


# ── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class Candle:
    symbol:            str
    timeframe_minutes: int
    open_time:         float   # milliseconds
    open:              float
    high:              float
    low:               float
    close:             float
    volume:            int     # tick count
    is_closed:         bool

    @property
    def close_time(self) -> float:
        return self.open_time + self.timeframe_minutes * 60_000

    def to_dict(self) -> dict:
        return {
            "symbol":            self.symbol,
            "timeframe_minutes": self.timeframe_minutes,
            "open_time":         int(self.open_time),
            "close_time":        int(self.close_time),
            "open":              round(self.open,  10),
            "high":              round(self.high,  10),
            "low":               round(self.low,   10),
            "close":             round(self.close, 10),
            "volume":            self.volume,
            "is_closed":         self.is_closed,
        }


@dataclass
class Instrument:
    id:           str
    symbol:       str
    name:         str
    display_name: str
    category:     str
    precision:    int
    is_active:    bool
    is_otc:       bool
    is_open:      bool

    def to_dict(self) -> dict:
        return asdict(self)


# ── TickAggregator ────────────────────────────────────────────────────────────

class TickAggregator:
    """Converts live ticks → OHLCV candles. Always holds the current open candle."""

    def __init__(self, symbol: str, timeframe_minutes: int = 1):
        self.symbol            = symbol
        self.timeframe_minutes = timeframe_minutes
        self._period_ms        = timeframe_minutes * 60_000
        self._current:  Optional[Candle] = None
        self.completed: list[Candle]     = []   # closed candles from live ticks

    def _period_start(self, ts_ms: float) -> float:
        return (ts_ms // self._period_ms) * self._period_ms

    def update(self, price: float, ts_ms: float) -> Optional[Candle]:
        """Feed one tick. Returns a closed candle if the period just rolled over."""
        period    = self._period_start(ts_ms)
        completed = None

        if self._current is None:
            self._current = Candle(
                symbol=self.symbol, timeframe_minutes=self.timeframe_minutes,
                open_time=period,
                open=price, high=price, low=price, close=price,
                volume=1, is_closed=False,
            )
        elif period > self._current.open_time:
            self._current.is_closed = True
            completed = self._current
            self.completed.append(completed)
            self._current = Candle(
                symbol=self.symbol, timeframe_minutes=self.timeframe_minutes,
                open_time=period,
                open=price, high=price, low=price, close=price,
                volume=1, is_closed=False,
            )
        else:
            self._current.high  = max(self._current.high, price)
            self._current.low   = min(self._current.low,  price)
            self._current.close = price
            self._current.volume += 1

        return completed

    @property
    def current_candle(self) -> Optional[Candle]:
        return self._current


# ── In-Memory Store ───────────────────────────────────────────────────────────

class DataStore:
    """
    Central in-memory store.

    For every symbol:
      rest_candles  – fetched from REST (historical, may lag 4+ min)
      aggregator    – builds live candles from WS ticks
      last_tick     – latest price + timestamp

    get_candles() merges rest + live + current_open for zero-gap data.
    """

    def __init__(self):
        self.instruments:  dict[str, Instrument]     = {}
        self.rest_candles: dict[str, list[Candle]]   = {}
        self.aggregators:  dict[str, TickAggregator] = {}
        self.last_tick:    dict[str, dict]            = {}
        self.ws_connected  = False
        self.ws_status     = "disconnected"
        self.subscribed:   set[str]                  = set()
        self.start_time    = time.time()

    def ensure_aggregator(self, symbol: str) -> TickAggregator:
        if symbol not in self.aggregators:
            self.aggregators[symbol] = TickAggregator(symbol, TIMEFRAME)
        return self.aggregators[symbol]

    def on_tick(self, symbol: str, price: float, ts_ms: float):
        agg = self.ensure_aggregator(symbol)
        agg.update(price, ts_ms)
        self.last_tick[symbol] = {"symbol": symbol, "price": price, "ts_ms": int(ts_ms)}

    def get_candles(self, symbol: str, limit: int = 500) -> list[dict]:
        """
        Returns merged candle list:
          1. REST candles (historical baseline)
          2. Live closed candles from WS ticks that are NEWER than last REST candle
          3. Current open (live) candle — always the latest minute, no gap!

        All deduplicated and sorted oldest → newest.
        """
        rest   = self.rest_candles.get(symbol, [])
        agg    = self.aggregators.get(symbol)

        # Build a dict keyed by open_time for deduplication
        merged: dict[float, Candle] = {}

        for c in rest:
            merged[c.open_time] = c

        if agg:
            # Add all live-closed candles from WS
            for c in agg.completed:
                merged[c.open_time] = c   # WS is more accurate (real ticks)

            # Add the current open candle (this is the "missing latest" candle!)
            if agg.current_candle:
                merged[agg.current_candle.open_time] = agg.current_candle

        sorted_candles = sorted(merged.values(), key=lambda c: c.open_time)
        return [c.to_dict() for c in sorted_candles[-limit:]]

    def status(self) -> dict:
        return {
            "ws_connected":   self.ws_connected,
            "ws_status":      self.ws_status,
            "subscribed_count": len(self.subscribed),
            "subscribed_pairs": sorted(self.subscribed),
            "instruments_total": len(self.instruments),
            "open_instruments": sum(1 for i in self.instruments.values() if i.is_open),
            "uptime_seconds": round(time.time() - self.start_time, 1),
        }


# ── REST History Fetcher ───────────────────────────────────────────────────────

def _fetch_rest_sync(symbol: str, token: str, count: int = 200) -> list[Candle]:
    params = urllib.parse.urlencode({
        "symbol":    symbol,
        "timeframe": 60,
        "count":     count,
        "offset":    0,
    })
    url = f"{REST_URL}?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Cookie":     f"oauth_session_token={token}",
        "Accept":     "application/json",
        "Referer":    "https://tradowix.com/trading",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read().decode())

    candles = []
    for d in body.get("candles", []):
        tf_sec = int(d.get("timeframe", 60))
        candles.append(Candle(
            symbol=d["symbol"],
            timeframe_minutes=max(1, tf_sec // 60),
            open_time=float(d["t"]),
            open=float(d["o"]),
            high=float(d["h"]),
            low=float(d["l"]),
            close=float(d["c"]),
            volume=0,
            is_closed=bool(d.get("isClosed", True)),
        ))

    candles.sort(key=lambda c: c.open_time)
    return candles


async def fetch_rest_history(store: DataStore, symbol: str):
    """Fetch REST history for one symbol and store it."""
    if not TOKEN:
        return
    try:
        loop    = asyncio.get_event_loop()
        candles = await loop.run_in_executor(None, _fetch_rest_sync, symbol, TOKEN)
        if candles:
            store.rest_candles[symbol] = candles
            log.info("REST history %-20s → %d candles", symbol, len(candles))
        else:
            log.debug("REST history %s → no data", symbol)
    except Exception as exc:
        log.warning("REST fetch failed for %s: %s", symbol, exc)


async def prefetch_all_history(store: DataStore):
    """Fetch REST history for every open instrument concurrently."""
    open_symbols = [s for s, i in store.instruments.items() if i.is_open]
    if not open_symbols:
        return
    log.info("Pre-fetching REST history for %d open instruments ...", len(open_symbols))
    tasks = [fetch_rest_history(store, sym) for sym in open_symbols]
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("REST pre-fetch done. %d symbols have history.", len(store.rest_candles))


# ── WebSocket Bot ─────────────────────────────────────────────────────────────

async def ws_bot(store: DataStore):
    """Main WebSocket loop — connects, authenticates, subscribes, receives forever."""
    while True:
        store.ws_status = "connecting"
        try:
            log.info("Connecting to %s ...", WS_URL)
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                additional_headers={"Origin": "https://tradowix.com"},
            ) as ws:
                # ── Authenticate ──────────────────────────────────────────────
                await ws.send(json.dumps({"type": "authenticate", "token": TOKEN}))
                store.ws_status = "authenticating"

                async def recv_one():
                    raw = await ws.recv()
                    return json.loads(raw)

                # Wait for authenticated
                while True:
                    msg = await asyncio.wait_for(recv_one(), timeout=15)
                    t = msg.get("type", "")
                    if t in ("authenticated", "ready"):
                        uid = (msg.get("data") or {}).get("userId", "?")
                        log.info("Authenticated (userId=%s)", uid)
                        store.ws_connected = True
                        store.ws_status = "authenticated"
                        break
                    if t in ("authError", "auth_error"):
                        raise RuntimeError(f"Auth rejected: {msg}")
                    if t == "authRequired":
                        await ws.send(json.dumps({"type": "authenticate", "token": TOKEN}))
                    # instruments may arrive before authenticated on some builds
                    if t == "instruments":
                        _handle_instruments(msg, store)

                # ── Subscribe to all open instruments ────────────────────────
                instruments_received = asyncio.Event()

                # Some servers push instruments right away; handle if already loaded
                if store.instruments:
                    instruments_received.set()

                async def wait_and_subscribe():
                    await asyncio.wait_for(instruments_received.wait(), timeout=10)
                    open_syms = [s for s, i in store.instruments.items() if i.is_open]
                    if not open_syms:
                        log.warning("No open instruments to subscribe to")
                        return
                    log.info("Subscribing to %d open instruments ...", len(open_syms))
                    # Subscribe in batches of 20 to avoid overloading
                    batch_size = 20
                    for i in range(0, len(open_syms), batch_size):
                        batch = open_syms[i:i + batch_size]
                        await ws.send(json.dumps({
                            "type":      "subscribe",
                            "symbols":   batch,
                            "timeframe": TIMEFRAME,
                        }))
                        await asyncio.sleep(0.1)
                    store.subscribed.update(open_syms)
                    log.info("Subscribed to %d pairs", len(open_syms))
                    # Now prefetch REST history in background
                    asyncio.create_task(prefetch_all_history(store))

                asyncio.create_task(wait_and_subscribe())

                # ── Main receive loop ─────────────────────────────────────────
                store.ws_status = "live"
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    t = msg.get("type", "")

                    if t == "quote":
                        _handle_quote(msg, store)

                    elif t == "instruments":
                        _handle_instruments(msg, store)
                        instruments_received.set()

                    elif t == "subscribed":
                        d = msg.get("data", {})
                        ok     = d.get("subscribed", [])
                        failed = d.get("failed", [])
                        if ok:
                            log.debug("Subscribed ok: %s", ok)
                        if failed:
                            log.warning("Subscribe failed: %s", failed)

                    elif t == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    elif t == "error":
                        err = (msg.get("data") or {}).get("error") or msg.get("message") or "?"
                        log.error("Server error: %s", err)

        except ConnectionClosed as exc:
            log.warning("WS disconnected: %s — reconnecting in 3s", exc)
        except asyncio.TimeoutError:
            log.warning("WS timeout — reconnecting in 3s")
        except Exception as exc:
            log.error("WS error: %s — reconnecting in 3s", exc)
        finally:
            store.ws_connected = False
            store.ws_status = "reconnecting"

        await asyncio.sleep(3)


def _handle_instruments(msg: dict, store: DataStore):
    data = msg.get("data", [])
    count = 0
    for d in data:
        inst = Instrument(
            id=d.get("id", ""),
            symbol=d.get("symbol", ""),
            name=d.get("name", ""),
            display_name=d.get("displayName", ""),
            category=d.get("category", ""),
            precision=int(d.get("precision", 5)),
            is_active=bool(d.get("isActive", True)),
            is_otc=bool(d.get("isOTC", False)),
            is_open=bool(d.get("isOpen", True)),
        )
        if inst.symbol:
            store.instruments[inst.symbol] = inst
            count += 1
    log.info("Instruments catalogue: %d total", count)


def _handle_quote(msg: dict, store: DataStore):
    d      = msg.get("data", msg)
    symbol = d.get("symbol", "")
    price  = float(d.get("price", d.get("bid", d.get("ask", 0))) or 0)
    ts_ms  = float(d.get("timestamp", d.get("t", time.time() * 1000)) or 0)
    if symbol and price:
        store.on_tick(symbol, price, ts_ms)


# ── HTTP API ──────────────────────────────────────────────────────────────────

def json_response(data, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps(data, ensure_ascii=False),
    )


async def handle_status(request: web.Request) -> web.Response:
    store: DataStore = request.app["store"]
    return json_response(store.status())


async def handle_instruments(request: web.Request) -> web.Response:
    store: DataStore = request.app["store"]
    data = [i.to_dict() for i in store.instruments.values()]
    data.sort(key=lambda x: x["symbol"])
    return json_response({"count": len(data), "instruments": data})


async def handle_candles(request: web.Request) -> web.Response:
    store:  DataStore = request.app["store"]
    symbol = request.rel_url.query.get("symbol", "").upper().strip()
    limit  = int(request.rel_url.query.get("limit", "500"))

    if not symbol:
        return json_response({"error": "symbol param required. e.g. ?symbol=EURUSD"}, 400)

    candles = store.get_candles(symbol, limit=limit)

    if not candles:
        # Try fetch REST on demand if we don't have data yet
        if symbol not in store.rest_candles and TOKEN:
            await fetch_rest_history(store, symbol)
            candles = store.get_candles(symbol, limit=limit)

    meta = {
        "symbol":          symbol,
        "count":           len(candles),
        "has_live_data":   symbol in store.aggregators,
        "has_rest_data":   symbol in store.rest_candles,
        "latest_candle":   candles[-1] if candles else None,
        "candles":         candles,
    }
    return json_response(meta)


async def handle_candles_all(request: web.Request) -> web.Response:
    """Return the current live candle for every subscribed pair."""
    store: DataStore = request.app["store"]
    result = {}
    for symbol, agg in store.aggregators.items():
        if agg.current_candle:
            result[symbol] = agg.current_candle.to_dict()
    return json_response({
        "count":   len(result),
        "candles": result,
    })


async def handle_ticks(request: web.Request) -> web.Response:
    store:  DataStore = request.app["store"]
    symbol = request.rel_url.query.get("symbol", "").upper().strip()
    if not symbol:
        all_ticks = dict(sorted(store.last_tick.items()))
        return json_response({"count": len(all_ticks), "ticks": all_ticks})
    tick = store.last_tick.get(symbol)
    if not tick:
        return json_response({"error": f"No tick data yet for {symbol}"}, 404)
    return json_response(tick)


async def handle_index(request: web.Request) -> web.Response:
    return json_response({
        "service": "TradoWix WebSocket Data Bot",
        "endpoints": {
            "GET /status":          "WS connection status + subscribed pairs",
            "GET /instruments":     "All instruments list",
            "GET /candles?symbol=EURUSD&limit=200": "Merged REST+live candles (latest included)",
            "GET /candles/all":     "Current live candle for all subscribed pairs",
            "GET /ticks":           "Latest tick price for all pairs",
            "GET /ticks?symbol=EURUSD": "Latest tick price for one pair",
        },
    })


def build_app(store: DataStore) -> web.Application:
    app = web.Application()
    app["store"] = store

    app.router.add_get("/",              handle_index)
    app.router.add_get("/status",        handle_status)
    app.router.add_get("/instruments",   handle_instruments)
    app.router.add_get("/candles/all",   handle_candles_all)
    app.router.add_get("/candles",       handle_candles)
    app.router.add_get("/ticks",         handle_ticks)

    return app


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main():
    if not TOKEN:
        log.error("TRADOWIX_TOKEN env var not set! Exiting.")
        log.error("  export TRADOWIX_TOKEN='your_oauth_session_token'")
        return

    store = DataStore()

    log.info("=" * 55)
    log.info("  TradoWix WebSocket Data Bot")
    log.info("  API → http://localhost:%d", API_PORT)
    log.info("=" * 55)

    app    = build_app(store)
    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, API_HOST, API_PORT)
    await site.start()
    log.info("HTTP API listening on http://localhost:%d", API_PORT)

    # Run WebSocket bot concurrently
    await ws_bot(store)


if __name__ == "__main__":
    asyncio.run(main())
