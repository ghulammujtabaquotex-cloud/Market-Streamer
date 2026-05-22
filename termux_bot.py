#!/usr/bin/env python3
"""
TradoWix Termux Bot  v2
========================
Pure WebSocket approach — REST only for history, WS for live/latest candle.

Key insight: TradoWix WS streams ONE pair at a time (activeSymbol).
This bot subscribes on-demand: when you request /EURUSD it subscribes
EURUSD on WS, waits for the first tick, builds the current open candle,
merges with REST history → ZERO GAP guaranteed.

Install:
    pip install websockets

Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

Endpoints:
    http://localhost:8765/EURUSD            last 200 candles, latest included
    http://localhost:8765/EURUSD?limit=50   last 50 candles
    http://localhost:8765/EURUSD-OTC        OTC pair
    http://localhost:8765/status            bot status
    http://localhost:8765/pairs             all available pairs
    http://localhost:8765/tick/EURUSD       latest live price
"""

from __future__ import annotations
import asyncio, json, logging, os, time, urllib.parse, urllib.request
from dataclasses import dataclass
from typing import Optional

# ── Config ─────────────────────────────────────────────────────────────────

TOKEN    = os.environ.get("TRADOWIX_TOKEN", "")
WS_URL   = "wss://api.tradowix.com/ws"
REST_URL = "https://tradowix.com/api/chart/candles"
PORT     = int(os.environ.get("PORT", "8765"))
UA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# How long to wait for first WS tick when serving a request
TICK_WAIT_SEC = 5.0
# REST cache TTL — re-fetch history after this many seconds
REST_CACHE_TTL = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")


# ── Models ─────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    symbol:            str
    timeframe_minutes: int
    open_time:         float
    open:              float
    high:              float
    low:               float
    close:             float
    volume:            int
    is_closed:         bool

    def to_dict(self) -> dict:
        return {
            "symbol":            self.symbol,
            "timeframe_minutes": self.timeframe_minutes,
            "open_time":         int(self.open_time),
            "close_time":        int(self.open_time + self.timeframe_minutes * 60_000),
            "open":              round(self.open,  10),
            "high":              round(self.high,  10),
            "low":               round(self.low,   10),
            "close":             round(self.close, 10),
            "volume":            self.volume,
            "is_closed":         self.is_closed,
        }


class TickAggregator:
    """Builds OHLCV candles from live ticks."""

    def __init__(self, symbol: str, tf: int = 1):
        self.symbol  = symbol
        self.tf      = tf
        self._ms     = tf * 60_000
        self._cur:   Optional[Candle] = None
        self.closed: list[Candle]     = []

    def _period(self, ts: float) -> float:
        return (ts // self._ms) * self._ms

    def feed(self, price: float, ts: float) -> Optional[Candle]:
        """Returns a closed candle if the minute just rolled over, else None."""
        p = self._period(ts)
        completed = None
        if self._cur is None:
            self._cur = Candle(self.symbol, self.tf, p, price, price, price, price, 1, False)
        elif p > self._cur.open_time:
            self._cur.is_closed = True
            completed = self._cur
            self.closed.append(completed)
            self._cur = Candle(self.symbol, self.tf, p, price, price, price, price, 1, False)
        else:
            self._cur.high   = max(self._cur.high, price)
            self._cur.low    = min(self._cur.low,  price)
            self._cur.close  = price
            self._cur.volume += 1
        return completed

    @property
    def live(self) -> Optional[Candle]:
        """Current (open) candle — this is the 'missing' latest candle."""
        return self._cur

    @property
    def last_price(self) -> Optional[float]:
        return self._cur.close if self._cur else None


# ── Global State ───────────────────────────────────────────────────────────

class Store:
    def __init__(self):
        # REST cache: sym → (candles, fetched_at)
        self.rest:      dict[str, tuple[list[Candle], float]] = {}
        # WS aggregators: sym → TickAggregator
        self.aggs:      dict[str, TickAggregator]             = {}
        # Latest ticks: sym → {price, ts}
        self.ticks:     dict[str, dict]                       = {}
        # All available pair symbols (from WS instruments)
        self.pairs:     list[str]                             = []
        self.started_at = time.time()


class WSManager:
    """
    Manages a single persistent WebSocket connection.
    Handles authentication, re-subscribe on reconnect,
    and exposes subscribe() / wait_for_tick() for HTTP handlers.
    """

    def __init__(self, store: Store):
        self.store         = store
        self.ws            = None
        self.connected     = False
        self.status        = "starting"
        self.active_sym:   Optional[str] = None

        # asyncio.Event fires whenever a tick arrives for active_sym
        self._tick_event   = asyncio.Event()
        # queue for outgoing messages (so HTTP handlers can request a subscribe)
        self._sub_queue:   asyncio.Queue = asyncio.Queue()
        # protect subscribe from concurrent callers
        self._sub_lock     = asyncio.Lock()

    async def subscribe(self, sym: str) -> None:
        """Switch active subscription to sym. Thread-safe."""
        async with self._sub_lock:
            if self.active_sym == sym:
                return
            log.info("WS subscribe → %s", sym)
            self.active_sym = sym
            self._tick_event.clear()
            if self.ws:
                try:
                    await self.ws.send(json.dumps({
                        "type": "subscribe",
                        "symbols": [sym],
                        "timeframe": 1,
                    }))
                except Exception as e:
                    log.warning("subscribe send error: %s", e)

    async def wait_for_tick(self, sym: str, timeout: float = TICK_WAIT_SEC) -> bool:
        """
        Subscribe to sym, wait for the first tick.
        Returns True if a tick arrived within timeout, False otherwise.
        """
        await self.subscribe(sym)

        # If we already have a live candle (from previous ticks) and it's
        # for the current minute — we're good, no need to wait.
        agg = self.store.aggs.get(sym)
        if agg and agg.live:
            now_period = (time.time() * 1000 // 60_000) * 60_000
            if agg.live.open_time == now_period:
                return True

        # Wait for a fresh tick
        self._tick_event.clear()
        try:
            await asyncio.wait_for(self._tick_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            log.warning("No tick for %s within %.1fs", sym, timeout)
            return False

    def on_tick(self, sym: str, price: float, ts: float) -> None:
        """Called from the recv loop for every incoming quote."""
        if sym not in self.store.aggs:
            self.store.aggs[sym] = TickAggregator(sym)
        self.store.aggs[sym].feed(price, ts)
        self.store.ticks[sym] = {"symbol": sym, "price": price, "ts_ms": int(ts)}

        if sym == self.active_sym:
            self._tick_event.set()

    def merged_candles(self, sym: str, limit: int = 200) -> list[dict]:
        """
        REST history  +  WS closed candles  +  WS current open candle
        = zero-gap complete data, latest minute always included.
        """
        rest_candles, _ = self.store.rest.get(sym, ([], 0))
        agg              = self.store.aggs.get(sym)

        merged: dict[float, Candle] = {}

        # 1. Base: REST historical candles
        for c in rest_candles:
            merged[c.open_time] = c

        if agg:
            # 2. WS closed candles override REST (more accurate, built from real ticks)
            for c in agg.closed:
                merged[c.open_time] = c

            # 3. Current open candle — this is the "missing latest" the user wanted!
            if agg.live:
                merged[agg.live.open_time] = agg.live

        sorted_c = sorted(merged.values(), key=lambda c: c.open_time)
        return [c.to_dict() for c in sorted_c[-limit:]]


# ── REST History ───────────────────────────────────────────────────────────

def _rest_sync(sym: str) -> list[Candle]:
    params = urllib.parse.urlencode({"symbol": sym, "timeframe": 60, "count": 200, "offset": 0})
    req = urllib.request.Request(
        f"{REST_URL}?{params}",
        headers={
            "User-Agent": UA,
            "Cookie":  f"session-token={TOKEN}; oauth_session_token={TOKEN}",
            "Accept":  "application/json",
            "Referer": "https://tradowix.com/trading",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    out = []
    for d in data.get("candles", []):
        out.append(Candle(
            symbol=d["symbol"],
            timeframe_minutes=max(1, int(d.get("timeframe", 60)) // 60),
            open_time=float(d["t"]),
            open=float(d["o"]), high=float(d["h"]),
            low=float(d["l"]),  close=float(d["c"]),
            volume=0, is_closed=bool(d.get("isClosed", True)),
        ))
    out.sort(key=lambda c: c.open_time)
    return out


async def ensure_rest(store: Store, sym: str) -> list[Candle]:
    """Fetch REST history if not cached or cache expired."""
    cached, fetched_at = store.rest.get(sym, ([], 0))
    if cached and (time.time() - fetched_at) < REST_CACHE_TTL:
        return cached
    try:
        loop    = asyncio.get_event_loop()
        candles = await loop.run_in_executor(None, _rest_sync, sym)
        store.rest[sym] = (candles, time.time())
        log.info("REST %-22s %d candles", sym, len(candles))
        return candles
    except Exception as e:
        log.warning("REST failed %s: %s", sym, e)
        return cached  # return stale cache if any


# ── WebSocket Bot ──────────────────────────────────────────────────────────

async def ws_bot(mgr: WSManager):
    import websockets
    from websockets.exceptions import ConnectionClosed

    while True:
        mgr.status = "connecting"
        try:
            log.info("Connecting to %s ...", WS_URL)
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                additional_headers={"Origin": "https://tradowix.com"},
            ) as ws:
                mgr.ws = ws

                async for raw in ws:
                    msg = json.loads(raw)
                    t   = msg.get("type", "")

                    if t == "authRequired":
                        await ws.send(json.dumps({"type": "authenticate", "token": TOKEN}))
                        mgr.status = "authenticating"

                    elif t in ("authenticated", "ready"):
                        uid = (msg.get("data") or {}).get("userId", "?")
                        log.info("Authenticated (userId=%s)", uid)
                        mgr.connected = True
                        mgr.status    = "live"
                        # Re-subscribe to active pair if any (after reconnect)
                        if mgr.active_sym:
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "symbols": [mgr.active_sym],
                                "timeframe": 1,
                            }))

                    elif t == "instruments":
                        insts = msg.get("data", [])
                        mgr.store.pairs = [
                            d["symbol"] for d in insts
                            if d.get("symbol") and d.get("isOpen", True)
                        ]
                        log.info("Instruments: %d open pairs", len(mgr.store.pairs))

                    elif t == "subscribed":
                        d = msg.get("data", {})
                        active = d.get("activeSymbol", "")
                        ok     = d.get("subscribed", [])
                        failed = d.get("failed", [])
                        if failed:
                            log.warning("Subscribe failed: %s", failed)
                        log.info("Active pair: %s (ok=%s)", active, ok)

                    elif t == "quote":
                        d   = msg.get("data", msg)
                        sym = d.get("symbol", "")
                        px  = float(d.get("price", d.get("bid", d.get("ask", 0))) or 0)
                        ts  = float(d.get("timestamp", d.get("t", time.time() * 1000)) or 0)
                        if sym and px:
                            mgr.on_tick(sym, px, ts)

                    elif t == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    elif t == "error":
                        log.error("Server: %s", (msg.get("data") or {}).get("error", msg))

        except ConnectionClosed as e:
            log.warning("Disconnected: %s — retry in 3s", e)
        except Exception as e:
            log.error("WS error: %s — retry in 3s", e)
        finally:
            mgr.connected = False
            mgr.ws        = None
            mgr.status    = "reconnecting"

        await asyncio.sleep(3)


# ── HTTP Server ─────────────────────────────────────────────────────────────

def _resp(data, status: int = 200) -> bytes:
    body = json.dumps(data, ensure_ascii=False).encode()
    head = (
        f"HTTP/1.1 {status} OK\r\n"
        "Content-Type: application/json\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    return head + body


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 mgr: WSManager):
    try:
        raw = await asyncio.wait_for(reader.read(4096), timeout=10)
        if not raw:
            return

        line  = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = line.split(" ")
        if len(parts) < 2:
            return

        parsed = urllib.parse.urlparse(parts[1])
        path   = parsed.path.rstrip("/").upper()
        query  = urllib.parse.parse_qs(parsed.query)
        limit  = int(query.get("limit", ["200"])[0])
        store  = mgr.store

        # ── /STATUS or / ──────────────────────────────────────────────────
        if path in ("/STATUS", "/", ""):
            data = {
                "connected":    mgr.connected,
                "status":       mgr.status,
                "active_pair":  mgr.active_sym,
                "pairs_loaded": len(store.pairs),
                "rest_cached":  len(store.rest),
                "ws_pairs_live": len(store.aggs),
                "uptime_sec":   round(time.time() - store.started_at, 1),
                "usage":        "GET /EURUSD  or  /EURUSD-OTC  or  /BTCUSD-OTC",
            }

        # ── /PAIRS ────────────────────────────────────────────────────────
        elif path == "/PAIRS":
            data = {"count": len(store.pairs), "pairs": sorted(store.pairs)}

        # ── /TICK/EURUSD ─────────────────────────────────────────────────
        elif path.startswith("/TICK/"):
            sym  = path[6:]
            tick = store.ticks.get(sym)
            if tick:
                data = tick
            else:
                # Subscribe and wait for first tick
                got = await mgr.wait_for_tick(sym, timeout=5.0)
                tick = store.ticks.get(sym)
                data = tick if tick else {"error": f"No tick for {sym} within 5s"}

        # ── /EURUSD  or  /EURUSD-OTC  etc. ───────────────────────────────
        else:
            sym = path.lstrip("/")
            if not sym:
                data = {"error": "Pair name required. e.g. /EURUSD"}
                writer.write(_resp(data, 400))
                await writer.drain()
                return

            # Step 1: Fetch REST history (cached, fast after first call)
            rest_task = asyncio.create_task(ensure_rest(store, sym))

            # Step 2: Subscribe to pair on WS, wait for live tick
            ws_task = asyncio.create_task(mgr.wait_for_tick(sym, timeout=5.0))

            # Run both concurrently
            await asyncio.gather(rest_task, ws_task, return_exceptions=True)
            got_tick = ws_task.result() if not ws_task.cancelled() else False

            # Step 3: Build merged candles
            candles = mgr.merged_candles(sym, limit=limit)

            if not candles:
                data = {"error": f"No data for {sym}. Try /pairs"}
            else:
                latest  = candles[-1]
                agg     = store.aggs.get(sym)
                rest_c, _ = store.rest.get(sym, ([], 0))

                # Verify no gap: check if WS candle is newer than last REST candle
                rest_latest_t  = rest_c[-1].open_time if rest_c else 0
                ws_live_t      = agg.live.open_time   if (agg and agg.live) else 0
                gap_minutes    = round((ws_live_t - rest_latest_t) / 60_000) if (rest_latest_t and ws_live_t) else None

                data = {
                    "symbol":       sym,
                    "count":        len(candles),
                    "has_live_ws":  got_tick,
                    "has_rest":     bool(rest_c),
                    "gap_check": {
                        "rest_latest":    latest["open_time"] if not got_tick else (int(rest_latest_t) if rest_c else None),
                        "ws_live_time":   int(ws_live_t) if ws_live_t else None,
                        "gap_minutes":    gap_minutes,
                        "verdict":        "NO GAP ✅" if (gap_minutes is not None and gap_minutes <= 1)
                                          else ("SMALL GAP ⚠️" if (gap_minutes and gap_minutes <= 5)
                                          else ("NO WS TICK YET" if not got_tick else "CHECK MANUALLY")),
                    },
                    "latest_candle": latest,
                    "candles":      candles,
                }

        writer.write(_resp(data))
        await writer.drain()

    except Exception as e:
        log.debug("HTTP error: %s", e)
    finally:
        writer.close()


async def http_server(mgr: WSManager):
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, mgr),
        "0.0.0.0", PORT,
    )
    log.info("HTTP API → http://localhost:%d", PORT)
    async with server:
        await server.serve_forever()


# ── Main ───────────────────────────────────────────────────────────────────

async def main():
    if not TOKEN:
        print("=" * 55)
        print("  ERROR: TRADOWIX_TOKEN not set!")
        print("  export TRADOWIX_TOKEN='your_session_token'")
        print("=" * 55)
        return

    store = Store()
    mgr   = WSManager(store)

    print("=" * 55)
    print("  TradoWix Termux Bot  v2")
    print(f"  API → http://localhost:{PORT}")
    print()
    print("  /status            bot status")
    print("  /pairs             all available pairs")
    print("  /EURUSD            candles with LIVE latest")
    print("  /EURUSD?limit=50   last 50 candles")
    print("  /EURUSD-OTC        OTC pair")
    print("  /tick/EURUSD       live price")
    print("=" * 55)

    await asyncio.gather(
        http_server(mgr),
        ws_bot(mgr),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped.")
