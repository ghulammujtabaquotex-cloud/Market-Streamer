#!/usr/bin/env python3
"""
TradoWix Termux Bot
====================
Single-file WebSocket market data bot for Android Termux.

Install:
    pip install websockets

Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

API (replace PAIR with any symbol):
    http://localhost:8765/EURUSD          -> last 200 candles (REST + live merged)
    http://localhost:8765/EURUSD?limit=50 -> last 50 candles
    http://localhost:8765/EURUSD-OTC      -> OTC pair candles
    http://localhost:8765/status          -> connection status
    http://localhost:8765/pairs           -> all available pairs list
    http://localhost:8765/tick/EURUSD     -> latest live price only
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
    """Builds live OHLCV candles from raw price ticks."""

    def __init__(self, symbol: str, tf: int = 1):
        self.symbol   = symbol
        self.tf       = tf
        self._ms      = tf * 60_000
        self._cur:    Optional[Candle] = None
        self.closed:  list[Candle]     = []

    def _period(self, ts: float) -> float:
        return (ts // self._ms) * self._ms

    def feed(self, price: float, ts: float) -> None:
        p = self._period(ts)
        if self._cur is None:
            self._cur = Candle(self.symbol, self.tf, p, price, price, price, price, 1, False)
        elif p > self._cur.open_time:
            self._cur.is_closed = True
            self.closed.append(self._cur)
            self._cur = Candle(self.symbol, self.tf, p, price, price, price, price, 1, False)
        else:
            self._cur.high   = max(self._cur.high, price)
            self._cur.low    = min(self._cur.low,  price)
            self._cur.close  = price
            self._cur.volume += 1

    @property
    def live(self) -> Optional[Candle]:
        return self._cur


# ── Data Store ─────────────────────────────────────────────────────────────

class Store:
    def __init__(self):
        self.rest:   dict[str, list[Candle]]   = {}
        self.aggs:   dict[str, TickAggregator] = {}
        self.ticks:  dict[str, dict]           = {}
        self.pairs:  list[str]                 = []
        self.connected    = False
        self.status_msg   = "starting"
        self.subscribed   = 0
        self.started_at   = time.time()

    def tick(self, sym: str, price: float, ts: float):
        if sym not in self.aggs:
            self.aggs[sym] = TickAggregator(sym)
        self.aggs[sym].feed(price, ts)
        self.ticks[sym] = {"symbol": sym, "price": price, "ts_ms": int(ts)}

    def candles(self, sym: str, limit: int = 200) -> list[dict]:
        """
        Merge REST history + live closed candles + current open candle.
        Result: zero-gap, latest minute always included.
        """
        merged: dict[float, Candle] = {}

        for c in self.rest.get(sym, []):
            merged[c.open_time] = c

        agg = self.aggs.get(sym)
        if agg:
            for c in agg.closed:
                merged[c.open_time] = c          # WS candles overwrite REST (more accurate)
            if agg.live:
                merged[agg.live.open_time] = agg.live   # current open candle — never missing!

        sorted_c = sorted(merged.values(), key=lambda c: c.open_time)
        return [c.to_dict() for c in sorted_c[-limit:]]


# ── REST History ───────────────────────────────────────────────────────────

def _rest_sync(sym: str, token: str) -> list[Candle]:
    params = urllib.parse.urlencode({"symbol": sym, "timeframe": 60, "count": 200, "offset": 0})
    req = urllib.request.Request(
        f"{REST_URL}?{params}",
        headers={
            "User-Agent": UA,
            "Cookie":     f"session-token={token}; oauth_session_token={token}",
            "Accept":     "application/json",
            "Referer":    "https://tradowix.com/trading",
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


async def fetch_history(store: Store, sym: str):
    if not TOKEN:
        return
    try:
        loop    = asyncio.get_event_loop()
        candles = await loop.run_in_executor(None, _rest_sync, sym, TOKEN)
        if candles:
            store.rest[sym] = candles
            log.info("History %-22s %d candles", sym, len(candles))
    except Exception as e:
        log.warning("History failed %s: %s", sym, e)


async def fetch_all(store: Store):
    log.info("Fetching REST history for %d pairs ...", len(store.pairs))
    await asyncio.gather(*[fetch_history(store, s) for s in store.pairs], return_exceptions=True)
    log.info("REST history done — %d/%d pairs", len(store.rest), len(store.pairs))


# ── WebSocket Bot ──────────────────────────────────────────────────────────

async def ws_bot(store: Store):
    import websockets
    from websockets.exceptions import ConnectionClosed

    while True:
        store.status_msg = "connecting"
        try:
            log.info("Connecting to %s ...", WS_URL)
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                additional_headers={"Origin": "https://tradowix.com"},
            ) as ws:

                # ── Auth ──────────────────────────────────────────────────
                store.status_msg = "authenticating"
                instruments_ready = asyncio.Event()

                async for raw in ws:
                    msg = json.loads(raw)
                    t   = msg.get("type", "")

                    if t == "authRequired":
                        await ws.send(json.dumps({"type": "authenticate", "token": TOKEN}))

                    elif t in ("authenticated", "ready"):
                        uid = (msg.get("data") or {}).get("userId", "?")
                        log.info("Authenticated (userId=%s)", uid)
                        store.connected  = True
                        store.status_msg = "live"

                    elif t == "instruments":
                        insts = msg.get("data", [])
                        open_syms = [d["symbol"] for d in insts if d.get("symbol") and d.get("isOpen", True)]
                        store.pairs = open_syms
                        log.info("Instruments: %d open pairs", len(open_syms))

                        # Subscribe in batches of 20
                        for i in range(0, len(open_syms), 20):
                            batch = open_syms[i:i + 20]
                            await ws.send(json.dumps({"type": "subscribe", "symbols": batch, "timeframe": 1}))
                            await asyncio.sleep(0.05)

                        store.subscribed = len(open_syms)
                        instruments_ready.set()
                        asyncio.create_task(fetch_all(store))

                    elif t == "subscribed":
                        pass  # already counted above

                    elif t == "quote":
                        d   = msg.get("data", msg)
                        sym = d.get("symbol", "")
                        px  = float(d.get("price", d.get("bid", d.get("ask", 0))) or 0)
                        ts  = float(d.get("timestamp", d.get("t", time.time() * 1000)) or 0)
                        if sym and px:
                            store.tick(sym, px, ts)

                    elif t == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    elif t == "error":
                        log.error("Server: %s", msg.get("data", {}).get("error", msg))

        except ConnectionClosed as e:
            log.warning("Disconnected: %s — retry in 3s", e)
        except Exception as e:
            log.error("WS error: %s — retry in 3s", e)
        finally:
            store.connected  = False
            store.status_msg = "reconnecting"

        await asyncio.sleep(3)


# ── HTTP Server (pure stdlib asyncio) ─────────────────────────────────────

def _json(data) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode()


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, store: Store):
    try:
        raw = await asyncio.wait_for(reader.read(4096), timeout=5)
        if not raw:
            return

        line = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = line.split(" ")
        if len(parts) < 2:
            return

        path_full = parts[1]                           # e.g. /EURUSD?limit=50
        parsed    = urllib.parse.urlparse(path_full)
        path      = parsed.path.rstrip("/").upper()    # /EURUSD
        query     = urllib.parse.parse_qs(parsed.query)
        limit     = int(query.get("limit", ["200"])[0])

        # ── Routes ────────────────────────────────────────────────────────

        if path in ("/STATUS", "/"):
            body = _json({
                "connected":   store.connected,
                "status":      store.status_msg,
                "subscribed":  store.subscribed,
                "pairs_total": len(store.pairs),
                "rest_loaded": len(store.rest),
                "uptime_sec":  round(time.time() - store.started_at, 1),
                "tip":         "Use /EURUSD or /EURUSD-OTC to get candles",
            })

        elif path == "/PAIRS":
            body = _json({
                "count": len(store.pairs),
                "pairs": sorted(store.pairs),
            })

        elif path.startswith("/TICK/"):
            sym  = path[6:]   # /TICK/EURUSD -> EURUSD
            tick = store.ticks.get(sym)
            if tick:
                body = _json(tick)
            else:
                body = _json({"error": f"No tick yet for {sym}. Available: /pairs"})

        else:
            # /EURUSD  or  /EURUSD-OTC  etc.
            sym = path.lstrip("/")
            if not sym:
                body = _json({"error": "Pair name required. e.g. /EURUSD or /EURUSD-OTC"})
            else:
                candles = store.candles(sym, limit=limit)

                # On-demand fetch if no data yet
                if not candles and TOKEN:
                    log.info("On-demand fetch for %s", sym)
                    await fetch_history(store, sym)
                    candles = store.candles(sym, limit=limit)

                if not candles:
                    body = _json({"error": f"No data for {sym}. Check /pairs for valid symbols."})
                else:
                    latest = candles[-1]
                    body   = _json({
                        "symbol":       sym,
                        "count":        len(candles),
                        "has_live":     sym in store.aggs,
                        "has_history":  sym in store.rest,
                        "latest": {
                            "time":      latest["open_time"],
                            "close":     latest["close"],
                            "is_closed": latest["is_closed"],
                        },
                        "candles": candles,
                    })

        # ── Send response ─────────────────────────────────────────────────
        headers  = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"Access-Control-Allow-Origin: *\r\n"
            + f"Content-Length: {len(body)}\r\n".encode()
            + b"Connection: close\r\n\r\n"
        )
        writer.write(headers + body)
        await writer.drain()

    except Exception as e:
        log.debug("HTTP handler error: %s", e)
    finally:
        writer.close()


async def http_server(store: Store):
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, store),
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
        print("")
        print("  export TRADOWIX_TOKEN='your_session_token'")
        print("  python termux_bot.py")
        print("=" * 55)
        return

    store = Store()

    print("=" * 55)
    print("  TradoWix Termux Bot")
    print(f"  API  → http://localhost:{PORT}")
    print("")
    print("  Endpoints:")
    print("    /status          connection status")
    print("    /pairs           all available pairs")
    print("    /EURUSD          candle data for EURUSD")
    print("    /EURUSD?limit=50 last 50 candles")
    print("    /EURUSD-OTC      OTC pair candles")
    print("    /tick/EURUSD     latest live price")
    print("=" * 55)

    await asyncio.gather(
        http_server(store),
        ws_bot(store),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped.")
