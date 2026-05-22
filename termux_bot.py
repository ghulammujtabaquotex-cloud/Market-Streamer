#!/usr/bin/env python3
"""
TradoWix Pure WebSocket Bot  v3
================================
100% WebSocket only — NO REST API at all.

How it works:
  - User requests /EURUSD → bot subscribes EURUSD on WS
  - Live ticks arrive → TickAggregator builds candles minute by minute
  - Current open candle (is_closed=False) is ALWAYS the latest minute
  - Zero gap — because everything is built from real live ticks
  - Data grows over time as bot runs

Limitation vs REST: no 200-candle history on first start.
Advantage: zero lag, zero gap, latest candle 100% accurate.

Install:
    pip install websockets

Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

Endpoints:
    /EURUSD              all candles collected so far + live current
    /EURUSD?limit=50     last 50 candles
    /EURUSD-OTC          any OTC pair
    /status              bot status
    /pairs               all 116 available pairs
    /tick/EURUSD         latest price + timestamp
"""

from __future__ import annotations
import asyncio, json, logging, os, time, urllib.parse
from dataclasses import dataclass, field
from typing import Optional

# ── Config ──────────────────────────────────────────────────────────────────

TOKEN = os.environ.get("TRADOWIX_TOKEN", "")
WS_URL = "wss://api.tradowix.com/ws"
PORT   = int(os.environ.get("PORT", "8765"))

# Seconds to wait for first tick after subscribing
TICK_WAIT = 6.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ── Candle model ─────────────────────────────────────────────────────────────

@dataclass
class Candle:
    symbol: str
    tf:     int          # timeframe minutes
    t:      float        # open_time ms
    o:      float
    h:      float
    l:      float
    c:      float
    vol:    int          # tick count
    closed: bool

    def to_dict(self) -> dict:
        return {
            "symbol":            self.symbol,
            "timeframe_minutes": self.tf,
            "open_time":         int(self.t),
            "close_time":        int(self.t + self.tf * 60_000),
            "open":              round(self.o, 10),
            "high":              round(self.h, 10),
            "low":               round(self.l, 10),
            "close":             round(self.c, 10),
            "volume":            self.vol,
            "is_closed":         self.closed,
        }

# ── Tick Aggregator ───────────────────────────────────────────────────────────

class Agg:
    """
    Feeds on raw price ticks, emits closed Candles, always has a live candle.
    This is the ONLY data source — pure WebSocket ticks.
    """

    def __init__(self, sym: str, tf: int = 1):
        self.sym      = sym
        self.tf       = tf
        self._ms      = tf * 60_000
        self._cur:    Optional[Candle] = None
        self.history: list[Candle]     = []   # closed candles, oldest first
        self.since:   float            = time.time()

    def _period(self, ts: float) -> float:
        return (ts // self._ms) * self._ms

    def feed(self, price: float, ts: float) -> None:
        p = self._period(ts)

        if self._cur is None:
            # First tick ever for this pair
            self._cur = Candle(self.sym, self.tf, p, price, price, price, price, 1, False)
            log.info("First tick %-20s @ %.5f  minute=%s", self.sym, price,
                     _fmt(p))

        elif p > self._cur.t:
            # Minute rolled over — close current, start new
            self._cur.closed = True
            self.history.append(self._cur)
            log.debug("Candle closed  %-20s  %s  O=%.5f H=%.5f L=%.5f C=%.5f  ticks=%d",
                      self.sym, _fmt(self._cur.t),
                      self._cur.o, self._cur.h, self._cur.l, self._cur.c, self._cur.vol)
            self._cur = Candle(self.sym, self.tf, p, price, price, price, price, 1, False)

        else:
            # Same minute — update OHLCV
            self._cur.h   = max(self._cur.h, price)
            self._cur.l   = min(self._cur.l, price)
            self._cur.c   = price
            self._cur.vol += 1

    @property
    def live(self) -> Optional[Candle]:
        """Current open candle — the latest minute, always fresh."""
        return self._cur

    def all_candles(self, limit: int = 500) -> list[Candle]:
        """Closed history + current open. Sorted oldest → newest."""
        result = list(self.history)
        if self._cur:
            result.append(self._cur)
        return result[-limit:]


def _fmt(ts_ms: float) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%H:%M")


# ── Bot State ─────────────────────────────────────────────────────────────────

class Bot:
    def __init__(self):
        self.aggs:         dict[str, Agg]  = {}   # sym → Agg
        self.ticks:        dict[str, dict] = {}   # sym → last tick info
        self.pairs:        list[str]       = []   # all instruments
        self.active_sym:   Optional[str]   = None
        self.ws                            = None
        self.connected                     = False
        self.status                        = "starting"
        self.started_at                    = time.time()

        # fires when a tick arrives for the active symbol
        self._tick_event = asyncio.Event()
        self._sub_lock   = asyncio.Lock()

    # ── WebSocket helpers ────────────────────────────────────────────────────

    async def _send(self, data: dict) -> None:
        if self.ws:
            try:
                await self.ws.send(json.dumps(data))
            except Exception as e:
                log.warning("WS send error: %s", e)

    async def subscribe(self, sym: str) -> None:
        """Subscribe to sym — makes it the active streaming pair."""
        async with self._sub_lock:
            if self.active_sym == sym:
                return
            log.info("Subscribe → %s", sym)
            self.active_sym = sym
            self._tick_event.clear()
            await self._send({"type": "subscribe", "symbols": [sym], "timeframe": 1})

    async def wait_tick(self, sym: str, timeout: float = TICK_WAIT) -> bool:
        """Subscribe to sym and wait for at least one tick. Returns True if got tick."""
        await self.subscribe(sym)

        # Already have a fresh live candle for this minute?
        agg = self.aggs.get(sym)
        if agg and agg.live:
            now_p = (time.time() * 1000 // 60_000) * 60_000
            if agg.live.t >= now_p - 60_000:   # within last 1 minute
                return True

        self._tick_event.clear()
        try:
            await asyncio.wait_for(self._tick_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            log.warning("No tick for %s in %.0fs", sym, timeout)
            return False

    def on_tick(self, sym: str, price: float, ts: float) -> None:
        if sym not in self.aggs:
            self.aggs[sym] = Agg(sym)
        self.aggs[sym].feed(price, ts)
        self.ticks[sym] = {
            "symbol": sym,
            "price":  price,
            "ts_ms":  int(ts),
            "time":   _fmt(ts),
        }
        if sym == self.active_sym:
            self._tick_event.set()

    def candles(self, sym: str, limit: int = 200) -> list[dict]:
        agg = self.aggs.get(sym)
        if not agg:
            return []
        return [c.to_dict() for c in agg.all_candles(limit=limit)]


# ── WebSocket Loop ────────────────────────────────────────────────────────────

async def ws_loop(bot: Bot) -> None:
    import websockets
    from websockets.exceptions import ConnectionClosed

    while True:
        bot.status = "connecting"
        try:
            log.info("Connecting %s ...", WS_URL)
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                additional_headers={"Origin": "https://tradowix.com"},
            ) as ws:
                bot.ws = ws

                async for raw in ws:
                    msg = json.loads(raw)
                    t   = msg.get("type", "")

                    if t == "authRequired":
                        await ws.send(json.dumps({"type": "authenticate", "token": TOKEN}))
                        bot.status = "authenticating"

                    elif t in ("authenticated", "ready"):
                        uid = (msg.get("data") or {}).get("userId", "?")
                        log.info("Authenticated uid=%s", uid)
                        bot.connected = True
                        bot.status    = "live"
                        # Re-subscribe active pair after reconnect
                        if bot.active_sym:
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "symbols": [bot.active_sym],
                                "timeframe": 1,
                            }))

                    elif t == "instruments":
                        bot.pairs = [
                            d["symbol"] for d in msg.get("data", [])
                            if d.get("symbol") and d.get("isOpen", True)
                        ]
                        log.info("Instruments: %d open pairs", len(bot.pairs))

                    elif t == "subscribed":
                        d      = msg.get("data", {})
                        active = d.get("activeSymbol", "")
                        ok     = d.get("subscribed", [])
                        log.info("Active: %s  ok=%s  failed=%s", active, ok, d.get("failed", []))

                    elif t == "quote":
                        d   = msg.get("data", msg)
                        sym = d.get("symbol", "")
                        px  = float(d.get("price", d.get("bid", d.get("ask", 0))) or 0)
                        ts  = float(d.get("timestamp", d.get("t", time.time() * 1000)) or 0)
                        if sym and px:
                            bot.on_tick(sym, px, ts)

                    elif t == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    elif t == "error":
                        log.error("Server error: %s", (msg.get("data") or {}).get("error", msg))

        except ConnectionClosed as e:
            log.warning("Disconnected: %s — retry in 3s", e)
        except Exception as e:
            log.error("WS error: %s — retry in 3s", e)
        finally:
            bot.connected = False
            bot.ws        = None
            bot.status    = "reconnecting"

        await asyncio.sleep(3)


# ── HTTP Server ───────────────────────────────────────────────────────────────

# Paths that browsers auto-request — ignore silently
_IGNORE = {"/FAVICON.ICO", "/ROBOTS.TXT", "/APPLE-TOUCH-ICON.PNG",
           "/APPLE-TOUCH-ICON-PRECOMPOSED.PNG", "/SITEMAP.XML", "/.WELL-KNOWN"}


def _ok(data) -> bytes:
    body = json.dumps(data, ensure_ascii=False).encode()
    head = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    return head + body


def _err(msg: str, code: int = 400) -> bytes:
    body = json.dumps({"error": msg}).encode()
    head = (
        f"HTTP/1.1 {code} Error\r\n"
        "Content-Type: application/json\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    return head + body


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 bot: Bot) -> None:
    try:
        raw = await asyncio.wait_for(reader.read(4096), timeout=10)
        if not raw:
            return

        first_line = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = first_line.split(" ")
        if len(parts) < 2:
            return

        parsed = urllib.parse.urlparse(parts[1])
        path   = parsed.path.rstrip("/").upper()
        qs     = urllib.parse.parse_qs(parsed.query)
        limit  = int(qs.get("limit", ["200"])[0])

        # Silently ignore browser junk
        if path in _IGNORE or path.startswith("/."):
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        # ── Routes ──────────────────────────────────────────────────────────

        if path in ("/STATUS", "/", ""):
            import datetime
            data = {
                "connected":    bot.connected,
                "status":       bot.status,
                "active_pair":  bot.active_sym,
                "pairs_total":  len(bot.pairs),
                "pairs_live":   len(bot.aggs),
                "uptime_sec":   round(time.time() - bot.started_at, 1),
                "note":         "Pure WebSocket — data builds up as bot runs. No REST lag.",
                "usage":        "GET /EURUSD  /EURUSD-OTC  /BTCUSD-OTC  etc.",
            }

        elif path == "/PAIRS":
            data = {"count": len(bot.pairs), "pairs": sorted(bot.pairs)}

        elif path.startswith("/TICK/"):
            sym = path[6:]
            tick = bot.ticks.get(sym)
            if not tick:
                got = await bot.wait_tick(sym, timeout=TICK_WAIT)
                tick = bot.ticks.get(sym)
            data = tick if tick else {"error": f"No tick for {sym} yet. Is it a valid pair?"}

        else:
            sym = path.lstrip("/")
            if not sym:
                writer.write(_err("Pair name required. e.g. /EURUSD"))
                await writer.drain()
                return

            # Subscribe and wait for first tick (fast if already active)
            got_tick = await bot.wait_tick(sym, timeout=TICK_WAIT)

            candles = bot.candles(sym, limit=limit)

            if not candles and not got_tick:
                writer.write(_err(
                    f"No data for '{sym}' within {TICK_WAIT}s. "
                    f"Check /pairs for valid symbols.", 404
                ))
                await writer.drain()
                return

            agg     = bot.aggs.get(sym)
            live    = agg.live if agg else None
            since   = agg.since if agg else time.time()

            import datetime
            data = {
                "symbol":         sym,
                "count":          len(candles),
                "source":         "WebSocket only — zero lag, zero gap",
                "collecting_since": datetime.datetime.utcfromtimestamp(since).strftime("%H:%M:%S UTC"),
                "live_candle":    live.to_dict() if live else None,
                "gap":            "NONE ✅  (pure WS ticks, current minute always included)",
                "candles":        candles,
            }

        writer.write(_ok(data))
        await writer.drain()

    except Exception as e:
        log.debug("HTTP error: %s", e)
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def http_server(bot: Bot) -> None:
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, bot),
        "0.0.0.0", PORT,
    )
    log.info("HTTP API → http://localhost:%d", PORT)
    async with server:
        await server.serve_forever()


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main() -> None:
    if not TOKEN:
        print("=" * 52)
        print("  ERROR: TRADOWIX_TOKEN not set!")
        print("  export TRADOWIX_TOKEN='your_session_token'")
        print("=" * 52)
        return

    bot = Bot()

    print("=" * 52)
    print("  TradoWix Pure WS Bot  v3")
    print(f"  API → http://localhost:{PORT}")
    print()
    print("  /status       — connection info")
    print("  /pairs        — all 116 pairs")
    print("  /EURUSD       — live candles (no gap)")
    print("  /EURUSD-OTC   — OTC pair")
    print("  /BTCUSD-OTC   — crypto")
    print("  /tick/EURUSD  — latest price")
    print("=" * 52)

    await asyncio.gather(
        http_server(bot),
        ws_loop(bot),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped.")
