#!/usr/bin/env python3
"""
TradoWix Bot  v4  — REST history + WS live, zero gap
======================================================
Exact same strategy as the production chart server:

  1. REST  → 200 historical candles (fresh every request, cache-busted)
  2. WS    → collects live ticks, builds closed candles to FILL the REST lag gap
  3. WS    → current open candle  (the "missing" latest minute)
  4. Merge → REST + gap-fill closed candles + open candle = complete, no gap

Install:
    pip install websockets

Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

Endpoints:
    /EURUSD              200 historical + live latest candle
    /EURUSD?limit=50     last 50 candles
    /EURUSD-OTC          OTC pair (REST lag ~10-40 min, WS fills gap over time)
    /status              connection + subscription info
    /pairs               all available pairs
    /tick/EURUSD         latest live price
"""

from __future__ import annotations
import asyncio, json, logging, os, time, urllib.parse, urllib.request
from dataclasses import dataclass
from typing import Optional

# ── Config ───────────────────────────────────────────────────────────────────

TOKEN = os.environ.get("TRADOWIX_TOKEN", "")
WS_URL   = "wss://api.tradowix.com/ws"
REST_URL = "https://tradowix.com/api/chart/candles"
PORT     = int(os.environ.get("PORT", "8765"))
TICK_WAIT_SEC   = 8.0    # wait this long for first WS tick when subscribing
MAX_WS_CANDLES  = 180    # keep at most N closed candles per pair from WS

# Subscribe this pair on startup so it accumulates live candles from the beginning
WARMUP_PAIR = "EURUSD"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ── Models ────────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    symbol: str
    tf_sec: int
    t:      float    # open_time ms
    o:      float
    h:      float
    l:      float
    c:      float
    closed: bool

    def to_dict(self) -> dict:
        return {
            "time":  _fmt_utc5(self.t),
            "open":  round(self.o, 10),
            "high":  round(self.h, 10),
            "low":   round(self.l, 10),
            "close": round(self.c, 10),
        }


def _fmt_utc5(ts_ms: float) -> str:
    import datetime
    utc5 = datetime.datetime.utcfromtimestamp(ts_ms / 1000) + datetime.timedelta(hours=5)
    return utc5.strftime("%Y-%m-%d %H:%M:%S")


# ── Tick Aggregator ───────────────────────────────────────────────────────────

class Agg:
    """
    Feeds on live WS ticks → builds closed + open candles.
    Closed candles are used to fill the REST lag gap.
    Open candle is the always-live current minute.
    """

    def __init__(self, sym: str, tf_sec: int = 60):
        self.sym    = sym
        self.tf_sec = tf_sec
        self._ms    = tf_sec * 1000
        self._cur:  Optional[Candle] = None
        self.closed: list[Candle]    = []   # newest at end

    def _period(self, ts: float) -> float:
        return (ts // self._ms) * self._ms

    def feed(self, price: float, ts: float) -> None:
        p = self._period(ts)

        if self._cur is None:
            self._cur = Candle(self.sym, self.tf_sec, p, price, price, price, price, False)

        elif p > self._cur.t:
            # Minute rolled — archive current as closed
            self._cur.closed = True
            self.closed.append(self._cur)
            if len(self.closed) > MAX_WS_CANDLES:
                self.closed.pop(0)
            self._cur = Candle(self.sym, self.tf_sec, p, price, price, price, price, False)

        else:
            self._cur.h = max(self._cur.h, price)
            self._cur.l = min(self._cur.l, price)
            self._cur.c = price

    @property
    def open_candle(self) -> Optional[Candle]:
        """Current (live) candle — is_closed=False, latest minute."""
        return self._cur


# ── REST fetch ────────────────────────────────────────────────────────────────

def _rest_sync(sym: str, count: int) -> list[Candle]:
    """Blocking REST fetch — runs in executor. Always cache-busted."""
    params = urllib.parse.urlencode({
        "symbol":    sym,
        "timeframe": 60,
        "count":     count,
        "_t":        int(time.time() * 1000),   # cache buster
    })
    req = urllib.request.Request(
        f"{REST_URL}?{params}",
        headers={
            "User-Agent":    UA,
            "Cookie":        f"session-token={TOKEN}; oauth_session_token={TOKEN}",
            "Accept":        "application/json",
            "Referer":       "https://tradowix.com/trading",
            "Cache-Control": "no-cache, no-store",
            "Pragma":        "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        body = json.loads(r.read())

    now_ms = time.time() * 1000
    out: list[Candle] = []
    for d in body.get("candles", []):
        t = float(d["t"])
        # isClosed fix: if candle period hasn't ended yet, it's still open
        is_closed = bool(d.get("isClosed", True)) if (t + 60_000 <= now_ms) else False
        out.append(Candle(
            symbol=d.get("symbol", sym),
            tf_sec=int(d.get("timeframe", 60)),
            t=t,
            o=float(d["o"]), h=float(d["h"]),
            l=float(d["l"]), c=float(d["c"]),
            closed=is_closed,
        ))

    out.sort(key=lambda c: c.t)
    return out


async def fetch_rest(sym: str, count: int = 200) -> list[Candle]:
    loop = asyncio.get_event_loop()
    candles = await loop.run_in_executor(None, _rest_sync, sym, count)
    log.info("REST %-22s %d candles", sym, len(candles))
    return candles


# ── Merge logic ───────────────────────────────────────────────────────────────

def merge(rest: list[Candle], agg: Optional[Agg], limit: int) -> list[dict]:
    """
    Merge REST history + WS gap-fill + WS open candle.

    REST is the base (200 historical candles).
    WS closed candles newer than last REST candle fill the REST lag gap.
    WS open candle (current minute) is always appended last.
    """
    cmap: dict[float, Candle] = {}

    # 1. REST history as base
    for c in rest:
        cmap[c.t] = c

    latest_rest_t = rest[-1].t if rest else 0

    if agg:
        # 2. WS closed candles → fill gap (only those NEWER than last REST candle)
        for c in agg.closed:
            if c.t > latest_rest_t:
                cmap[c.t] = c   # WS overwrites REST if same timestamp

        # 3. WS open candle — always the latest minute, is_closed=False
        oc = agg.open_candle
        if oc:
            if oc.t in cmap:
                # Merge: keep REST OHLC but update close + is_closed from WS
                existing = cmap[oc.t]
                cmap[oc.t] = Candle(
                    symbol=existing.symbol, tf_sec=existing.tf_sec, t=existing.t,
                    o=existing.o,
                    h=max(existing.h, oc.h),
                    l=min(existing.l, oc.l),
                    c=oc.c,
                    closed=False,
                )
            else:
                cmap[oc.t] = oc

    sorted_c = sorted(cmap.values(), key=lambda c: c.t)
    return [c.to_dict() for c in sorted_c[-limit:]]


# ── Bot state ─────────────────────────────────────────────────────────────────

class Bot:
    """
    TradoWix WS streams EXACTLY ONE pair at a time (activeSymbol).
    Strategy:
      - active_sym = currently streaming pair
      - aggs keeps ALL pairs ever requested (data never lost)
      - switching pair: old agg data stays, new pair starts collecting
      - warmup on startup: subscribe WARMUP_PAIR immediately so it accumulates
        closed candles before any user request arrives
    """
    def __init__(self):
        self.aggs:      dict[str, Agg]  = {}
        self.ticks:     dict[str, dict] = {}
        self.pairs:     list[str]       = []
        self.active_sym: Optional[str]  = None   # currently streaming from WS

        self.ws           = None
        self.connected    = False
        self.status       = "starting"
        self.started_at   = time.time()
        self.warmup_done  = False

        # Per-symbol tick events
        self._tick_events: dict[str, asyncio.Event] = {}
        self._sub_lock    = asyncio.Lock()

    def _get_event(self, sym: str) -> asyncio.Event:
        if sym not in self._tick_events:
            self._tick_events[sym] = asyncio.Event()
        return self._tick_events[sym]

    async def _send(self, data: dict) -> None:
        if self.ws:
            try:
                await self.ws.send(json.dumps(data))
            except Exception as e:
                log.warning("WS send error: %s", e)

    async def switch_to(self, sym: str) -> None:
        """Switch WS stream to sym. Old sym's agg data is preserved."""
        async with self._sub_lock:
            if self.active_sym == sym:
                return
            log.info("WS → %s  (was: %s)", sym, self.active_sym or "none")
            self.active_sym = sym
            if sym not in self.aggs:
                self.aggs[sym] = Agg(sym)
            self._get_event(sym).clear()
            await self._send({"type": "subscribe", "symbols": [sym], "timeframe": 1})

    async def wait_tick(self, sym: str, timeout: float = TICK_WAIT_SEC) -> bool:
        """Switch to sym and wait for first tick. Instant if already has open candle."""
        await self.switch_to(sym)

        # Already have live data for this sym? Use it immediately.
        agg = self.aggs.get(sym)
        if agg and agg.open_candle:
            return True

        ev = self._get_event(sym)
        ev.clear()
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            log.warning("No tick for %s in %.0fs", sym, timeout)
            return False

    def on_tick(self, sym: str, price: float, ts: float) -> None:
        if sym not in self.aggs:
            self.aggs[sym] = Agg(sym)
        self.aggs[sym].feed(price, ts)
        self.ticks[sym] = {"symbol": sym, "price": price, "ts_ms": int(ts),
                           "time": _fmt_utc5(ts)}
        if sym in self._tick_events:
            self._tick_events[sym].set()


# ── WebSocket loop ────────────────────────────────────────────────────────────

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
                            await bot._send({
                                "type": "subscribe",
                                "symbols": [bot.active_sym],
                                "timeframe": 1,
                            })

                    elif t == "instruments":
                        bot.pairs = [
                            d["symbol"] for d in msg.get("data", [])
                            if d.get("symbol") and d.get("isOpen", True)
                        ]
                        log.info("Instruments: %d open pairs", len(bot.pairs))
                        # Warmup: subscribe one common pair immediately on first connect
                        # so its candles start accumulating before any user request
                        if not bot.warmup_done and WARMUP_PAIR in bot.pairs:
                            bot.warmup_done = True
                            await bot.switch_to(WARMUP_PAIR)
                            log.info("Warmup: subscribed %s — accumulating candles", WARMUP_PAIR)

                    elif t == "subscribed":
                        d = msg.get("data", {})
                        log.info("Active: %s  ok=%s  failed=%s",
                                 d.get("activeSymbol"), d.get("subscribed", []),
                                 d.get("failed", []))

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
                        log.error("Server: %s", (msg.get("data") or {}).get("error", msg))

        except ConnectionClosed as e:
            log.warning("Disconnected: %s — retry in 3s", e)
        except Exception as e:
            log.error("WS error: %s — retry in 3s", e)
        finally:
            bot.connected = False
            bot.ws        = None
            bot.status    = "reconnecting"

        await asyncio.sleep(3)


# ── HTTP Server ────────────────────────────────────────────────────────────────

_BROWSER_JUNK = {
    "/FAVICON.ICO", "/ROBOTS.TXT", "/SITEMAP.XML",
    "/APPLE-TOUCH-ICON.PNG", "/APPLE-TOUCH-ICON-PRECOMPOSED.PNG",
    "/MANIFEST.JSON", "/.WELL-KNOWN",
}


def _resp(data, status: int = 200) -> bytes:
    body = json.dumps(data, ensure_ascii=False).encode()
    head = (
        f"HTTP/1.1 {status} {'OK' if status == 200 else 'Error'}\r\n"
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

        # Silently ignore browser auto-requests
        if path in _BROWSER_JUNK or path.startswith("/."):
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        # ── /STATUS ────────────────────────────────────────────────────────
        if path in ("/STATUS", "/", ""):
            data = {
                "connected":    bot.connected,
                "status":       bot.status,
                "active_pair":  bot.active_sym,
                "tracking":     sorted(bot.aggs.keys()),
                "pairs_total":  len(bot.pairs),
                "uptime_sec":   round(time.time() - bot.started_at, 1),
                "note":         "TradoWix WS = 1 pair at a time. Pair stays tracked after switch. Gap fills over time.",
            }

        # ── /PAIRS ─────────────────────────────────────────────────────────
        elif path == "/PAIRS":
            data = {"count": len(bot.pairs), "pairs": sorted(bot.pairs)}

        # ── /TICK/<SYM> ────────────────────────────────────────────────────
        elif path.startswith("/TICK/"):
            sym  = path[6:]
            tick = bot.ticks.get(sym)
            if not tick:
                await bot.wait_tick(sym, timeout=TICK_WAIT_SEC)
                tick = bot.ticks.get(sym)
            data = tick if tick else {"error": f"No tick for {sym}. Check /pairs"}

        # ── /<SYMBOL> ──────────────────────────────────────────────────────
        else:
            sym = path.lstrip("/")
            if not sym:
                writer.write(_resp({"error": "Pair required. e.g. /EURUSD"}, 400))
                await writer.drain()
                return

            # Run REST fetch + WS subscribe concurrently for speed
            rest_task = asyncio.create_task(fetch_rest(sym, count=200))
            ws_task   = asyncio.create_task(bot.wait_tick(sym, timeout=TICK_WAIT_SEC))
            rest_candles, got_tick = await asyncio.gather(
                rest_task, ws_task, return_exceptions=False
            )

            if not rest_candles and not got_tick:
                writer.write(_resp({
                    "error": f"No data for '{sym}'. Check /pairs for valid symbols."
                }, 404))
                await writer.drain()
                return

            agg     = bot.aggs.get(sym)
            candles = merge(rest_candles, agg, limit=limit)

            # Gap analysis
            rest_last_t  = rest_candles[-1].t if rest_candles else 0
            ws_open_t    = agg.open_candle.t  if (agg and agg.open_candle) else 0
            ws_closed_n  = len([c for c in (agg.closed if agg else []) if c.t > rest_last_t])
            # Total gap = minutes between last REST candle and current live candle
            total_gap_min = round((ws_open_t - rest_last_t) / 60_000) if (rest_last_t and ws_open_t) else None
            remain_gap    = max(0, (total_gap_min or 0) - ws_closed_n)

            if not got_tick:
                verdict = "WAITING FOR FIRST TICK ⏳"
            elif total_gap_min is None or total_gap_min <= 1:
                verdict = "NO GAP ✅"
            elif ws_closed_n == 0:
                # Just subscribed — gap exists, will fill over time
                verdict = f"GAP {total_gap_min}min — bot needs ~{total_gap_min} more minutes to fill (WS collecting)"
            elif remain_gap <= 0:
                verdict = "NO GAP ✅ (WS filled all missing candles)"
            else:
                verdict = f"FILLING: {ws_closed_n}/{total_gap_min} candles filled — {remain_gap}min remaining"

            data = candles

        writer.write(_resp(data))
        await writer.drain()

    except Exception as e:
        log.debug("HTTP error: %s", e)
        try:
            writer.write(_resp({"error": "Internal error"}, 500))
            await writer.drain()
        except Exception:
            pass
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
    log.info("HTTP → http://localhost:%d", PORT)
    async with server:
        await server.serve_forever()


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main() -> None:
    if not TOKEN:
        print("ERROR: TRADOWIX_TOKEN not set!")
        print("  export TRADOWIX_TOKEN='your_session_token'")
        return

    bot = Bot()

    print("=" * 52)
    print("  TradoWix Bot  v4")
    print(f"  API → http://localhost:{PORT}")
    print()
    print("  /EURUSD          200 history + live latest")
    print("  /EURUSD-OTC      OTC pair")
    print("  /BTCUSD-OTC      crypto")
    print("  /EURUSD?limit=50 last 50 candles")
    print("  /tick/EURUSD     live price")
    print("  /status          bot info")
    print("  /pairs           all 116 pairs")
    print("=" * 52)

    await asyncio.gather(http_server(bot), ws_loop(bot))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
