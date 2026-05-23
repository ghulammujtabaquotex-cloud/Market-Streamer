#!/usr/bin/env python3
"""
TradoWix Bot  v5  — Pure REST, Zero Gap
========================================
Strategy (no WebSocket needed):
  1. REST /candles → 200 historical 1-min candles
  2. REST /ticks   → raw ticks from last candle time → NOW (fills gap)
  3. Aggregate     → build 1-min OHLC from ticks
  4. Merge         → REST + tick-candles = complete, zero gap

Install: nothing (pure Python stdlib)
Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

Endpoints:
    /EURUSD              200 candles + live (zero gap)
    /EURUSD?limit=50     last 50 candles
    /EURUSD-OTC          OTC — gap auto-filled via tick API
    /status              server info
    /pairs               available pairs
"""
from __future__ import annotations
import asyncio, json, logging, os, time, urllib.parse, urllib.request
from dataclasses import dataclass
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN    = os.environ.get("TRADOWIX_TOKEN", "")
BASE_URL = "https://tradowix.com/api"
PORT     = int(os.environ.get("PORT", "8765"))
TF_SEC   = 60
TF_MS    = TF_SEC * 1000

# If gap > this, market is considered closed (no ticks expected)
MARKET_CLOSED_MIN = 10

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bot")

# ── UTC+5 formatter ────────────────────────────────────────────────────────────
def _utc5(ts_ms: float) -> str:
    import datetime
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000) + datetime.timedelta(hours=5)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ── Candle ────────────────────────────────────────────────────────────────────
@dataclass
class Candle:
    t: float; o: float; h: float; l: float; c: float; closed: bool

    def to_dict(self) -> dict:
        return {
            "time":  _utc5(self.t),
            "open":  round(self.o, 10),
            "high":  round(self.h, 10),
            "low":   round(self.l, 10),
            "close": round(self.c, 10),
        }

# ── Shared headers ────────────────────────────────────────────────────────────
def _hdrs() -> dict:
    return {
        "User-Agent":    UA,
        "Cookie":        f"session-token={TOKEN}; oauth_session_token={TOKEN}",
        "Accept":        "application/json",
        "Referer":       "https://tradowix.com/trading",
        "Cache-Control": "no-cache, no-store",
        "Pragma":        "no-cache",
    }

def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers=_hdrs())
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── Step 1: REST candles ──────────────────────────────────────────────────────
def _fetch_candles(sym: str) -> list[Candle]:
    url = (f"{BASE_URL}/chart/candles"
           f"?symbol={urllib.parse.quote(sym)}"
           f"&timeframe={TF_SEC}&count=200&_t={int(time.time()*1000)}")
    body = _get(url)
    now  = time.time() * 1000
    out: list[Candle] = []
    for d in body.get("candles", []):
        t  = float(d["t"])
        ic = bool(d.get("isClosed", True)) if (t + TF_MS <= now) else False
        out.append(Candle(t, float(d["o"]), float(d["h"]), float(d["l"]), float(d["c"]), ic))
    out.sort(key=lambda c: c.t)
    return out

# ── Step 2: Raw ticks for gap window ─────────────────────────────────────────
def _fetch_ticks(sym: str, from_ms: int, to_ms: int) -> list:
    """Fetch all ticks with pagination. Returns [[price, ts_ms], ...]"""
    all_ticks: list = []
    cursor = from_ms
    for _ in range(20):  # max 20 pages
        url = (f"{BASE_URL}/chart/ticks"
               f"?symbol={urllib.parse.quote(sym)}"
               f"&from={cursor}&to={to_ms}")
        try:
            body  = _get(url)
            batch = body.get("ticks", [])
            if batch:
                all_ticks.extend(batch)
            nf = body.get("nextFrom")
            if not body.get("hasMore") or not nf or nf <= cursor:
                break
            cursor = nf
        except Exception as e:
            log.warning("Tick page error: %s", e)
            break
    all_ticks.sort(key=lambda x: x[1])
    return all_ticks

# ── Step 3: Aggregate ticks → 1-min candles ──────────────────────────────────
def _agg(sym: str, ticks: list, now_ms: float) -> list[Candle]:
    groups: dict[float, list] = {}
    for tick in ticks:
        price, ts = float(tick[0]), float(tick[1])
        p = (ts // TF_MS) * TF_MS
        if p not in groups:
            groups[p] = [price, price, price, price]   # o h l c
        else:
            g = groups[p]
            if price > g[1]: g[1] = price
            if price < g[2]: g[2] = price
            g[3] = price
    out: list[Candle] = []
    for t, g in sorted(groups.items()):
        closed = (t + TF_MS) <= now_ms
        out.append(Candle(t, g[0], g[1], g[2], g[3], closed))
    return out

# ── Step 4: Merge REST + tick candles ────────────────────────────────────────
def _merge(rest: list[Candle], tick_candles: list[Candle]) -> list[Candle]:
    cmap: dict[float, Candle] = {c.t: c for c in rest}
    for tc in tick_candles:
        if tc.t in cmap:
            ex = cmap[tc.t]
            cmap[tc.t] = Candle(ex.t, ex.o,
                                max(ex.h, tc.h), min(ex.l, tc.l),
                                tc.c, tc.closed)
        else:
            cmap[tc.t] = tc
    return sorted(cmap.values(), key=lambda c: c.t)

# ── Full pipeline ─────────────────────────────────────────────────────────────
async def get_chart(sym: str, limit: int = 200) -> tuple[list[dict], dict]:
    loop    = asyncio.get_event_loop()
    now_ms  = int(time.time() * 1000)

    # Step 1
    rest = await loop.run_in_executor(None, _fetch_candles, sym)
    if not rest:
        return [], {"error": "No REST data"}

    last_rest_t = int(rest[-1].t)
    gap_min     = round((now_ms - last_rest_t) / 60_000)

    # Step 2 — fetch ticks only if gap is reasonable (market open)
    ticks: list = []
    tick_candles: list[Candle] = []
    market_closed = False

    # Always try tick API; if no new ticks & gap large → market closed
    ticks = await loop.run_in_executor(None, _fetch_ticks, sym, last_rest_t, now_ms)
    tick_candles = _agg(sym, ticks, now_ms)

    # Count tick candles that are NEWER than last REST candle
    new_tc = [c for c in tick_candles if c.t > last_rest_t]

    # Market closed = large gap + ticks exist only at REST candle time (no progress)
    all_newer_ticks = [tk for tk in ticks if float(tk[1]) > last_rest_t + TF_MS]
    if gap_min > MARKET_CLOSED_MIN and len(all_newer_ticks) == 0:
        market_closed = True

    # Step 3 — merge
    merged = _merge(rest, tick_candles)

    # ── Gap status ────────────────────────────────────────────────────────────
    our_last_t  = merged[-1].t if merged else 0
    our_gap_min = round((now_ms - our_last_t) / 60_000) if our_last_t else gap_min
    is_live     = merged[-1] and not merged[-1].closed if merged else False

    if market_closed:
        status = f"MARKET CLOSED — last candle {_utc5(last_rest_t)} (gap {gap_min}min, no new ticks)"
    elif our_gap_min <= 1:
        status = "NO GAP ✅  live candle included"
    elif new_tc:
        status = f"FILLING: {len(new_tc)}/{gap_min} min filled — {max(0,gap_min-len(new_tc))}min remaining"
    else:
        status = f"GAP {gap_min}min — waiting for ticks"

    log.info("%-14s  rest=%d  ticks=%d  new_tc=%d  gap=%dmin→%dmin  %s",
             sym, len(rest), len(ticks), len(new_tc), gap_min, our_gap_min, status)

    meta = {
        "rest_candles": len(rest),
        "tick_count":   len(ticks),
        "tick_candles": len(new_tc),
        "gap_before":   gap_min,
        "gap_after":    our_gap_min,
        "gap_status":   status,
        "market_closed": market_closed,
        "live_candle":  is_live,
    }
    return [c.to_dict() for c in merged[-limit:]], meta

# ── HTTP server ───────────────────────────────────────────────────────────────
_JUNK = {"/FAVICON.ICO","/ROBOTS.TXT","/SITEMAP.XML","/MANIFEST.JSON"}

def _resp(data, status: int = 200) -> bytes:
    body = json.dumps(data, ensure_ascii=False).encode()
    head = (
        f"HTTP/1.1 {status} {'OK' if status==200 else 'Error'}\r\n"
        "Content-Type: application/json\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    return head + body

async def handle(reader, writer, started_at: float) -> None:
    try:
        raw = await asyncio.wait_for(reader.read(4096), timeout=10)
        if not raw:
            return
        line   = raw.split(b"\r\n")[0].decode(errors="replace")
        parts  = line.split(" ")
        if len(parts) < 2:
            return
        parsed = urllib.parse.urlparse(parts[1])
        path   = parsed.path.rstrip("/").upper()
        qs     = urllib.parse.parse_qs(parsed.query)
        limit  = int(qs.get("limit", ["200"])[0])

        if path in _JUNK or path.startswith("/."):
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            await writer.drain(); return

        # /STATUS
        if path in ("/STATUS", "/", ""):
            data = {
                "status":     "running",
                "uptime_sec": round(time.time() - started_at, 1),
                "mode":       "Pure REST — Candles + Tick API gap fill",
                "endpoints":  ["/EURUSD", "/EURUSD?limit=50", "/EURUSD-OTC", "/BTCUSD-OTC", "/pairs"],
            }

        # /PAIRS
        elif path == "/PAIRS":
            data = {
                "note": "Pass any TradoWix symbol",
                "regular": ["EURUSD","GBPUSD","USDJPY","AUDUSD","USDCHF","EURJPY","GBPJPY","USDCAD","EURGBP","NZDCAD"],
                "otc":     ["EURUSD-OTC","GBPUSD-OTC","USDJPY-OTC","AUDUSD-OTC","USDCHF-OTC",
                            "EURJPY-OTC","GBPJPY-OTC","USDCAD-OTC","EURGBP-OTC","NZDUSD-OTC"],
                "crypto":  ["BTCUSD-OTC","ETHUSD-OTC","LTCUSD-OTC","XRPUSD-OTC","BNBUSD-OTC"],
            }

        # /<SYMBOL>
        else:
            sym = path.lstrip("/")
            if not sym:
                writer.write(_resp({"error": "Pair required. e.g. /EURUSD"}, 400))
                await writer.drain(); return
            try:
                candles, _meta = await get_chart(sym, limit=limit)
                data = candles
            except Exception as e:
                log.error("Error %s: %s", sym, e)
                writer.write(_resp({"error": str(e)}, 500))
                await writer.drain(); return

        writer.write(_resp(data))
        await writer.drain()
    except Exception as e:
        log.debug("HTTP: %s", e)
        try:
            writer.write(_resp({"error": "Internal error"}, 500))
            await writer.drain()
        except: pass
    finally:
        try: writer.close()
        except: pass

async def main() -> None:
    if not TOKEN:
        print("ERROR: export TRADOWIX_TOKEN='your_token'")
        return

    started_at = time.time()
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, started_at),
        "0.0.0.0", PORT,
    )

    print("=" * 52)
    print("  TradoWix Bot  v5  —  Pure REST, Zero Gap")
    print(f"  http://localhost:{PORT}")
    print()
    print("  /EURUSD          200 candles + live")
    print("  /EURUSD-OTC      gap auto-filled via ticks")
    print("  /BTCUSD-OTC      crypto OTC")
    print("  /EURUSD?limit=50 last 50 candles")
    print("  /status          server info")
    print("  /pairs           pair list")
    print()
    print("  No WebSocket needed.")
    print("=" * 52)
    log.info("HTTP → http://localhost:%d", PORT)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
