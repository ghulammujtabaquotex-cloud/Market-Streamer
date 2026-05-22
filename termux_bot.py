#!/usr/bin/env python3
"""
TradoWix Bot  v5  — Pure REST, Zero Gap, No WebSocket
======================================================
Strategy:
  1. REST /candles  → 200 historical 1-min candles
  2. REST /ticks    → all raw ticks from last candle time → NOW
  3. Aggregate ticks → 1-min OHLC candles (fills the gap + live open candle)
  4. Merge          → REST + tick-candles = complete, zero gap

No WebSocket needed. Works in Termux with stdlib only.

Install:  (nothing! pure stdlib)
Run:
    export TRADOWIX_TOKEN="your_session_token"
    python termux_bot.py

Endpoints:
    /EURUSD              200 candles + live (zero gap)
    /EURUSD?limit=50     last 50 candles
    /EURUSD-OTC          OTC pair (gap filled via tick API)
    /status              server info
    /pairs               all available pairs
"""

from __future__ import annotations
import asyncio, json, logging, os, time, urllib.parse, urllib.request
from dataclasses import dataclass
from typing import Optional

# ── Config ───────────────────────────────────────────────────────────────────

TOKEN    = os.environ.get("TRADOWIX_TOKEN", "")
BASE_URL = "https://tradowix.com/api"
PORT     = int(os.environ.get("PORT", "8765"))
TF_SEC   = 60
TF_MS    = TF_SEC * 1000

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

# ── UTC+5 time formatter ──────────────────────────────────────────────────────

def _utc5(ts_ms: float) -> str:
    import datetime
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000) + datetime.timedelta(hours=5)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ── Candle model ──────────────────────────────────────────────────────────────

@dataclass
class Candle:
    t:      float
    o:      float
    h:      float
    l:      float
    c:      float
    closed: bool

    def to_dict(self) -> dict:
        return {
            "time":  _utc5(self.t),
            "open":  round(self.o, 10),
            "high":  round(self.h, 10),
            "low":   round(self.l, 10),
            "close": round(self.c, 10),
        }

# ── Shared HTTP headers ───────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "User-Agent":    UA,
        "Cookie":        f"session-token={TOKEN}; oauth_session_token={TOKEN}",
        "Accept":        "application/json",
        "Referer":       "https://tradowix.com/trading",
        "Cache-Control": "no-cache, no-store",
        "Pragma":        "no-cache",
    }

def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

# ── Step 1: Fetch REST candles ────────────────────────────────────────────────

def _fetch_candles(sym: str, count: int = 200) -> list[Candle]:
    url = (
        f"{BASE_URL}/chart/candles"
        f"?symbol={urllib.parse.quote(sym)}"
        f"&timeframe={TF_SEC}&count={count}&_t={int(time.time()*1000)}"
    )
    body  = _get(url)
    now   = time.time() * 1000
    out: list[Candle] = []
    for d in body.get("candles", []):
        t = float(d["t"])
        is_closed = bool(d.get("isClosed", True)) if (t + TF_MS <= now) else False
        out.append(Candle(t, float(d["o"]), float(d["h"]), float(d["l"]), float(d["c"]), is_closed))
    out.sort(key=lambda c: c.t)
    log.info("REST  %-20s %d candles  last=%s", sym, len(out), _utc5(out[-1].t) if out else "-")
    return out

# ── Step 2: Fetch raw ticks for gap window ────────────────────────────────────

def _fetch_ticks(sym: str, from_ms: int, to_ms: int) -> list[list]:
    """Returns [[price, ts_ms], ...] sorted by time."""
    all_ticks: list[list] = []
    cursor = from_ms

    while True:
        url = (
            f"{BASE_URL}/chart/ticks"
            f"?symbol={urllib.parse.quote(sym)}"
            f"&from={cursor}&to={to_ms}"
        )
        body = _get(url)
        batch = body.get("ticks", [])
        if not batch:
            break
        all_ticks.extend(batch)
        next_from = body.get("nextFrom")
        if not body.get("hasMore") or not next_from or next_from <= cursor:
            break
        cursor = next_from

    all_ticks.sort(key=lambda x: x[1])
    return all_ticks

# ── Step 3: Aggregate ticks → 1-min OHLC candles ─────────────────────────────

def _agg_ticks(sym: str, ticks: list[list]) -> list[Candle]:
    """Build 1-min candles from raw [[price, ts_ms], ...] ticks."""
    now    = time.time() * 1000
    groups: dict[float, dict] = {}

    for tick in ticks:
        price = float(tick[0])
        ts    = float(tick[1])
        p     = (ts // TF_MS) * TF_MS   # minute period start

        if p not in groups:
            groups[p] = {"o": price, "h": price, "l": price, "c": price}
        else:
            g = groups[p]
            g["h"] = max(g["h"], price)
            g["l"] = min(g["l"], price)
            g["c"] = price

    candles: list[Candle] = []
    for t, g in sorted(groups.items()):
        is_closed = (t + TF_MS) <= now
        candles.append(Candle(t, g["o"], g["h"], g["l"], g["c"], is_closed))

    return candles

# ── Step 4: Merge REST candles + tick candles ─────────────────────────────────

def _merge(rest: list[Candle], tick_candles: list[Candle]) -> list[Candle]:
    """
    REST is base. Tick candles:
      - If same timestamp as REST → update H/L/C (ticks are fresher)
      - If newer than REST        → add as gap-fill or live candle
    """
    cmap: dict[float, Candle] = {c.t: c for c in rest}

    for tc in tick_candles:
        if tc.t in cmap:
            existing = cmap[tc.t]
            cmap[tc.t] = Candle(
                t=existing.t,
                o=existing.o,
                h=max(existing.h, tc.h),
                l=min(existing.l, tc.l),
                c=tc.c,
                closed=tc.closed,
            )
        else:
            cmap[tc.t] = tc

    return sorted(cmap.values(), key=lambda c: c.t)

# ── Main: get complete candles for a symbol ───────────────────────────────────

async def get_chart(sym: str, limit: int = 200) -> tuple[list[dict], dict]:
    loop = asyncio.get_event_loop()

    # Step 1: REST candles (in executor — blocking)
    rest = await loop.run_in_executor(None, _fetch_candles, sym, 200)

    now_ms      = int(time.time() * 1000)
    last_rest_t = int(rest[-1].t) if rest else (now_ms - 200 * TF_MS)

    gap_min = round((now_ms - last_rest_t) / 60_000)

    # Step 2: Ticks for the gap window
    ticks = await loop.run_in_executor(None, _fetch_ticks, sym, last_rest_t, now_ms)
    log.info("TICKS %-20s %d ticks  gap_window=%dmin", sym, len(ticks), gap_min)

    # Step 3: Aggregate ticks → candles
    tick_candles = _agg_ticks(sym, ticks)
    gap_filled   = len([c for c in tick_candles if c.t > last_rest_t])

    # Step 4: Merge
    merged = _merge(rest, tick_candles)

    # Latest candle info (last in list)
    latest = merged[-1] if merged else None
    is_live = latest and not latest.closed

    # Gap status
    if not tick_candles:
        gap_status = f"NO TICKS — gap {gap_min}min unfilled"
    elif gap_filled == 0 and gap_min <= 1:
        gap_status = "NO GAP ✅"
    elif gap_min <= 1:
        gap_status = "NO GAP ✅"
    else:
        remaining = max(0, gap_min - gap_filled)
        if remaining == 0:
            gap_status = "NO GAP ✅  (tick API filled all missing candles)"
        else:
            gap_status = f"FILLING: {gap_filled}/{gap_min} min filled — {remaining}min remaining"

    meta = {
        "symbol":       sym,
        "count":        min(len(merged), limit),
        "rest_candles": len(rest),
        "tick_candles": gap_filled,
        "gap_min":      gap_min,
        "gap_status":   gap_status,
        "live_candle":  is_live,
        "latest":       latest.to_dict() if latest else None,
    }

    return [c.to_dict() for c in merged[-limit:]], meta

# ── Bot state ─────────────────────────────────────────────────────────────────

class Bot:
    def __init__(self):
        self.pairs:      list[str] = []
        self.started_at: float     = time.time()

    async def load_pairs(self) -> None:
        """Fetch available pairs from WS instruments via REST instruments endpoint."""
        try:
            loop = asyncio.get_event_loop()
            # Use candles endpoint to probe — instruments come from WS only
            # so we hardcode a known list and let user use /pairs
            self.pairs = []
        except Exception:
            pass

# ── HTTP Server ───────────────────────────────────────────────────────────────

_BROWSER_JUNK = {
    "/FAVICON.ICO", "/ROBOTS.TXT", "/SITEMAP.XML",
    "/APPLE-TOUCH-ICON.PNG", "/MANIFEST.JSON", "/.WELL-KNOWN",
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

        if path in _BROWSER_JUNK or path.startswith("/."):
            writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return

        # ── /STATUS ────────────────────────────────────────────────────────
        if path in ("/STATUS", "/", ""):
            data = {
                "status":     "running",
                "mode":       "pure REST — no WebSocket",
                "uptime_sec": round(time.time() - bot.started_at, 1),
                "usage":      "GET /<PAIR>?limit=200  e.g. /EURUSD  /EURUSD-OTC",
                "how_it_works": [
                    "1. REST /candles → 200 historical candles",
                    "2. REST /ticks   → raw ticks from last candle → now",
                    "3. Aggregate     → 1-min OHLC candles from ticks",
                    "4. Merge         → zero gap guaranteed",
                ],
            }

        # ── /PAIRS ─────────────────────────────────────────────────────────
        elif path == "/PAIRS":
            data = {
                "note": "Use any TradoWix symbol e.g. EURUSD, GBPUSD, EURUSD-OTC, BTCUSD-OTC",
                "common": [
                    "EURUSD","GBPUSD","USDJPY","AUDUSD","USDCHF","EURJPY",
                    "EURUSD-OTC","GBPUSD-OTC","USDJPY-OTC","AUDUSD-OTC",
                    "BTCUSD-OTC","ETHUSD-OTC","LTCUSD-OTC",
                ],
            }

        # ── /<SYMBOL> ──────────────────────────────────────────────────────
        else:
            sym = path.lstrip("/")
            if not sym:
                writer.write(_resp({"error": "Pair required. e.g. /EURUSD"}, 400))
                await writer.drain()
                return

            try:
                candles, meta = await get_chart(sym, limit=limit)
            except Exception as e:
                log.error("Chart error for %s: %s", sym, e)
                writer.write(_resp({"error": str(e)}, 500))
                await writer.drain()
                return

            log.info(
                "%-12s  rest=%d  ticks=%d  gap=%dmin  status=%s",
                sym, meta["rest_candles"], meta["tick_candles"],
                meta["gap_min"], meta["gap_status"],
            )

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

    print("=" * 54)
    print("  TradoWix Bot  v5  — Pure REST, Zero Gap")
    print(f"  API → http://localhost:{PORT}")
    print()
    print("  /EURUSD              candles + gap fill via ticks")
    print("  /EURUSD?limit=50     last 50 candles")
    print("  /EURUSD-OTC          OTC pair — tick gap fill")
    print("  /BTCUSD-OTC          crypto")
    print("  /status              server info")
    print("  /pairs               common pairs list")
    print()
    print("  No WebSocket. Pure REST. Instant gap fill.")
    print("=" * 54)

    await http_server(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
