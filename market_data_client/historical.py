from __future__ import annotations

"""
HistoricalDataClient
====================
Fetches historical OHLCV candle data from the TradoWix REST endpoint:

    GET https://tradowix.com/api/chart/candles
        ?symbol=EURUSD
        &timeframe=60          ← always 60 (seconds) — only 1-minute candles available
        &count=200             ← max 200 per request
        &offset=0              ← for pagination

Authentication: Cookie  oauth_session_token=<TOKEN>

Discovered limits (live-tested):
 - Max ~200 candles of recent history per symbol (server-side rolling window)
 - Only timeframe=60 (1-minute) returns data
 - Available symbols: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD,
                      EURGBP, EURJPY, GBPJPY, ETHUSD, EURUSD-OTC, GBPUSD-OTC
"""

import asyncio
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass
from typing import Optional

from .models import Candle

logger = logging.getLogger(__name__)

BASE_URL = "https://tradowix.com/api/chart/candles"
TIMEFRAME_SECONDS = 60          # only value the server returns data for
PAGE_SIZE = 200                 # server maximum
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Symbols confirmed to have live candle data
AVAILABLE_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD",
    "USDCAD", "EURGBP", "EURJPY", "GBPJPY", "ETHUSD",
    "EURUSD-OTC", "GBPUSD-OTC",
]


def _candle_from_api(data: dict) -> Candle:
    """Parse one candle dict returned by the REST endpoint."""
    timeframe_sec = int(data.get("timeframe", 60))
    timeframe_min = max(1, timeframe_sec // 60)
    return Candle(
        symbol=data["symbol"],
        timeframe_minutes=timeframe_min,
        open_time=float(data["t"]),
        open=float(data["o"]),
        high=float(data["h"]),
        low=float(data["l"]),
        close=float(data["c"]),
        volume=0,                           # REST endpoint does not expose tick count
        is_closed=bool(data.get("isClosed", True)),
    )


def _fetch_sync(url: str, token: str) -> dict:
    """Blocking HTTP GET — always run inside an executor."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Cookie": f"oauth_session_token={token}",
            "Accept": "application/json",
            "Referer": "https://tradowix.com/trading",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


class HistoricalDataClient:
    """
    Async client for fetching historical 1-minute OHLCV candle data.

    Usage
    -----
    client = HistoricalDataClient(token=os.environ["TRADOWIX_TOKEN"])
    candles = await client.fetch_candles("EURUSD")

    # Fetch all available symbols
    all_data = await client.fetch_all_symbols()
    """

    def __init__(
        self,
        token: str = "",
        output_dir: str = "data",
        request_timeout: float = 15.0,
    ):
        self.token = token or os.environ.get("TRADOWIX_TOKEN", "")
        self.output_dir = output_dir
        self.request_timeout = request_timeout
        os.makedirs(output_dir, exist_ok=True)

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _get_page(self, symbol: str, count: int, offset: int) -> dict:
        """Fetch one page of candle data asynchronously."""
        params = urllib.parse.urlencode({
            "symbol": symbol,
            "timeframe": TIMEFRAME_SECONDS,
            "count": count,
            "offset": offset,
        })
        url = f"{BASE_URL}?{params}"
        loop = asyncio.get_event_loop()
        logger.debug("GET %s", url)
        return await loop.run_in_executor(None, _fetch_sync, url, self.token)

    # ── Public API ────────────────────────────────────────────────────────────

    async def fetch_candles(
        self,
        symbol: str,
        max_candles: int = 200,
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> list[Candle]:
        """
        Fetch up to *max_candles* historical 1-minute candles for *symbol*.

        Returns a list sorted oldest → newest.
        The server keeps a rolling window of the last ~200 minutes.

        Parameters
        ----------
        symbol      : Trading symbol, e.g. "EURUSD"
        max_candles : Maximum candles to return (server cap is 200)
        retries     : Number of retry attempts on timeout/error
        retry_delay : Seconds to wait between retries
        """
        if not self.token:
            raise ValueError("TRADOWIX_TOKEN not set")

        count = min(max_candles, PAGE_SIZE)
        last_exc: Exception = RuntimeError("no attempts made")

        for attempt in range(1, retries + 1):
            try:
                data = await self._get_page(symbol, count, 0)
                raw_candles = data.get("candles", [])
                if not raw_candles:
                    logger.info("%s: no candle data available", symbol)
                    return []

                candles = [_candle_from_api(c) for c in raw_candles]
                candles.sort(key=lambda c: c.open_time)

                logger.info(
                    "%s: fetched %d candles  [%s → %s]",
                    symbol,
                    len(candles),
                    _fmt_ts(candles[0].open_time),
                    _fmt_ts(candles[-1].open_time),
                )
                return candles
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    logger.warning(
                        "%s: attempt %d/%d failed (%s) — retrying in %.0fs",
                        symbol, attempt, retries, exc, retry_delay,
                    )
                    await asyncio.sleep(retry_delay)

        raise last_exc

    async def fetch_all_symbols(
        self,
        symbols: Optional[list[str]] = None,
        max_candles: int = 200,
        delay_between: float = 1.0,
    ) -> dict[str, list[Candle]]:
        """
        Fetch candles for multiple symbols sequentially (avoids rate-limit triggers).

        Parameters
        ----------
        symbols         : List of symbols (defaults to AVAILABLE_SYMBOLS)
        max_candles     : Max candles per symbol
        delay_between   : Seconds to wait between each symbol request

        Returns
        -------
        dict mapping symbol → list[Candle]  (only symbols with data)
        """
        syms = symbols or AVAILABLE_SYMBOLS
        out: dict[str, list[Candle]] = {}

        for i, sym in enumerate(syms):
            if i > 0 and delay_between > 0:
                await asyncio.sleep(delay_between)
            try:
                result = await self.fetch_candles(sym, max_candles)
                if result:
                    out[sym] = result
            except Exception as exc:
                logger.warning("%s: fetch failed — %s", sym, exc)

        return out

    # ── CSV export ────────────────────────────────────────────────────────────

    def save_to_csv(self, candles: list[Candle], symbol: str) -> str:
        """
        Write candles to CSV and return the file path.

        File: <output_dir>/history_<SYMBOL>_1m.csv
        Columns: symbol, timeframe_min, open_time_ms, datetime_utc, open, high, low, close, is_closed
        """
        import csv
        import datetime

        if not candles:
            return ""

        safe_sym = symbol.replace("/", "_").replace("-", "_").replace(":", "_")
        path = os.path.join(self.output_dir, f"history_{safe_sym}_1m.csv")

        with open(path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "symbol", "timeframe_min", "open_time_ms", "datetime_utc",
                "open", "high", "low", "close", "is_closed",
            ])
            for c in candles:
                dt_utc = datetime.datetime.utcfromtimestamp(c.open_time / 1000).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                writer.writerow([
                    c.symbol,
                    c.timeframe_minutes,
                    int(c.open_time),
                    dt_utc,
                    round(c.open, 10),
                    round(c.high, 10),
                    round(c.low, 10),
                    round(c.close, 10),
                    c.is_closed,
                ])

        logger.info("Saved %d candles → %s", len(candles), path)
        return path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_ts(ts_ms: float) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")
