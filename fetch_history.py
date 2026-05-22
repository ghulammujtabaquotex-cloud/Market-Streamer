"""
fetch_history.py
================
Fetch historical 1-minute OHLCV candle data from TradoWix and save to CSV.

Usage
-----
    python fetch_history.py                     # all available symbols
    python fetch_history.py EURUSD              # single symbol
    python fetch_history.py EURUSD GBPUSD USDJPY  # multiple symbols

Output
------
    data/history_EURUSD_1m.csv
    data/history_GBPUSD_1m.csv
    ...

Each CSV row: symbol, timeframe_min, open_time_ms, datetime_utc, open, high, low, close, is_closed

Notes
-----
- Server returns last ~200 minutes of 1-minute candles (rolling window)
- Available symbols: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD,
                     EURGBP, EURJPY, GBPJPY, ETHUSD, EURUSD-OTC, GBPUSD-OTC
- Requires TRADOWIX_TOKEN env var (your oauth_session_token cookie value)
"""

import asyncio
import logging
import os
import sys

from market_data_client.historical import HistoricalDataClient, AVAILABLE_SYMBOLS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    token = os.environ.get("TRADOWIX_TOKEN", "")
    if not token:
        print("ERROR: TRADOWIX_TOKEN environment variable is not set.")
        print("  export TRADOWIX_TOKEN=your_oauth_session_token")
        sys.exit(1)

    # Determine symbols to fetch
    if len(sys.argv) > 1:
        symbols = [s.upper() for s in sys.argv[1:]]
    else:
        symbols = AVAILABLE_SYMBOLS

    print(f"\nTradoWix Historical Candle Fetcher")
    print(f"===================================")
    print(f"Symbols  : {', '.join(symbols)}")
    print(f"Timeframe: 1 minute (60s)")
    print(f"Max bars : 200 per symbol (server rolling window)\n")

    client = HistoricalDataClient(token=token, output_dir="data")

    results = await client.fetch_all_symbols(symbols=symbols, max_candles=200)

    if not results:
        print("\nNo candle data received. Check your token or try again later.")
        sys.exit(1)

    print(f"\nResults:")
    print(f"{'Symbol':<16} {'Candles':>7}  {'From (UTC)':<20} {'To (UTC)':<20}  {'CSV File'}")
    print("-" * 95)

    saved_files = []
    for symbol, candles in sorted(results.items()):
        from_dt = _ts_str(candles[0].open_time)
        to_dt   = _ts_str(candles[-1].open_time)
        csv_path = client.save_to_csv(candles, symbol)
        saved_files.append(csv_path)
        print(f"{symbol:<16} {len(candles):>7}  {from_dt:<20} {to_dt:<20}  {csv_path}")

    print(f"\n✓ Saved {len(saved_files)} files to data/")
    print()

    # Print a sample of the most recent candles for the first symbol
    first_sym = next(iter(results))
    candles = results[first_sym]
    last5 = candles[-5:]
    print(f"Sample — last 5 candles for {first_sym}:")
    print(f"  {'datetime_utc':<22} {'open':>10} {'high':>10} {'low':>10} {'close':>10}")
    for c in last5:
        dt = _ts_str(c.open_time)
        print(f"  {dt:<22} {c.open:>10.5f} {c.high:>10.5f} {c.low:>10.5f} {c.close:>10.5f}")
    print()


def _ts_str(ts_ms: float) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    asyncio.run(main())
