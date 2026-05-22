"""
Final live test using the updated MarketDataClient with correct TradoWix protocol.
"""

import asyncio
import logging
import os

from market_data_client import ClientConfig, MarketDataClient
from market_data_client.models import Candle, Tick

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

TOKEN = os.environ.get("TRADOWIX_TOKEN", "")

received_ticks   = []
received_candles = []

def on_tick(tick: Tick):
    received_ticks.append(tick)
    print(f"  TICK  {tick.symbol}  {tick.price:.5f}  ts={tick.timestamp}")

def on_candle(candle: Candle):
    received_candles.append(candle)
    print(f"\n  CANDLE  {candle.symbol}/{candle.timeframe_minutes}m"
          f"  O={candle.open:.5f} H={candle.high:.5f} L={candle.low:.5f} C={candle.close:.5f}"
          f"  ticks={candle.volume}  closed={candle.is_closed}")


async def main():
    if not TOKEN:
        raise SystemExit("Set TRADOWIX_TOKEN")

    cfg = ClientConfig(
        token=TOKEN,
        csv_dir="data_test",
        reconnect_enabled=False,   # single run
    )

    print(f"\n{'='*60}")
    print(f"  Final Live Test — MarketDataClient (correct protocol)")
    print(f"  Token: {TOKEN[:12]}...{TOKEN[-4:]}")
    print(f"{'='*60}\n")

    stop = asyncio.Event()

    async with MarketDataClient(cfg) as client:
        print("✓ Connected & authenticated!\n")

        # Wait a moment for instruments to arrive
        await asyncio.sleep(1.2)

        if client.instruments:
            print(f"  Instruments loaded: {len(client.instruments)}")
            for sym in ["EURUSD", "BTCUSDT", "USDPKR-OTC"]:
                if sym in client.instruments:
                    i = client.instruments[sym]
                    print(f"  {sym:15s} precision={i.precision}  isOTC={i.is_otc}  isOpen={i.is_open}")
            print()

        # Subscribe EURUSD 1-minute candles
        await client.subscribe(
            "EURUSD",
            timeframe_minutes=1,
            on_tick=on_tick,
            on_candle=on_candle,
            export_ticks=True,
            export_candles=True,
        )
        print("Subscribed to EURUSD / 1m\n")

        # Wait up to 20 s for ticks then stop
        async def _stop_after():
            await asyncio.sleep(20)
            stop.set()

        asyncio.create_task(_stop_after())
        await stop.wait()

    # ── Summary ──────────────────────────────────────────────────────────────
    from market_data_client import CsvExporter
    exp = CsvExporter("data_test")
    files = exp.list_files()

    print(f"\n{'='*60}  RESULTS")
    print(f"  Ticks received      : {len(received_ticks)}")
    print(f"  Candles completed   : {len(received_candles)}")
    if received_ticks:
        t = received_ticks[-1]
        print(f"  Latest EURUSD price : {t.price}")
    live_candle = client._aggregators.get("EURUSD:1")
    if live_candle and live_candle.current_candle:
        c = live_candle.current_candle
        print(f"  Current open candle : O={c.open} H={c.high} L={c.low} C={c.close}  ticks={c.volume}")
    print(f"  CSV files written   : {len(files)}")
    for f in files:
        print(f"    {f}")
    print()

    # Print first few rows of ticks CSV
    import os as _os
    tick_csv = next((f for f in files if "ticks_EURUSD" in f), None)
    if tick_csv and _os.path.exists(tick_csv):
        with open(tick_csv) as fh:
            lines = fh.readlines()
        print(f"  ticks CSV ({len(lines)-1} rows):")
        for line in lines[:6]:
            print(f"    {line.rstrip()}")


if __name__ == "__main__":
    asyncio.run(main())
