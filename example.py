"""
TradoWix Market Data Streaming Client — live example

Connects to wss://api.tradowix.com/ws, streams EURUSD ticks,
aggregates them into 1-minute candles, and exports to CSV.

Run:
    TRADOWIX_TOKEN=<your_token> python example.py
"""

import asyncio
import logging
import os
import signal

from market_data_client import ClientConfig, MarketDataClient
from market_data_client.models import Candle, Tick

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

TOKEN = os.environ.get("TRADOWIX_TOKEN", "")


def on_tick(tick: Tick) -> None:
    print(f"  TICK  {tick.symbol:12s}  price={tick.price:.5f}  ts={tick.timestamp}")


def on_candle(candle: Candle) -> None:
    print(
        f"\n  CANDLE CLOSED  {candle.symbol}/{candle.timeframe_minutes}m"
        f"  O={candle.open:.5f}  H={candle.high:.5f}  L={candle.low:.5f}  C={candle.close:.5f}"
        f"  ticks={candle.volume}"
    )


async def main() -> None:
    if not TOKEN:
        raise SystemExit(
            "\nERROR: TRADOWIX_TOKEN is not set.\n"
            "Usage: TRADOWIX_TOKEN=<token> python example.py\n"
        )

    config = ClientConfig.from_env()

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop.set())

    print(f"\nConnecting to TradoWix…")

    async with MarketDataClient(config) as client:
        print(f"Connected and authenticated!\n")

        # Wait for instruments catalogue (arrives automatically on connect)
        await asyncio.sleep(1)
        if client.instruments:
            eurusd = client.instruments.get("EURUSD")
            if eurusd:
                print(f"  EURUSD: precision={eurusd.precision}  isOTC={eurusd.is_otc}  isOpen={eurusd.is_open}")

        # Subscribe to EURUSD 1-minute candles
        await client.subscribe(
            "EURUSD",
            timeframe_minutes=1,
            on_tick=on_tick,
            on_candle=on_candle,
            export_ticks=True,
            export_candles=True,
        )
        print("\nSubscribed to EURUSD — streaming live ticks…")
        print("(Candles close every 1 minute; press Ctrl+C to stop)\n")

        # Also subscribe to BTCUSDT with 5-minute candles (ticks only, no CSV)
        await client.subscribe(
            "BTCUSDT",
            timeframe_minutes=5,
            on_tick=lambda t: print(f"  TICK  {t.symbol:12s}  price={t.price:.2f}"),
            on_candle=lambda c: print(f"\n  CANDLE  BTCUSDT/5m  C={c.close:.2f}  ticks={c.volume}"),
            export_ticks=True,
            export_candles=True,
        )
        print("Also subscribed to BTCUSDT/5m\n")

        await stop.wait()

    files = ClientConfig.from_env()
    from market_data_client import CsvExporter
    exp = CsvExporter(config.csv_dir)
    csv_files = exp.list_files()
    if csv_files:
        print("\nCSV files written:")
        for f in csv_files:
            print(f"  {f}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
