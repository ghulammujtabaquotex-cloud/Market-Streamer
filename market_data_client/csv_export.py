from __future__ import annotations

import asyncio
import csv
import logging
import os
from collections import defaultdict

from .models import Candle, Tick

logger = logging.getLogger(__name__)


class CsvExporter:
    """
    Async CSV exporter for tick and candle data.
    Buffers rows and flushes in batches.
    One file per symbol (ticks) or per symbol+timeframe (candles).
    """

    def __init__(self, output_dir: str = "data", flush_interval: int = 50):
        self._output_dir = output_dir
        self._flush_interval = flush_interval
        self._buffers: dict[str, list[list]] = defaultdict(list)
        self._writers: dict[str, csv.writer] = {}
        self._file_handles: dict[str, object] = {}
        self._lock = asyncio.Lock()
        os.makedirs(output_dir, exist_ok=True)

    def _safe_name(self, s: str) -> str:
        return s.replace("/", "_").replace(":", "_").replace(" ", "_")

    def _ensure_writer(self, key: str, headers: list[str]) -> csv.writer:
        if key not in self._writers:
            path = os.path.join(self._output_dir, f"{key}.csv")
            write_header = not os.path.exists(path) or os.path.getsize(path) == 0
            fh = open(path, "a", newline="", buffering=1)
            self._file_handles[key] = fh
            w = csv.writer(fh)
            if write_header:
                w.writerow(headers)
            self._writers[key] = w
            logger.info("Opened CSV: %s", path)
        return self._writers[key]

    async def _flush_key(self, key: str, headers: list[str]) -> None:
        rows = self._buffers.pop(key, [])
        if not rows:
            return
        writer = self._ensure_writer(key, headers)
        for row in rows:
            writer.writerow(row)
        fh = self._file_handles.get(key)
        if fh:
            fh.flush()
        logger.debug("Flushed %d rows → %s.csv", len(rows), key)

    # ── Tick exports ─────────────────────────────────────────────────────────

    async def write_tick(self, tick: Tick) -> None:
        key = f"ticks_{self._safe_name(tick.symbol)}"
        async with self._lock:
            self._buffers[key].append(tick.to_row())
            if len(self._buffers[key]) >= self._flush_interval:
                await self._flush_key(key, Tick.csv_headers())

    # ── Candle exports ────────────────────────────────────────────────────────

    async def write_candle(self, candle: Candle) -> None:
        key = f"candles_{self._safe_name(candle.symbol)}_{candle.timeframe_minutes}m"
        async with self._lock:
            self._buffers[key].append(candle.to_row())
            if len(self._buffers[key]) >= self._flush_interval:
                await self._flush_key(key, Candle.csv_headers())

    async def write_candles_bulk(self, candles: list[Candle]) -> None:
        if not candles:
            return
        async with self._lock:
            for c in candles:
                key = f"candles_{self._safe_name(c.symbol)}_{c.timeframe_minutes}m"
                self._buffers[key].append(c.to_row())
            for key in list(self._buffers.keys()):
                headers = Candle.csv_headers() if "candles_" in key else Tick.csv_headers()
                await self._flush_key(key, headers)
        logger.info("Wrote %d candles to CSV", len(candles))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def flush_all(self) -> None:
        async with self._lock:
            for key in list(self._buffers.keys()):
                headers = Candle.csv_headers() if "candles_" in key else Tick.csv_headers()
                await self._flush_key(key, headers)

    def close(self) -> None:
        for fh in self._file_handles.values():
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
        self._writers.clear()
        self._file_handles.clear()
        self._buffers.clear()

    def list_files(self) -> list[str]:
        try:
            return [
                os.path.join(self._output_dir, f)
                for f in sorted(os.listdir(self._output_dir))
                if f.endswith(".csv")
            ]
        except FileNotFoundError:
            return []
