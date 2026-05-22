from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


# ── Tick ─────────────────────────────────────────────────────────────────────

@dataclass
class Tick:
    symbol: str
    price: float
    timestamp: float        # milliseconds epoch

    @classmethod
    def from_dict(cls, data: dict, symbol: str = "") -> "Tick":
        return cls(
            symbol=data.get("symbol", symbol),
            price=float(data.get("price", data.get("bid", data.get("ask", 0)))),
            timestamp=float(data.get("timestamp", data.get("t", time.time() * 1000))),
        )

    @property
    def timestamp_sec(self) -> float:
        return self.timestamp / 1000.0

    def to_row(self) -> list:
        return [self.symbol, self.timestamp, self.price]

    @staticmethod
    def csv_headers() -> list[str]:
        return ["symbol", "timestamp_ms", "price"]


# ── Candle (OHLCV) built from ticks ──────────────────────────────────────────

@dataclass
class Candle:
    symbol: str
    timeframe_minutes: int
    open_time: float        # milliseconds, start of candle period
    open: float
    high: float
    low: float
    close: float
    volume: int = 0         # tick count in this candle
    is_closed: bool = False

    @property
    def close_time(self) -> float:
        return self.open_time + self.timeframe_minutes * 60_000

    @classmethod
    def from_dict(cls, data: dict, symbol: str = "", timeframe_minutes: int = 1) -> "Candle":
        open_time = float(data.get("t", data.get("open_time", data.get("timestamp", time.time() * 1000))))
        return cls(
            symbol=data.get("symbol", symbol),
            timeframe_minutes=data.get("timeframe", timeframe_minutes),
            open_time=open_time,
            open=float(data.get("o", data.get("open", 0))),
            high=float(data.get("h", data.get("high", 0))),
            low=float(data.get("l", data.get("low", 0))),
            close=float(data.get("c", data.get("close", 0))),
            volume=int(data.get("v", data.get("volume", 0))),
            is_closed=bool(data.get("closed", data.get("is_closed", True))),
        )

    def to_row(self) -> list:
        return [
            self.symbol,
            self.timeframe_minutes,
            self.open_time,
            round(self.open, 10),
            round(self.high, 10),
            round(self.low, 10),
            round(self.close, 10),
            self.volume,
            self.is_closed,
        ]

    @staticmethod
    def csv_headers() -> list[str]:
        return ["symbol", "timeframe_min", "open_time_ms", "open", "high", "low", "close", "tick_count", "is_closed"]


# ── TickAggregator — builds candles from a stream of Tick objects ─────────────

class TickAggregator:
    """
    Converts a live stream of Tick objects into OHLCV Candle objects.
    Emits a completed candle when the candle period rolls over.
    """

    def __init__(self, symbol: str, timeframe_minutes: int):
        self.symbol = symbol
        self.timeframe_minutes = timeframe_minutes
        self._current: Optional[Candle] = None
        self._period_ms = timeframe_minutes * 60_000

    def _period_start(self, ts_ms: float) -> float:
        return (ts_ms // self._period_ms) * self._period_ms

    def update(self, tick: Tick) -> Optional[Candle]:
        """
        Feed a tick.  Returns a closed Candle if the previous period just ended,
        otherwise returns None.  The current (open) candle is always available
        as self.current_candle.
        """
        ts = tick.timestamp
        period = self._period_start(ts)
        completed: Optional[Candle] = None

        if self._current is None:
            self._current = Candle(
                symbol=self.symbol,
                timeframe_minutes=self.timeframe_minutes,
                open_time=period,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=1,
                is_closed=False,
            )
        elif period > self._current.open_time:
            self._current.is_closed = True
            completed = self._current
            self._current = Candle(
                symbol=self.symbol,
                timeframe_minutes=self.timeframe_minutes,
                open_time=period,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=1,
                is_closed=False,
            )
        else:
            self._current.high  = max(self._current.high, tick.price)
            self._current.low   = min(self._current.low, tick.price)
            self._current.close = tick.price
            self._current.volume += 1

        return completed

    @property
    def current_candle(self) -> Optional[Candle]:
        return self._current


# ── ServerMessage ─────────────────────────────────────────────────────────────

@dataclass
class ServerMessage:
    type: str
    data: dict = field(default_factory=dict)
    request_id: Optional[str] = None
    error: Optional[str] = None
    success: bool = True

    @classmethod
    def from_dict(cls, raw: dict) -> "ServerMessage":
        return cls(
            type=raw.get("type", raw.get("event", "unknown")),
            data=raw.get("data", raw.get("payload", {})),
            request_id=raw.get("requestId", raw.get("id")),
            error=raw.get("error", raw.get("message") if not raw.get("success", True) else None),
            success=raw.get("success", True),
        )


# ── Instrument ────────────────────────────────────────────────────────────────

@dataclass
class Instrument:
    id: str
    symbol: str
    name: str
    display_name: str
    category: str
    precision: int
    is_active: bool
    is_otc: bool
    is_open: bool

    @classmethod
    def from_dict(cls, d: dict) -> "Instrument":
        return cls(
            id=d.get("id", ""),
            symbol=d.get("symbol", ""),
            name=d.get("name", ""),
            display_name=d.get("displayName", ""),
            category=d.get("category", ""),
            precision=int(d.get("precision", 5)),
            is_active=bool(d.get("isActive", True)),
            is_otc=bool(d.get("isOTC", False)),
            is_open=bool(d.get("isOpen", True)),
        )
