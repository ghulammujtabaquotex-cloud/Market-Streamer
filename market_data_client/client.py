from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .auth import TokenAuthHandler
from .config import ClientConfig
from .csv_export import CsvExporter
from .models import Candle, Instrument, ServerMessage, Tick, TickAggregator
from .reconnect import ReconnectPolicy

logger = logging.getLogger(__name__)

HandlerFn = Callable[[ServerMessage], Coroutine[Any, Any, None]]
TickCb    = Callable[[Tick],   Any]
CandleCb  = Callable[[Candle], Any]


class MarketDataClient:
    """
    Async WebSocket client for TradoWix (wss://api.tradowix.com/ws).

    Confirmed protocol
    ------------------
    Auth   : {type:"authenticate", token:"<oauth_session_token>"}
    Confirm: {type:"authenticated", data:{userId:...}}
    Sub    : {type:"subscribe", symbols:["EURUSD"], timeframe:<int_minutes>}
    Confirm: {type:"subscribed", data:{subscribed:[...], failed:[...], activeSymbol:...}}
    Live   : {type:"quote",     data:{symbol:"EURUSD", price:1.15987, timestamp:<ms>}}
    Server push on connect:
             {type:"instruments", data:[{id, symbol, name, precision, isOTC, isOpen, ...}]}
             {type:"balanceUpdate", data:{balance:{demoBalance, realBalance, ...}}}

    Candle history is NOT available via WebSocket.
    Candles are built client-side from the live quote tick stream using TickAggregator.

    Quick start
    -----------
    >>> cfg = ClientConfig.from_env()   # reads TRADOWIX_TOKEN from env
    >>> async with MarketDataClient(cfg) as client:
    ...     await client.subscribe("EURUSD", timeframe_minutes=1,
    ...                            on_tick=lambda t: print(t.price))
    ...     await asyncio.sleep(120)
    """

    WS_URL = "wss://api.tradowix.com/ws"

    def __init__(self, config: ClientConfig):
        self._cfg = config
        self._auth = TokenAuthHandler(config.token, timeout=config.request_timeout)
        self._reconnect = ReconnectPolicy(
            initial_delay=config.reconnect_initial_delay,
            max_delay=config.reconnect_max_delay,
            backoff_factor=config.reconnect_backoff_factor,
            max_attempts=config.reconnect_max_attempts,
        )
        self._exporter = CsvExporter(config.csv_dir, config.csv_flush_interval)

        self._ws: Optional[Any] = None
        self._running  = False

        # Subscriptions: (symbol, timeframe_min) → set of callbacks
        self._tick_callbacks:   dict[str, list[TickCb]]   = {}
        self._candle_callbacks: dict[str, list[CandleCb]] = {}
        self._aggregators:      dict[str, TickAggregator] = {}

        # Raw message handlers per type
        self._handlers: dict[str, list[HandlerFn]] = {}

        # Active subscribe registry: symbol → timeframe_min
        self._subscriptions: dict[str, int] = {}

        # Server-pushed instrument catalogue
        self.instruments: dict[str, Instrument] = {}

        self._recv_task: Optional[asyncio.Task] = None

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self) -> "MarketDataClient":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        while True:
            try:
                logger.info("Connecting to %s", self.WS_URL)
                ws = await websockets.connect(
                    self.WS_URL,
                    ping_interval=self._cfg.ping_interval,
                    ping_timeout=self._cfg.ping_timeout,
                    additional_headers={"Origin": "https://tradowix.com"},
                )
                self._ws = ws
                await self._auth.authenticate(ws)
                self._reconnect.reset()
                self._running = True
                self._recv_task = asyncio.create_task(self._recv_loop(), name="recv-loop")
                await self._resubscribe()
                logger.info("Connected and authenticated")
                return
            except Exception as exc:
                self._auth.invalidate()
                if not self._cfg.reconnect_enabled or not self._reconnect.should_reconnect():
                    raise
                logger.warning("Connection failed (%s) — will retry", exc)
                await self._reconnect.wait()

    async def disconnect(self) -> None:
        self._running = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        await self._exporter.flush_all()
        self._exporter.close()
        logger.info("Disconnected — CSV buffers flushed")

    # ── Subscription API ──────────────────────────────────────────────────────

    async def subscribe(
        self,
        symbol: str,
        timeframe_minutes: int = 1,
        on_tick:   Optional[TickCb]   = None,
        on_candle: Optional[CandleCb] = None,
        export_ticks:   bool = True,
        export_candles: bool = True,
    ) -> None:
        """
        Subscribe to live price ticks for a symbol.

        Parameters
        ----------
        symbol            : e.g. "EURUSD", "BTCUSDT"
        timeframe_minutes : candle aggregation window (1, 5, 15, 60, ...)
        on_tick           : called on every raw price tick
        on_candle         : called whenever a completed candle is emitted
        export_ticks      : write every tick to data/ticks_<SYMBOL>.csv
        export_candles    : write completed candles to data/candles_<SYMBOL>_<TF>m.csv
        """
        key = f"{symbol}:{timeframe_minutes}"

        if symbol not in self._subscriptions:
            await self._send_subscribe(symbol, timeframe_minutes)
            self._subscriptions[symbol] = timeframe_minutes
            self._aggregators[key] = TickAggregator(symbol, timeframe_minutes)
            logger.info("Subscribed to %s (tf=%dm)", symbol, timeframe_minutes)

        if on_tick:
            self._tick_callbacks.setdefault(key, []).append(on_tick)
        if on_candle:
            self._candle_callbacks.setdefault(key, []).append(on_candle)

        if export_ticks:
            self._tick_callbacks.setdefault(key, []).append(
                lambda t: asyncio.ensure_future(self._exporter.write_tick(t))
            )
        if export_candles:
            self._candle_callbacks.setdefault(key, []).append(
                lambda c: asyncio.ensure_future(self._exporter.write_candle(c))
            )

    async def unsubscribe(self, symbol: str) -> None:
        if symbol in self._subscriptions:
            await self._send({"type": "unsubscribe", "topics": [symbol]})
            tf = self._subscriptions.pop(symbol)
            key = f"{symbol}:{tf}"
            self._tick_callbacks.pop(key, None)
            self._candle_callbacks.pop(key, None)
            self._aggregators.pop(key, None)
            logger.info("Unsubscribed from %s", symbol)

    def on(self, event_type: str, handler: HandlerFn) -> None:
        """Register a raw-message handler for any server event type."""
        self._handlers.setdefault(event_type, []).append(handler)

    def current_candle(self, symbol: str, timeframe_minutes: int) -> Optional[Candle]:
        """Return the live (open) candle for symbol/tf, or None."""
        return self._aggregators.get(f"{symbol}:{timeframe_minutes}", None) and \
               self._aggregators[f"{symbol}:{timeframe_minutes}"].current_candle

    # ── Internal send ─────────────────────────────────────────────────────────

    async def _send(self, data: dict) -> None:
        if not self._ws:
            raise RuntimeError("Not connected")
        await self._ws.send(json.dumps(data))

    async def _send_subscribe(self, symbol: str, timeframe_minutes: int) -> None:
        await self._send({
            "type":      "subscribe",
            "symbols":   [symbol],
            "timeframe": timeframe_minutes,
        })

    # ── Receive loop ──────────────────────────────────────────────────────────

    async def _recv_loop(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    msg  = json.loads(raw)
                    smsg = ServerMessage.from_dict(msg)
                    await self._dispatch(smsg, msg)
                except json.JSONDecodeError as exc:
                    logger.warning("JSON parse error: %s | raw=%r", exc, raw[:200])
                except Exception as exc:
                    logger.exception("Dispatch error: %s", exc)
        except ConnectionClosed as exc:
            logger.warning("Connection closed: %s", exc)
        except Exception as exc:
            logger.error("Receive loop error: %s", exc)
        finally:
            self._auth.invalidate()
            if self._running and self._cfg.reconnect_enabled:
                asyncio.create_task(self._reconnect_loop(), name="reconnect")

    async def _dispatch(self, smsg: ServerMessage, raw: dict) -> None:
        t = smsg.type

        if t == "quote":
            await self._handle_quote(raw)

        elif t == "instruments":
            insts = raw.get("data", [])
            for d in insts:
                inst = Instrument.from_dict(d)
                self.instruments[inst.symbol] = inst
            logger.info("Instruments catalogue loaded: %d symbols", len(insts))

        elif t == "balanceUpdate":
            bal = (raw.get("data") or {}).get("balance", {})
            logger.debug(
                "Balance update — demo=%.2f real=%.2f currency=%s",
                bal.get("demoBalance", 0),
                bal.get("realBalance", 0),
                bal.get("currency", "?"),
            )

        elif t == "subscribed":
            d = raw.get("data", {})
            logger.info(
                "Subscribed: ok=%s  failed=%s  active=%s",
                d.get("subscribed", []),
                d.get("failed", []),
                d.get("activeSymbol", ""),
            )

        elif t == "error":
            err = (raw.get("data") or {}).get("error") or raw.get("message") or "?"
            logger.error("Server error: %s", err)

        elif t in ("pong", "heartbeat"):
            pass

        elif t == "ping":
            await self._send({"type": "pong"})

        elif t == "authRequired":
            await self._send(json.loads(self._auth.build_auth_message()))

        for handler in self._handlers.get(t, []):
            try:
                await handler(smsg)
            except Exception as exc:
                logger.exception("Handler error for %s: %s", t, exc)

    async def _handle_quote(self, raw: dict) -> None:
        d      = raw.get("data", raw)
        symbol = d.get("symbol", "")
        tf     = self._subscriptions.get(symbol, 1)
        key    = f"{symbol}:{tf}"

        tick = Tick.from_dict(d, symbol=symbol)

        # Feed aggregator — may yield a completed candle
        agg = self._aggregators.get(key)
        completed: Optional[Candle] = agg.update(tick) if agg else None

        # Fire tick callbacks
        for cb in self._tick_callbacks.get(key, []):
            try:
                r = cb(tick)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as exc:
                logger.exception("Tick callback error: %s", exc)

        # Fire candle callbacks for completed candle
        if completed:
            for cb in self._candle_callbacks.get(key, []):
                try:
                    r = cb(completed)
                    if asyncio.iscoroutine(r):
                        await r
                except Exception as exc:
                    logger.exception("Candle callback error: %s", exc)

    # ── Reconnect ──────────────────────────────────────────────────────────────

    async def _resubscribe(self) -> None:
        if not self._subscriptions:
            return
        logger.info("Re-subscribing to %d symbols", len(self._subscriptions))
        for symbol, tf in list(self._subscriptions.items()):
            await self._send_subscribe(symbol, tf)

    async def _reconnect_loop(self) -> None:
        while self._running and self._reconnect.should_reconnect():
            await self._reconnect.wait()
            try:
                ws = await websockets.connect(
                    self.WS_URL,
                    ping_interval=self._cfg.ping_interval,
                    ping_timeout=self._cfg.ping_timeout,
                    additional_headers={"Origin": "https://tradowix.com"},
                )
                self._ws = ws
                await self._auth.authenticate(ws)
                self._reconnect.reset()
                self._recv_task = asyncio.create_task(self._recv_loop(), name="recv-loop")
                await self._resubscribe()
                logger.info("Reconnected successfully")
                return
            except Exception as exc:
                logger.warning("Reconnect attempt failed: %s", exc)

        if self._running:
            logger.error("Max reconnect attempts exhausted — giving up")
            self._running = False
