import { WebSocketServer, WebSocket } from "ws";
import type { IncomingMessage, Server } from "http";
import { tradowixWs, type TickCallback } from "./tradowix-ws.js";
import { logger } from "./logger.js";
import type { Candle } from "./tick-aggregator.js";

// Max bytes we allow to queue in the send buffer before dropping a tick.
// Keeps slow clients from accumulating a stale backlog.
const MAX_BUFFERED_BYTES = 64 * 1024;   // 64 KB

// How often to send a protocol-level ping to keep browser connections alive.
const CLIENT_PING_INTERVAL_MS = 20_000;

const UTC5_OFFSET_MS = 5 * 60 * 60 * 1000;

function toUtcString(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").replace(/\.\d+Z$/, "");
}

function toUtc5String(ms: number): string {
  return new Date(ms + UTC5_OFFSET_MS)
    .toISOString()
    .replace("T", " ")
    .replace(/\.\d+Z$/, "");
}

function enrichCandle(candle: Candle) {
  return {
    ...candle,
    datetime_utc:  toUtcString(candle.t),
    datetime_utc5: toUtc5String(candle.t),
  };
}

function trySend(ws: WebSocket, payload: string): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  // Drop tick if client is backed up — it will catch up on next REST refetch
  if ((ws as unknown as { bufferedAmount: number }).bufferedAmount > MAX_BUFFERED_BYTES) {
    return;
  }
  try { ws.send(payload); } catch {}
}

export function attachWsServer(server: Server): void {
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req: IncomingMessage, socket, head) => {
    let pathname = "/";
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      pathname = url.pathname;
    } catch {}

    if (pathname !== "/api/ws") {
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket as import("net").Socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
    let symbol = "EURUSD";
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      symbol = (url.searchParams.get("symbol") ?? "EURUSD").toUpperCase();
    } catch {}

    logger.info({ symbol }, "Frontend WS: client connected");

    // ── Tick callback — fired on every live quote ──────────────────────────
    // closedCandle is non-null when a candle period just rolled over.
    // We send it so the frontend can fill gaps between REST data and the live feed.
    const onTick: TickCallback = (openCandle, price, timestamp, closedCandle) => {
      if (closedCandle) {
        trySend(ws, JSON.stringify({
          type: "candle_closed",
          candle: enrichCandle(closedCandle),
        }));
      }

      trySend(ws, JSON.stringify({
        type:      "tick",
        price,
        timestamp,
        candle:    enrichCandle(openCandle),
      }));
    };

    // ── Subscribe before sending the initial snapshot ──────────────────────
    // This order ensures zero ticks are missed: the callback is registered
    // before we read the open candle, so any concurrent tick will be delivered.
    tradowixWs.subscribeForClient(symbol, onTick);

    // Send current open candle so the chart can render immediately while
    // waiting for the next live tick.
    const openCandle = tradowixWs.getOpenCandle(symbol);
    if (openCandle) {
      trySend(ws, JSON.stringify({ type: "candle", candle: enrichCandle(openCandle) }));
    }

    // ── Keep-alive: ping every 20s ─────────────────────────────────────────
    // Prevents browsers from silently dropping idle WS connections which
    // would cause "stuck" tick displays.
    const pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch {}
      } else {
        clearInterval(pingTimer);
      }
    }, CLIENT_PING_INTERVAL_MS);

    ws.on("close", () => {
      clearInterval(pingTimer);
      tradowixWs.unsubscribeForClient(symbol, onTick);
      logger.info({ symbol }, "Frontend WS: client disconnected");
    });

    ws.on("error", (err) => {
      logger.error({ err, symbol }, "Frontend WS: client error");
    });
  });

  logger.info("Frontend WebSocket server attached at /api/ws");
}
