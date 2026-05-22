import { WebSocketServer, WebSocket } from "ws";
import type { IncomingMessage, Server } from "http";
import { tradowixWs } from "./tradowix-ws.js";
import { logger } from "./logger.js";
import type { Candle } from "./tick-aggregator.js";

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
    datetime_utc: toUtcString(candle.t),
    datetime_utc5: toUtc5String(candle.t),
  };
}

export function attachWsServer(server: Server): void {
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req: IncomingMessage, socket, head) => {
    let pathname = "/";
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      pathname = url.pathname;
    } catch {}

    if (pathname !== "/ws") {
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

    const openCandle = tradowixWs.getOpenCandle(symbol);
    if (openCandle) {
      ws.send(JSON.stringify({ type: "candle", candle: enrichCandle(openCandle) }));
    }

    const onTick = (candle: Candle, price: number, timestamp: number) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: "tick",
          price,
          timestamp,
          candle: enrichCandle(candle),
        }));
      }
    };

    tradowixWs.subscribeToTicks(symbol, onTick);

    ws.on("close", () => {
      tradowixWs.unsubscribeFromTicks(symbol, onTick);
      logger.info({ symbol }, "Frontend WS: client disconnected");
    });

    ws.on("error", (err) => {
      logger.error({ err, symbol }, "Frontend WS: client error");
    });
  });

  logger.info("Frontend WebSocket server attached at /ws");
}
