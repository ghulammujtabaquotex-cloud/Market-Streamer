import http from "http";
import app from "./app.js";
import { logger } from "./lib/logger.js";
import { tradowixWs } from "./lib/tradowix-ws.js";
import { attachWsServer } from "./lib/ws-server.js";

const rawPort = process.env["PORT"];

if (!rawPort) {
  throw new Error("PORT environment variable is required but was not provided.");
}

const port = Number(rawPort);

if (Number.isNaN(port) || port <= 0) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

const token = process.env["TRADOWIX_TOKEN"] ?? "";
if (token) {
  tradowixWs.start(token);
  logger.info("TradoWix WebSocket manager started");
} else {
  logger.warn("TRADOWIX_TOKEN not set — live tick aggregation disabled");
}

const server = http.createServer(app);

attachWsServer(server);

server.listen(port, () => {
  logger.info({ port }, "Server listening");
});

server.on("error", (err) => {
  logger.error({ err }, "Server error");
  process.exit(1);
});
