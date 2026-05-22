import { Router, type IRouter, type Request, type Response } from "express";

const router: IRouter = Router();

const BASE = process.env["REPLIT_DEV_DOMAIN"]
  ? `https://${process.env["REPLIT_DEV_DOMAIN"]}/api`
  : "/api";

router.get("/", (_req: Request, res: Response) => {
  res.json({
    name: "TradoWix Market Data API",
    version: "1.0.0",
    base_url: BASE,
    endpoints: [
      {
        method: "GET",
        path: "/api/healthz",
        url: `${BASE}/healthz`,
        description: "Server health check",
        example: `${BASE}/healthz`,
      },
      {
        method: "GET",
        path: "/api/instruments",
        url: `${BASE}/instruments`,
        description: "All trading instruments — symbol, displayName, price, payout rates, open status",
        example: `${BASE}/instruments`,
      },
      {
        method: "GET",
        path: "/api/latest",
        url: `${BASE}/latest`,
        description: "Latest price snapshot for all open instruments (no symbol param) OR a single symbol",
        params: {
          symbol: "optional — e.g. EURUSD, GBPUSD, BTCUSDT",
        },
        examples: {
          all_open: `${BASE}/latest`,
          single: `${BASE}/latest?symbol=EURUSD`,
          gbp: `${BASE}/latest?symbol=GBPUSD`,
          btc: `${BASE}/latest?symbol=BTCUSDT`,
        },
      },
      {
        method: "GET",
        path: "/api/candles",
        url: `${BASE}/candles`,
        description: "Historical + live 1-minute OHLCV candlestick data for a symbol (fetched from TradoWix)",
        params: {
          symbol: "required — e.g. EURUSD",
          count: "optional — number of candles to fetch (default 500, max 1000)",
          timeframe: "optional — candle timeframe in seconds (default 60 = 1m)",
        },
        examples: {
          default: `${BASE}/candles?symbol=EURUSD`,
          custom_count: `${BASE}/candles?symbol=GBPUSD&count=100`,
          five_min: `${BASE}/candles?symbol=EURUSD&timeframe=300`,
          btc: `${BASE}/candles?symbol=BTCUSDT&count=200`,
        },
      },
      {
        method: "WS",
        path: "/api/ws",
        url: `${BASE.replace("https://", "wss://").replace("http://", "ws://").replace("/api", "")}/api/ws`,
        description: "Live WebSocket tick stream for a symbol (OHLCV candle + raw price on every tick)",
        params: {
          symbol: "required query param — e.g. ?symbol=EURUSD",
        },
        message_types: {
          candle: "Sent on connect with the current open candle state",
          tick: "{ type:'tick', price, timestamp, candle:{t,o,h,l,c,isClosed,...} }",
        },
        example: `${BASE.replace("https://", "wss://").replace("http://", "ws://").replace("/api", "")}/api/ws?symbol=EURUSD`,
      },
    ],
  });
});

export default router;
