import { Router, type IRouter, type Request, type Response } from "express";
import { tradowixWs } from "../lib/tradowix-ws.js";

const router: IRouter = Router();

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

router.get("/latest", (req: Request, res: Response) => {
  const symbol = (req.query["symbol"] as string | undefined)?.toUpperCase();

  if (!symbol) {
    const instruments = tradowixWs.getInstruments().filter((i) => i.isOpen);
    res.json({
      instruments: instruments.map((i) => ({
        symbol: i.symbol,
        displayName: i.displayName,
        category: i.category,
        price: i.currentPrice,
        change24h: i.change24h,
        changePercent24h: i.changePercent24h,
        turboPayoutRate: i.turboPayoutRate,
        blitzPayoutRate: i.blitzPayoutRate,
        isOpen: i.isOpen,
        updated_at: toUtcString(Date.now()),
        updated_at_utc5: toUtc5String(Date.now()),
      })),
      count: instruments.length,
      server_time_utc: toUtcString(Date.now()),
      server_time_utc5: toUtc5String(Date.now()),
    });
    return;
  }

  const inst = tradowixWs.getInstrument(symbol);
  if (!inst) {
    res.status(404).json({ error: `Symbol "${symbol}" not found` });
    return;
  }

  const openCandle = tradowixWs.getOpenCandle(symbol);
  const closedCandles = tradowixWs.getClosedCandles(symbol);
  const lastClosed = closedCandles.length > 0 ? closedCandles[closedCandles.length - 1] : null;

  res.json({
    symbol: inst.symbol,
    displayName: inst.displayName,
    category: inst.category,
    groupName: inst.groupName,
    isOpen: inst.isOpen,
    precision: inst.precision,
    price: inst.currentPrice,
    change24h: inst.change24h,
    changePercent24h: inst.changePercent24h,
    turboPayoutRate: inst.turboPayoutRate,
    blitzPayoutRate: inst.blitzPayoutRate,
    live_candle: openCandle
      ? {
          t: openCandle.t,
          datetime_utc: toUtcString(openCandle.t),
          datetime_utc5: toUtc5String(openCandle.t),
          o: openCandle.o,
          h: openCandle.h,
          l: openCandle.l,
          c: openCandle.c,
          isClosed: false,
        }
      : null,
    last_closed_candle: lastClosed
      ? {
          t: lastClosed.t,
          datetime_utc: toUtcString(lastClosed.t),
          datetime_utc5: toUtc5String(lastClosed.t),
          o: lastClosed.o,
          h: lastClosed.h,
          l: lastClosed.l,
          c: lastClosed.c,
          isClosed: true,
        }
      : null,
    server_time_utc: toUtcString(Date.now()),
    server_time_utc5: toUtc5String(Date.now()),
  });
});

export default router;
