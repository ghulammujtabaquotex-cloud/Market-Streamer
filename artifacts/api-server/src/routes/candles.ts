import { Router, type IRouter, type Request, type Response } from "express";
import { tradowixWs } from "../lib/tradowix-ws.js";
import type { Candle } from "../lib/tick-aggregator.js";

const router: IRouter = Router();

const TRADOWIX_BASE = "https://tradowix.com/api/chart/candles";
const DEFAULT_TIMEFRAME_SEC = 60;
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

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

async function fetchHistoricalCandles(
  symbol: string,
  token: string,
  timeframeSec: number,
  count: number,
): Promise<Candle[]> {
  const cacheBust = Date.now();
  const url =
    `${TRADOWIX_BASE}?symbol=${encodeURIComponent(symbol)}` +
    `&timeframe=${timeframeSec}&count=${count}&_t=${cacheBust}`;

  const res = await fetch(url, {
    headers: {
      "User-Agent": UA,
      Cookie: `oauth_session_token=${token}`,
      Accept: "application/json",
      Referer: "https://tradowix.com/trading",
      "Cache-Control": "no-cache, no-store",
      "Pragma": "no-cache",
    },
    cache: "no-store",
    signal: AbortSignal.timeout(15_000),
  });

  if (!res.ok) {
    throw new Error(`TradoWix REST ${res.status}: ${await res.text()}`);
  }

  const body = (await res.json()) as { candles?: Candle[] };
  return (body.candles ?? []).sort((a, b) => a.t - b.t);
}

function enrichCandle(candle: Candle): Record<string, unknown> {
  return {
    symbol: candle.symbol,
    timeframe: candle.timeframe,
    t: candle.t,
    datetime_utc: toUtcString(candle.t),
    datetime_utc5: toUtc5String(candle.t),
    o: candle.o,
    h: candle.h,
    l: candle.l,
    c: candle.c,
    isClosed: candle.isClosed,
  };
}

router.get("/candles", async (req: Request, res: Response) => {
  const symbol = (req.query["symbol"] as string | undefined)?.toUpperCase();

  if (!symbol) {
    res.status(400).json({ error: "symbol query parameter is required" });
    return;
  }

  const rawCount = parseInt((req.query["count"] as string | undefined) ?? "500", 10);
  const count = isNaN(rawCount) || rawCount < 1 ? 500 : Math.min(rawCount, 1000);

  const rawTf = parseInt((req.query["timeframe"] as string | undefined) ?? String(DEFAULT_TIMEFRAME_SEC), 10);
  const timeframeSec = isNaN(rawTf) || rawTf < 1 ? DEFAULT_TIMEFRAME_SEC : rawTf;
  const timeframeMs = timeframeSec * 1000;

  const token = process.env["TRADOWIX_TOKEN"] ?? "";
  if (!token) {
    res.status(500).json({ error: "TRADOWIX_TOKEN not configured on server" });
    return;
  }

  tradowixWs.subscribe(symbol);

  let restCandles: Candle[] = [];
  let fetchError: string | null = null;

  try {
    restCandles = await fetchHistoricalCandles(symbol, token, timeframeSec, count);
  } catch (err: unknown) {
    fetchError = err instanceof Error ? err.message : String(err);
    req.log.warn({ symbol, err: fetchError }, "Historical candle fetch failed");
  }

  const wsClosedCandles = tradowixWs.getClosedCandles(symbol);
  const openCandle = tradowixWs.getOpenCandle(symbol);

  const candleMap = new Map<number, Candle>();

  for (const c of restCandles) {
    candleMap.set(c.t, c);
  }

  for (const c of wsClosedCandles) {
    const existing = candleMap.get(c.t);
    if (!existing) {
      candleMap.set(c.t, c);
    } else {
      candleMap.set(c.t, {
        ...existing,
        h: Math.max(existing.h, c.h),
        l: Math.min(existing.l, c.l),
        c: c.c,
        isClosed: true,
      });
    }
  }

  if (openCandle) {
    const existing = candleMap.get(openCandle.t);
    if (!existing) {
      candleMap.set(openCandle.t, { ...openCandle, isClosed: false });
    } else {
      candleMap.set(openCandle.t, {
        ...existing,
        h: Math.max(existing.h, openCandle.h),
        l: Math.min(existing.l, openCandle.l),
        c: openCandle.c,
        isClosed: false,
      });
    }
  }

  const sortedTs = [...candleMap.keys()].sort((a, b) => a - b);

  const allTs = sortedTs.length > 1 ? sortedTs : [];
  if (allTs.length >= 2) {
    const first = allTs[0];
    const last = allTs[allTs.length - 1];
    for (let t = first; t <= last; t += timeframeMs) {
      if (!candleMap.has(t)) {
        const prev = candleMap.get(t - timeframeMs);
        if (prev) {
          candleMap.set(t, {
            symbol,
            timeframe: timeframeSec,
            t,
            o: prev.c,
            h: prev.c,
            l: prev.c,
            c: prev.c,
            isClosed: t < (openCandle?.t ?? Infinity),
          });
        }
      }
    }
  }

  const finalTs = [...candleMap.keys()].sort((a, b) => a - b);
  const candles = finalTs.map((t) => candleMap.get(t)!);
  const enriched = candles.map(enrichCandle);

  req.log.info(
    {
      symbol,
      total: enriched.length,
      restCount: restCandles.length,
      wsClosedCount: wsClosedCandles.length,
      hasOpen: openCandle !== null,
    },
    "Candles assembled",
  );

  const tfLabel =
    timeframeSec < 60 ? `${timeframeSec}s`
    : timeframeSec < 3600 ? `${timeframeSec / 60}m`
    : `${timeframeSec / 3600}h`;

  res.json({
    symbol,
    timeframe: timeframeSec,
    timeframe_label: tfLabel,
    count: enriched.length,
    oldest_t: enriched.length > 0 ? enriched[0]["t"] : null,
    latest_t: enriched.length > 0 ? enriched[enriched.length - 1]["t"] : null,
    has_live_tick: openCandle !== null,
    fetch_error: fetchError,
    candles: enriched,
  });
});

export default router;
