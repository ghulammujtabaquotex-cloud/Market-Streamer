import { Router, type IRouter, type Request, type Response } from "express";
import { tradowixWs } from "../lib/tradowix-ws.js";
import type { Candle } from "../lib/tick-aggregator.js";

const router: IRouter = Router();

const TRADOWIX_BASE     = "https://tradowix.com/api/chart/candles";
const DEFAULT_TF_SEC    = 60;
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const UTC5_OFFSET_MS = 5 * 60 * 60 * 1000;

function toUtcString(ms: number)  { return new Date(ms).toISOString().replace("T", " ").replace(/\.\d+Z$/, ""); }
function toUtc5String(ms: number) { return new Date(ms + UTC5_OFFSET_MS).toISOString().replace("T", " ").replace(/\.\d+Z$/, ""); }

function enrichCandle(c: Candle): Record<string, unknown> {
  return {
    symbol:        c.symbol,
    timeframe:     c.timeframe,
    t:             c.t,
    datetime_utc:  toUtcString(c.t),
    datetime_utc5: toUtc5String(c.t),
    o: c.o, h: c.h, l: c.l, c: c.c,
    isClosed: c.isClosed,
  };
}

async function fetchHistoricalCandles(
  symbol: string,
  token: string,
  timeframeSec: number,
  count: number,
): Promise<Candle[]> {
  const url =
    `${TRADOWIX_BASE}?symbol=${encodeURIComponent(symbol)}` +
    `&timeframe=${timeframeSec}&count=${count}&_t=${Date.now()}`;

  const res = await fetch(url, {
    headers: {
      "User-Agent":    UA,
      Cookie:          `oauth_session_token=${token}`,
      Accept:          "application/json",
      Referer:         "https://tradowix.com/trading",
      "Cache-Control": "no-cache, no-store",
      Pragma:          "no-cache",
    },
    cache:  "no-store",
    signal: AbortSignal.timeout(15_000),
  });

  if (!res.ok) throw new Error(`TradoWix REST ${res.status}: ${await res.text()}`);

  const body = (await res.json()) as { candles?: Array<Record<string, unknown>> };
  const periodMs = timeframeSec * 1000;
  const now = Date.now();

  return (body.candles ?? [])
    .map((raw) => {
      const t = raw["t"] as number;
      return {
        // Inject symbol + timeframe (TradoWix REST includes them, but be safe)
        symbol:    (raw["symbol"] as string | undefined) ?? symbol,
        timeframe: (raw["timeframe"] as number | undefined) ?? timeframeSec,
        t,
        o: raw["o"] as number,
        h: raw["h"] as number,
        l: raw["l"] as number,
        c: raw["c"] as number,
        // ── isClosed fix ──────────────────────────────────────────────────
        // TradoWix REST sometimes returns the live candle as isClosed:true.
        // Override: if the candle's period hasn't ended yet → it's still open.
        isClosed: t + periodMs <= now
          ? Boolean(raw["isClosed"])
          : false,
      } satisfies Candle;
    })
    .sort((a, b) => a.t - b.t);
}

router.get("/candles", async (req: Request, res: Response) => {
  const symbol = (req.query["symbol"] as string | undefined)?.toUpperCase();
  if (!symbol) {
    res.status(400).json({ error: "symbol query parameter is required" });
    return;
  }

  const rawCount  = parseInt((req.query["count"]     as string | undefined) ?? "500", 10);
  const rawTf     = parseInt((req.query["timeframe"] as string | undefined) ?? String(DEFAULT_TF_SEC), 10);
  const count     = isNaN(rawCount) || rawCount < 1  ? 500 : Math.min(rawCount, 1000);
  const timeframeSec = isNaN(rawTf) || rawTf < 1 ? DEFAULT_TF_SEC : rawTf;

  const token = process.env["TRADOWIX_TOKEN"] ?? "";
  if (!token) {
    res.status(500).json({ error: "TRADOWIX_TOKEN not configured on server" });
    return;
  }

  // ── 1. Fresh historical candles from TradoWix REST ────────────────────────
  let restCandles: Candle[] = [];
  let fetchError: string | null = null;
  try {
    restCandles = await fetchHistoricalCandles(symbol, token, timeframeSec, count);
  } catch (err: unknown) {
    fetchError = err instanceof Error ? err.message : String(err);
    req.log.warn({ symbol, err: fetchError }, "Historical candle fetch failed");
  }

  // ── 2. Build map (REST is source of truth) ────────────────────────────────
  const candleMap = new Map<number, Candle>();
  for (const c of restCandles) candleMap.set(c.t, c);

  // ── 3. Backfill gap with WS-aggregated closed candles ────────────────────
  // TradoWix REST can lag 1-40 min behind real-time (worse for OTC pairs).
  // The aggregator holds up to 180 closed candles it collected from live ticks.
  // Merge any that are NEWER than the last REST candle to bridge the gap.
  const latestRestTs = restCandles.length > 0
    ? restCandles[restCandles.length - 1].t
    : 0;

  const wsClosedCandles = tradowixWs.getClosedCandles(symbol);
  let wsFilledCount = 0;
  for (const c of wsClosedCandles) {
    if (c.t > latestRestTs && !candleMap.has(c.t)) {
      candleMap.set(c.t, { ...c, isClosed: true });
      wsFilledCount++;
    }
  }

  // ── 4. Merge live open candle from WS aggregator ──────────────────────────
  const openCandle = tradowixWs.getOpenCandle(symbol);
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

  // ── 5. Sort + enrich ───────────────────────────────────────────────────────
  const sorted  = [...candleMap.values()].sort((a, b) => a.t - b.t);
  const enriched = sorted.map(enrichCandle);

  const tfLabel =
    timeframeSec < 60   ? `${timeframeSec}s`
    : timeframeSec < 3600 ? `${timeframeSec / 60}m`
    : `${timeframeSec / 3600}h`;

  req.log.info(
    { symbol, total: enriched.length, restCount: restCandles.length,
      hasLiveCandle: openCandle !== null, timeframe: tfLabel },
    "Candles assembled",
  );

  res.json({
    symbol,
    timeframe:       timeframeSec,
    timeframe_label: tfLabel,
    count:           enriched.length,
    oldest_t:        enriched.length > 0 ? enriched[0]["t"]                      : null,
    latest_t:        enriched.length > 0 ? enriched[enriched.length - 1]["t"]    : null,
    has_live_tick:   openCandle !== null,
    fetch_error:     fetchError,
    candles:         enriched,
  });
});

export default router;
