import { Router, type IRouter, type Request, type Response } from "express";
import { tradowixWs } from "../lib/tradowix-ws.js";
import type { Candle } from "../lib/tick-aggregator.js";

const router: IRouter = Router();

const TRADOWIX_BASE     = "https://tradowix.com/api/chart/candles";
const TRADOWIX_TICKS    = "https://tradowix.com/api/chart/ticks";
const DEFAULT_TF_SEC    = 60;
const MARKET_CLOSED_MIN = 15;   // gap > 15min + no new ticks = market closed

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

function makeHeaders(token: string): Record<string, string> {
  return {
    "User-Agent":    UA,
    Cookie:          `session-token=${token}; oauth_session_token=${token}`,
    Accept:          "application/json",
    Referer:         "https://tradowix.com/trading",
    "Cache-Control": "no-cache, no-store",
    Pragma:          "no-cache",
  };
}

// ── Step 1: REST historical candles ──────────────────────────────────────────
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
    headers: makeHeaders(token),
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
        symbol:    (raw["symbol"] as string | undefined) ?? symbol,
        timeframe: (raw["timeframe"] as number | undefined) ?? timeframeSec,
        t,
        o: raw["o"] as number,
        h: raw["h"] as number,
        l: raw["l"] as number,
        c: raw["c"] as number,
        isClosed: t + periodMs <= now ? Boolean(raw["isClosed"]) : false,
      } satisfies Candle;
    })
    .sort((a, b) => a.t - b.t);
}

// ── Step 2: REST tick API (fills the gap) ────────────────────────────────────
interface RawTick { 0: number; 1: number }   // [price, ts_ms]

async function fetchTicks(
  symbol: string,
  token: string,
  fromMs: number,
  toMs: number,
): Promise<RawTick[]> {
  const all: RawTick[] = [];
  let cursor = fromMs;

  for (let page = 0; page < 20; page++) {
    const url =
      `${TRADOWIX_TICKS}?symbol=${encodeURIComponent(symbol)}` +
      `&from=${cursor}&to=${toMs}`;

    let body: Record<string, unknown>;
    try {
      const res = await fetch(url, {
        headers: makeHeaders(token),
        cache: "no-store",
        signal: AbortSignal.timeout(12_000),
      });
      if (!res.ok) break;
      body = (await res.json()) as Record<string, unknown>;
    } catch {
      break;
    }

    const batch = (body["ticks"] as RawTick[] | undefined) ?? [];
    if (batch.length) all.push(...batch);

    const hasMore  = Boolean(body["hasMore"]);
    const nextFrom = body["nextFrom"] as number | undefined;
    if (!hasMore || !nextFrom || nextFrom <= cursor) break;
    cursor = nextFrom;
  }

  return all.sort((a, b) => a[1] - b[1]);
}

// ── Step 3: Aggregate raw ticks → 1-min candles ──────────────────────────────
function aggTicks(
  symbol: string,
  ticks: RawTick[],
  timeframeSec: number,
  nowMs: number,
): Candle[] {
  const periodMs = timeframeSec * 1000;
  const groups = new Map<number, [number, number, number, number]>(); // o h l c

  for (const [price, ts] of ticks) {
    const p = Math.floor(ts / periodMs) * periodMs;
    const g = groups.get(p);
    if (!g) {
      groups.set(p, [price, price, price, price]);
    } else {
      if (price > g[1]) g[1] = price;
      if (price < g[2]) g[2] = price;
      g[3] = price;
    }
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a - b)
    .map(([t, [o, h, l, c]]) => ({
      symbol,
      timeframe: timeframeSec,
      t,
      o, h, l, c,
      isClosed: t + periodMs <= nowMs,
    }));
}

// ── Step 4: Merge REST candles + tick candles ─────────────────────────────────
function mergeCandles(rest: Candle[], tickCandles: Candle[]): Candle[] {
  const map = new Map<number, Candle>(rest.map((c) => [c.t, c]));

  for (const tc of tickCandles) {
    const ex = map.get(tc.t);
    if (ex) {
      map.set(tc.t, {
        ...ex,
        h: Math.max(ex.h, tc.h),
        l: Math.min(ex.l, tc.l),
        c: tc.c,
        isClosed: tc.isClosed,
      });
    } else {
      map.set(tc.t, tc);
    }
  }

  return [...map.values()].sort((a, b) => a.t - b.t);
}

// ── Route ─────────────────────────────────────────────────────────────────────
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
  const periodMs  = timeframeSec * 1000;
  const nowMs     = Date.now();

  const token = process.env["TRADOWIX_TOKEN"] ?? "";
  if (!token) {
    res.status(500).json({ error: "TRADOWIX_TOKEN not configured on server" });
    return;
  }

  // ── 1. Historical REST candles ────────────────────────────────────────────
  let restCandles: Candle[] = [];
  let fetchError: string | null = null;
  try {
    restCandles = await fetchHistoricalCandles(symbol, token, timeframeSec, count);
  } catch (err: unknown) {
    fetchError = err instanceof Error ? err.message : String(err);
    req.log.warn({ symbol, err: fetchError }, "Historical candle fetch failed");
  }

  const lastRestTs  = restCandles.length > 0 ? restCandles[restCandles.length - 1].t : 0;
  const gapBeforeMin = lastRestTs > 0 ? Math.round((nowMs - lastRestTs) / 60_000) : 0;

  // ── 2. REST Tick API gap fill ─────────────────────────────────────────────
  let allTicks: RawTick[]    = [];
  let tickCandles: Candle[]  = [];
  let tickCandlesFilled      = 0;

  if (lastRestTs > 0) {
    try {
      allTicks    = await fetchTicks(symbol, token, lastRestTs, nowMs);
      tickCandles = aggTicks(symbol, allTicks, timeframeSec, nowMs);
      tickCandlesFilled = tickCandles.filter((c) => c.t > lastRestTs).length;
    } catch (err) {
      req.log.warn({ symbol, err }, "Tick fetch failed — falling back to WS candles only");
    }
  }

  // ── 3. WS-aggregated closed candles (secondary fallback) ─────────────────
  const wsClosedCandles = tradowixWs.getClosedCandles(symbol);
  for (const c of wsClosedCandles) {
    if (c.t > lastRestTs) {
      tickCandles.push({ ...c, isClosed: true });
    }
  }

  // ── 4. Merge REST + tick/WS candles ──────────────────────────────────────
  const candleMap = new Map<number, Candle>(restCandles.map((c) => [c.t, c]));

  for (const tc of tickCandles) {
    const ex = candleMap.get(tc.t);
    if (ex) {
      candleMap.set(tc.t, {
        ...ex,
        h: Math.max(ex.h, tc.h),
        l: Math.min(ex.l, tc.l),
        c: tc.c,
        isClosed: tc.isClosed,
      });
    } else {
      candleMap.set(tc.t, tc);
    }
  }

  // ── 5. Merge live open candle from WS ────────────────────────────────────
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

  // ── 6. Sort + compute gap metadata ───────────────────────────────────────
  const sorted  = [...candleMap.values()].sort((a, b) => a.t - b.t);
  const enriched = sorted.map(enrichCandle);

  const latestT     = sorted.length > 0 ? sorted[sorted.length - 1].t : 0;
  const gapAfterMin = latestT > 0 ? Math.round((nowMs - latestT) / periodMs) : gapBeforeMin;

  // Newer ticks = ticks that arrive AFTER the last REST candle period ended
  const newerTickCount = allTicks.filter((tk) => tk[1] > lastRestTs + periodMs).length;
  const marketClosed   = gapBeforeMin > MARKET_CLOSED_MIN && newerTickCount === 0 && !openCandle;

  let gapStatus: string;
  if (marketClosed) {
    gapStatus = `MARKET CLOSED — last candle ${toUtc5String(lastRestTs)} (gap ${gapBeforeMin}min, no ticks)`;
  } else if (gapAfterMin <= 1) {
    gapStatus = "NO GAP ✅  live candle included";
  } else if (tickCandlesFilled > 0) {
    const remaining = Math.max(0, gapAfterMin);
    gapStatus = `FILLING — ${tickCandlesFilled} candles from ticks · ${remaining}min remaining`;
  } else {
    gapStatus = `GAP ${gapAfterMin}min — tick data pending`;
  }

  const tfLabel =
    timeframeSec < 60   ? `${timeframeSec}s`
    : timeframeSec < 3600 ? `${timeframeSec / 60}m`
    : `${timeframeSec / 3600}h`;

  req.log.info(
    {
      symbol,
      total: enriched.length,
      restCount: restCandles.length,
      tickCount: allTicks.length,
      tickCandles: tickCandlesFilled,
      gapBefore: gapBeforeMin,
      gapAfter: gapAfterMin,
      marketClosed,
      hasLiveCandle: openCandle !== null,
      timeframe: tfLabel,
    },
    "Candles assembled",
  );

  res.json({
    symbol,
    timeframe:           timeframeSec,
    timeframe_label:     tfLabel,
    count:               enriched.length,
    oldest_t:            enriched.length > 0 ? enriched[0]["t"]                   : null,
    latest_t:            enriched.length > 0 ? enriched[enriched.length - 1]["t"] : null,
    has_live_tick:       openCandle !== null,
    fetch_error:         fetchError,
    gap_before_min:      gapBeforeMin,
    gap_after_min:       gapAfterMin,
    tick_count:          allTicks.length,
    tick_candles_filled: tickCandlesFilled,
    market_closed:       marketClosed,
    gap_status:          gapStatus,
    candles:             enriched,
  });
});

export default router;
