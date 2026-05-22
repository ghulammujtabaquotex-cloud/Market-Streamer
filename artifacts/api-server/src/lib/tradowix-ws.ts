import WebSocket from "ws";
import { logger } from "./logger.js";
import { TickAggregator, type Candle } from "./tick-aggregator.js";

const WS_URL = "wss://api.tradowix.com/ws";
const TIMEFRAME_SEC = 60;
const RECONNECT_DELAY_MS = 5_000;
const PING_INTERVAL_MS = 15_000;   // keep-alive to TradoWix

/**
 * Callback fired when a tick arrives.
 * @param openCandle  - current (still open) candle being built
 * @param price       - latest quote price
 * @param timestamp   - quote timestamp (ms)
 * @param closedCandle - the candle that JUST closed this tick (null if no roll-over)
 */
export type TickCallback = (
  openCandle: Candle,
  price: number,
  timestamp: number,
  closedCandle: Candle | null,
) => void;

export interface InstrumentInfo {
  id: number;
  symbol: string;
  displayName: string;
  name: string;
  category: string;
  groupName: string;
  isOpen: boolean;
  isOTC: boolean;
  precision: number;
  turboPayoutRate: number;
  blitzPayoutRate: number;
  change24h: number;
  changePercent24h: number;
  currentPrice: number | null;
}

class TradowixWsManager {
  private ws: WebSocket | null = null;
  private token: string = "";
  private aggregators = new Map<string, TickAggregator>();
  // Cleanup timers: aggregator is kept alive for 5 min after last client leaves
  // so re-opening the chart can backfill the REST gap from WS-collected candles.
  private aggregatorCleanupTimers = new Map<string, NodeJS.Timeout>();

  private subscribed   = new Set<string>();
  private clientCount  = new Map<string, number>();
  private tickListeners = new Map<string, Set<TickCallback>>();

  private pingTimer:     NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private started = false;
  private authenticated = false;

  private instruments = new Map<string, InstrumentInfo>();
  private lastPrice   = new Map<string, number>();
  private priceSeeded = false;

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  start(token: string): void {
    if (this.started) return;
    this.started = true;
    this.token = token;
    this.connect();
  }

  private connect(): void {
    if (this.ws) {
      try { this.ws.terminate(); } catch {}
      this.ws = null;
    }
    this.authenticated = false;

    logger.info("TradoWix WS: connecting");

    const ws = new WebSocket(WS_URL, {
      headers: { Origin: "https://tradowix.com" },
    });
    this.ws = ws;

    ws.on("open", () => {
      logger.info("TradoWix WS: connected");
      this.startPing();
    });

    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        this.handleMessage(msg);
      } catch {}
    });

    ws.on("close", () => {
      logger.warn("TradoWix WS: disconnected — reconnecting in 5s");
      this.authenticated = false;
      this.stopPing();
      this.scheduleReconnect();
    });

    ws.on("error", (err) => {
      logger.error({ err }, "TradoWix WS error");
    });
  }

  // ── Message dispatch ────────────────────────────────────────────────────────

  private handleMessage(msg: Record<string, unknown>): void {
    const type = msg["type"] as string;

    // ── Auth ──
    if (type === "authRequired") {
      this.send({ type: "authenticate", token: this.token });
      return;
    }

    if (type === "authenticated") {
      logger.info("TradoWix WS: authenticated");
      this.authenticated = true;
      this.resubscribeAll();
      return;
    }

    // ── Instruments list ──
    if (type === "instruments") {
      const data = msg["data"] as Record<string, unknown>[] | undefined;
      if (!Array.isArray(data)) return;

      for (const raw of data) {
        const symbol = raw["symbol"] as string;
        if (!symbol) continue;

        const rawPrice =
          (raw["currentPrice"] as number | undefined) ??
          (raw["price"] as number | undefined) ??
          (raw["lastPrice"] as number | undefined);

        if (rawPrice && !this.lastPrice.has(symbol)) {
          this.lastPrice.set(symbol, rawPrice);
        }

        const info: InstrumentInfo = {
          id:               (raw["id"] as number) ?? 0,
          symbol,
          displayName:      (raw["displayName"] as string) || symbol,
          name:             (raw["name"] as string) || symbol,
          category:         (raw["category"] as string) || "forex",
          groupName:        (raw["groupName"] as string) || "",
          isOpen:           Boolean(raw["isOpen"]),
          isOTC:            Boolean(raw["isOTC"]),
          precision:        (raw["precision"] as number) ?? 5,
          turboPayoutRate:  (raw["turboPayoutRate"] as number) ?? 0,
          blitzPayoutRate:  (raw["blitzPayoutRate"] as number) ?? 0,
          change24h:        (raw["change24h"] as number) ?? 0,
          changePercent24h: (raw["changePercent24h"] as number) ?? 0,
          currentPrice:     this.lastPrice.get(symbol) ?? null,
        };
        this.instruments.set(symbol, info);
      }

      const openSymbols = [...this.instruments.values()]
        .filter((i) => i.isOpen)
        .map((i) => i.symbol);

      logger.info({ count: openSymbols.length }, "TradoWix WS: instruments received");

      if (!this.priceSeeded) {
        this.priceSeeded = true;
        void this.seedPricesFromRest(openSymbols);
      }

      // Instruments list arrives on auth too — resubscribe if needed
      this.resubscribeAll();
      return;
    }

    // ── Live tick ──
    if (type === "quote") {
      const data = msg["data"] as Record<string, unknown>;
      if (!data) return;

      const symbol    = data["symbol"] as string;
      const price     = data["price"] as number;
      const ts        = data["timestamp"] as number;

      if (!symbol || typeof price !== "number" || typeof ts !== "number") return;

      this.lastPrice.set(symbol, price);
      const inst = this.instruments.get(symbol);
      if (inst) inst.currentPrice = price;

      if (!this.subscribed.has(symbol)) return;

      const agg = this.getOrCreateAggregator(symbol);
      const closedCandle = agg.update(price, ts);   // returns closed candle on roll-over
      const openCandle   = agg.getOpenCandle();

      if (!openCandle) return;

      const listeners = this.tickListeners.get(symbol);
      if (!listeners || listeners.size === 0) return;

      for (const cb of listeners) {
        try { cb(openCandle, price, ts, closedCandle); } catch {}
      }
      return;
    }

    // Silently ignore known non-data messages
    if (
      type === "pong"       ||
      type === "balanceUpdate" ||
      type === "subscribed" ||
      type === "timeSync"
    ) return;
  }

  // ── On-demand subscription (frontend WS lifecycle) ─────────────────────────

  subscribeForClient(symbol: string, cb: TickCallback): void {
    // Cancel any pending aggregator cleanup so we keep accumulated candles
    const cleanupTimer = this.aggregatorCleanupTimers.get(symbol);
    if (cleanupTimer) {
      clearTimeout(cleanupTimer);
      this.aggregatorCleanupTimers.delete(symbol);
    }

    const count = this.clientCount.get(symbol) ?? 0;
    this.clientCount.set(symbol, count + 1);

    if (!this.tickListeners.has(symbol)) {
      this.tickListeners.set(symbol, new Set());
    }
    this.tickListeners.get(symbol)!.add(cb);

    if (!this.subscribed.has(symbol)) {
      this.subscribed.add(symbol);
      this.getOrCreateAggregator(symbol);
      // Only send subscribe if already authenticated; resubscribeAll() handles
      // the case where we connect before auth completes.
      if (this.ws?.readyState === WebSocket.OPEN && this.authenticated) {
        this.send({ type: "subscribe", symbols: [symbol], timeframe: 1 });
        logger.info({ symbol }, "TradoWix WS: subscribed (on-demand)");
      }
    }
  }

  unsubscribeForClient(symbol: string, cb: TickCallback): void {
    this.tickListeners.get(symbol)?.delete(cb);

    const count = (this.clientCount.get(symbol) ?? 1) - 1;
    if (count <= 0) {
      this.clientCount.delete(symbol);
      this.subscribed.delete(symbol);

      // Stop receiving ticks from TradoWix
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.send({ type: "unsubscribe", symbols: [symbol] });
        logger.info({ symbol }, "TradoWix WS: unsubscribed (no clients)");
      }

      // Keep aggregator alive for 5 min — lets the REST /candles endpoint
      // backfill the gap between its lag and the present using accumulated
      // WS closed candles (critical for OTC pairs with 10-40 min REST lag).
      const existing = this.aggregatorCleanupTimers.get(symbol);
      if (existing) clearTimeout(existing);
      const timer = setTimeout(() => {
        this.aggregators.delete(symbol);
        this.aggregatorCleanupTimers.delete(symbol);
        logger.info({ symbol }, "TradoWix WS: aggregator cleaned up (5 min idle)");
      }, 5 * 60 * 1000);
      this.aggregatorCleanupTimers.set(symbol, timer);
    } else {
      this.clientCount.set(symbol, count);
    }
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private resubscribeAll(): void {
    if (this.subscribed.size === 0) return;
    if (this.ws?.readyState !== WebSocket.OPEN || !this.authenticated) return;
    const symbols = [...this.subscribed];
    this.send({ type: "subscribe", symbols, timeframe: 1 });
    logger.info({ count: symbols.length }, "TradoWix WS: resubscribed active symbols");
  }

  private getOrCreateAggregator(symbol: string): TickAggregator {
    if (!this.aggregators.has(symbol)) {
      this.aggregators.set(symbol, new TickAggregator(symbol, TIMEFRAME_SEC));
    }
    return this.aggregators.get(symbol)!;
  }

  private async seedPricesFromRest(symbols: string[]): Promise<void> {
    const token = this.token;
    if (!token) return;

    const BATCH    = 8;
    const DELAY_MS = 400;
    const BASE     = "https://tradowix.com/api/chart/candles";
    const UA       =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

    let seeded = 0;

    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      await Promise.all(
        batch.map(async (symbol) => {
          if (this.lastPrice.has(symbol)) return;
          try {
            const url = `${BASE}?symbol=${encodeURIComponent(symbol)}&timeframe=60&count=1&_t=${Date.now()}`;
            const res = await fetch(url, {
              headers: {
                "User-Agent": UA,
                Cookie: `oauth_session_token=${token}`,
                Accept: "application/json",
                Referer: "https://tradowix.com/trading",
                "Cache-Control": "no-cache",
              },
              signal: AbortSignal.timeout(8_000),
            });
            if (!res.ok) return;
            const body = (await res.json()) as { candles?: Array<{ c?: number }> };
            const candles = body.candles ?? [];
            if (candles.length === 0) return;
            const last = candles[candles.length - 1];
            if (typeof last.c === "number") {
              this.lastPrice.set(symbol, last.c);
              const inst = this.instruments.get(symbol);
              if (inst) inst.currentPrice = last.c;
              seeded++;
            }
          } catch {}
        }),
      );
      if (i + BATCH < symbols.length) {
        await new Promise<void>((r) => setTimeout(r, DELAY_MS));
      }
    }

    logger.info({ seeded, total: symbols.length }, "Price seeding complete");
  }

  // ── Transport ────────────────────────────────────────────────────────────────

  private send(payload: unknown): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      this.send({ type: "ping" });
    }, PING_INTERVAL_MS);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, RECONNECT_DELAY_MS);
  }

  // ── Public read-only accessors ───────────────────────────────────────────────

  getOpenCandle(symbol: string): Candle | null {
    return this.aggregators.get(symbol)?.getOpenCandle() ?? null;
  }

  getClosedCandles(symbol: string): Candle[] {
    return this.aggregators.get(symbol)?.getClosedCandles() ?? [];
  }

  getInstruments(): InstrumentInfo[] {
    return [...this.instruments.values()];
  }

  getInstrument(symbol: string): InstrumentInfo | null {
    return this.instruments.get(symbol) ?? null;
  }

  getLastPrice(symbol: string): number | null {
    return this.lastPrice.get(symbol) ?? null;
  }
}

export const tradowixWs = new TradowixWsManager();
