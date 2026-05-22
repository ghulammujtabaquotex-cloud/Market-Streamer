import WebSocket from "ws";
import { logger } from "./logger.js";
import { TickAggregator, type Candle } from "./tick-aggregator.js";

const WS_URL = "wss://api.tradowix.com/ws";
const TIMEFRAME_SEC = 60;
const RECONNECT_DELAY_MS = 5000;
const PING_INTERVAL_MS = 20000;

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

type TickCallback = (candle: Candle, price: number, timestamp: number) => void;

class TradowixWsManager {
  private ws: WebSocket | null = null;
  private token: string = "";
  private aggregators = new Map<string, TickAggregator>();
  private subscribed = new Set<string>();
  private pingTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private started = false;
  private instruments = new Map<string, InstrumentInfo>();
  private lastPrice = new Map<string, number>();
  private tickListeners = new Map<string, Set<TickCallback>>();
  private priceSeeded = false;

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
      this.stopPing();
      this.scheduleReconnect();
    });

    ws.on("error", (err) => {
      logger.error({ err }, "TradoWix WS error");
    });
  }

  private handleMessage(msg: Record<string, unknown>): void {
    const type = msg["type"] as string;

    if (type === "authRequired") {
      this.send({ type: "authenticate", token: this.token });
      return;
    }

    if (type === "authenticated") {
      logger.info("TradoWix WS: authenticated");
      this.resubscribeAll();
      return;
    }

    if (type === "instruments") {
      const data = msg["data"] as Record<string, unknown>[] | undefined;
      if (!Array.isArray(data)) return;

      let newOpenSymbols: string[] = [];

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
          id: (raw["id"] as number) ?? 0,
          symbol,
          displayName: (raw["displayName"] as string) || symbol,
          name: (raw["name"] as string) || symbol,
          category: (raw["category"] as string) || "forex",
          groupName: (raw["groupName"] as string) || "",
          isOpen: Boolean(raw["isOpen"]),
          isOTC: Boolean(raw["isOTC"]),
          precision: (raw["precision"] as number) ?? 5,
          turboPayoutRate: (raw["turboPayoutRate"] as number) ?? 0,
          blitzPayoutRate: (raw["blitzPayoutRate"] as number) ?? 0,
          change24h: (raw["change24h"] as number) ?? 0,
          changePercent24h: (raw["changePercent24h"] as number) ?? 0,
          currentPrice: this.lastPrice.get(symbol) ?? null,
        };
        this.instruments.set(symbol, info);

        if (info.isOpen && !this.subscribed.has(symbol)) {
          newOpenSymbols.push(symbol);
        }
      }

      const openSymbols = [...this.instruments.values()]
        .filter((i) => i.isOpen)
        .map((i) => i.symbol);

      logger.info({ count: openSymbols.length }, "TradoWix WS: instruments received, subscribing open symbols");

      for (const sym of openSymbols) {
        if (!this.subscribed.has(sym)) {
          this.subscribed.add(sym);
          this.getOrCreateAggregator(sym);
        }
      }

      if (this.ws?.readyState === WebSocket.OPEN && openSymbols.length > 0) {
        this.send({ type: "subscribe", symbols: openSymbols, timeframe: 1 });
      }

      if (newOpenSymbols.length > 0 && !this.priceSeeded) {
        this.priceSeeded = true;
        void this.seedPricesFromRest(openSymbols);
      }

      return;
    }

    if (type === "quote") {
      const data = msg["data"] as Record<string, unknown>;
      if (!data) return;
      const symbol = data["symbol"] as string;
      const price = data["price"] as number;
      const ts = data["timestamp"] as number;
      if (symbol && typeof price === "number" && typeof ts === "number") {
        this.lastPrice.set(symbol, price);
        const inst = this.instruments.get(symbol);
        if (inst) inst.currentPrice = price;
        this.getOrCreateAggregator(symbol).update(price, ts);

        const candle = this.aggregators.get(symbol)?.getOpenCandle();
        if (candle) {
          const listeners = this.tickListeners.get(symbol);
          if (listeners && listeners.size > 0) {
            for (const cb of listeners) {
              try { cb(candle, price, ts); } catch {}
            }
          }
        }
      }
      return;
    }

    if (type === "pong" || type === "balanceUpdate" ||
        type === "subscribed" || type === "timeSync") {
      return;
    }
  }

  private resubscribeAll(): void {
    if (this.subscribed.size === 0) return;
    const symbols = [...this.subscribed];
    this.send({ type: "subscribe", symbols, timeframe: 1 });
    logger.info({ symbols }, "TradoWix WS: resubscribed");
  }

  private getOrCreateAggregator(symbol: string): TickAggregator {
    if (!this.aggregators.has(symbol)) {
      this.aggregators.set(symbol, new TickAggregator(symbol, TIMEFRAME_SEC));
    }
    return this.aggregators.get(symbol)!;
  }

  subscribe(symbol: string): void {
    if (this.subscribed.has(symbol)) return;
    this.subscribed.add(symbol);
    this.getOrCreateAggregator(symbol);
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.send({ type: "subscribe", symbols: [...this.subscribed], timeframe: 1 });
      logger.info({ symbol, total: this.subscribed.size }, "TradoWix WS: subscribed (full list sent)");
    }
  }

  subscribeToTicks(symbol: string, cb: TickCallback): void {
    if (!this.tickListeners.has(symbol)) {
      this.tickListeners.set(symbol, new Set());
    }
    this.tickListeners.get(symbol)!.add(cb);
    this.subscribe(symbol);
  }

  unsubscribeFromTicks(symbol: string, cb: TickCallback): void {
    this.tickListeners.get(symbol)?.delete(cb);
  }

  getOpenCandle(symbol: string): Candle | null {
    return this.aggregators.get(symbol)?.getOpenCandle() ?? null;
  }

  getClosedCandles(symbol: string): Candle[] {
    return this.aggregators.get(symbol)?.getClosedCandles() ?? [];
  }

  private async seedPricesFromRest(symbols: string[]): Promise<void> {
    const token = this.token;
    if (!token) return;

    const BATCH = 5;
    const DELAY_MS = 600;
    const BASE = "https://tradowix.com/api/chart/candles";
    const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

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
            if (typeof last.c === "number" && !this.lastPrice.has(symbol)) {
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

  getInstruments(): InstrumentInfo[] {
    return [...this.instruments.values()];
  }

  getInstrument(symbol: string): InstrumentInfo | null {
    return this.instruments.get(symbol) ?? null;
  }

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
}

export const tradowixWs = new TradowixWsManager();
