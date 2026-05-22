export interface Candle {
  symbol: string;
  timeframe: number;
  t: number;
  o: number;
  h: number;
  l: number;
  c: number;
  isClosed: boolean;
}

const MAX_CLOSED_BUFFER = 180;

export class TickAggregator {
  private symbol: string;
  private periodMs: number;
  private current: Candle | null = null;
  private closed: Candle[] = [];

  constructor(symbol: string, timeframeSec: number) {
    this.symbol = symbol;
    this.periodMs = timeframeSec * 1000;
  }

  private periodStart(tsMs: number): number {
    return Math.floor(tsMs / this.periodMs) * this.periodMs;
  }

  update(price: number, tsMs: number): void {
    const period = this.periodStart(tsMs);

    if (!this.current) {
      this.current = {
        symbol: this.symbol,
        timeframe: this.periodMs / 1000,
        t: period,
        o: price,
        h: price,
        l: price,
        c: price,
        isClosed: false,
      };
      return;
    }

    if (period > this.current.t) {
      const closedCandle: Candle = { ...this.current, isClosed: true };
      this.closed.push(closedCandle);
      if (this.closed.length > MAX_CLOSED_BUFFER) {
        this.closed.shift();
      }

      this.current = {
        symbol: this.symbol,
        timeframe: this.periodMs / 1000,
        t: period,
        o: price,
        h: price,
        l: price,
        c: price,
        isClosed: false,
      };
      return;
    }

    this.current.h = Math.max(this.current.h, price);
    this.current.l = Math.min(this.current.l, price);
    this.current.c = price;
  }

  getOpenCandle(): Candle | null {
    return this.current ? { ...this.current } : null;
  }

  getClosedCandles(): Candle[] {
    return [...this.closed];
  }
}
