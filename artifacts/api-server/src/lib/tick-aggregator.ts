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
  // Track the most recently closed candle so callers can detect roll-overs
  private lastClosed: Candle | null = null;

  constructor(symbol: string, timeframeSec: number) {
    this.symbol = symbol;
    this.periodMs = timeframeSec * 1000;
  }

  private periodStart(tsMs: number): number {
    return Math.floor(tsMs / this.periodMs) * this.periodMs;
  }

  /**
   * Returns the candle that was just closed by this update, or null if no
   * roll-over happened (i.e. tick is within the same candle period).
   */
  update(price: number, tsMs: number): Candle | null {
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
      return null;
    }

    // New period → close the current candle and open a fresh one
    if (period > this.current.t) {
      const closedCandle: Candle = { ...this.current, isClosed: true };
      this.closed.push(closedCandle);
      if (this.closed.length > MAX_CLOSED_BUFFER) {
        this.closed.shift();
      }
      this.lastClosed = closedCandle;

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
      return closedCandle;   // signal to caller that a candle was closed
    }

    // Same period → update OHLC
    this.current.h = Math.max(this.current.h, price);
    this.current.l = Math.min(this.current.l, price);
    this.current.c = price;
    return null;
  }

  getOpenCandle(): Candle | null {
    return this.current ? { ...this.current } : null;
  }

  getLastClosedCandle(): Candle | null {
    return this.lastClosed ? { ...this.lastClosed } : null;
  }

  getClosedCandles(): Candle[] {
    return [...this.closed];
  }
}
