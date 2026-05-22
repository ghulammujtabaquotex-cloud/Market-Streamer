import { useEffect, useRef } from "react";
import { createChart, ColorType, CrosshairMode } from "lightweight-charts";
import type { Candle } from "@workspace/api-client-react";

export interface LiveCandleData {
  t: number;
  o: number;
  h: number;
  l: number;
  c: number;
  isClosed: boolean;
}

export interface ChartProps {
  candles: Candle[];
  liveCandle?: LiveCandleData | null;
}

// TradoWix shows UTC+5 times — shift candle timestamps for display
const UTC5_SHIFT_SEC = 5 * 3600;

function toChartTime(ms: number) {
  return (Math.floor(ms / 1000) + UTC5_SHIFT_SEC) as unknown as import("lightweight-charts").Time;
}

function candleColor(c: number, o: number, isLive = false) {
  const bull = isLive ? "#00e5b3" : "#26a69a";
  const bear = isLive ? "#ff5c7a" : "#ef5350";
  return c >= o ? bull : bear;
}

export const Chart = ({ candles, liveCandle }: ChartProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<ReturnType<ReturnType<typeof createChart>["addCandlestickSeries"]> | null>(null);
  // Track whether we've done the initial setData so liveCandle updates are safe
  const dataLoadedRef = useRef(false);
  // Track the last candle timestamp we set — to avoid redundant full reloads
  const lastDataKeyRef = useRef<string>("");

  // ── Create chart instance once ──────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8b95a1",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "#0f1923" },
        horzLines: { color: "#0f1923" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "#2d4a6e", labelBackgroundColor: "#1e2a3a", width: 1 },
        horzLine: { color: "#2d4a6e", labelBackgroundColor: "#1e2a3a", width: 1 },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: "#1a2535",
        rightOffset: 10,
        barSpacing: 8,
        minBarSpacing: 2,
        fixLeftEdge: false,
        fixRightEdge: false,
        tickMarkFormatter: (time: number) => {
          const d = new Date(time * 1000);
          const h = String(d.getUTCHours()).padStart(2, "0");
          const m = String(d.getUTCMinutes()).padStart(2, "0");
          return `${h}:${m}`;
        },
      },
      localization: {
        timeFormatter: (time: number) => {
          const d = new Date(time * 1000);
          const Y = d.getUTCFullYear();
          const Mo = String(d.getUTCMonth() + 1).padStart(2, "0");
          const D = String(d.getUTCDate()).padStart(2, "0");
          const h = String(d.getUTCHours()).padStart(2, "0");
          const m = String(d.getUTCMinutes()).padStart(2, "0");
          return `${Y}-${Mo}-${D} ${h}:${m} (UTC+5)`;
        },
      },
      rightPriceScale: {
        borderColor: "#1a2535",
        scaleMargins: { top: 0.06, bottom: 0.06 },
        minimumWidth: 70,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    });
    chartRef.current = chart;

    const series = chart.addCandlestickSeries({
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      dataLoadedRef.current = false;
      lastDataKeyRef.current = "";
    };
  }, []);

  // ── Load / refresh full candle dataset ────────────────────────────────────
  useEffect(() => {
    if (!seriesRef.current || !candles || candles.length === 0) return;

    // Build a lightweight key from first+last timestamp to detect real data changes
    const first = candles[0].t;
    const last = candles[candles.length - 1].t;
    const key = `${first}-${last}-${candles.length}`;
    if (key === lastDataKeyRef.current) return; // same data, skip
    lastDataKeyRef.current = key;

    const formatted = candles.map((c) => ({
      time: toChartTime(c.t),
      open: c.o,
      high: c.h,
      low: c.l,
      close: c.c,
      color: c.isClosed ? undefined : candleColor(c.c, c.o, true),
      wickColor: c.isClosed ? undefined : candleColor(c.c, c.o, true),
    }));

    try {
      seriesRef.current.setData(formatted);
      dataLoadedRef.current = true;
      // Fit all data into view — scrollToRealTime() uses raw UTC and would
      // overshoot our UTC+5 shifted timestamps, leaving an empty viewport.
      chartRef.current?.timeScale().fitContent();
    } catch {}
  }, [candles]);

  // ── Apply live WS tick — update the current open candle ──────────────────
  useEffect(() => {
    if (!seriesRef.current || !liveCandle || !dataLoadedRef.current) return;

    try {
      const col = candleColor(liveCandle.c, liveCandle.o, true);
      seriesRef.current.update({
        time: toChartTime(liveCandle.t),
        open: liveCandle.o,
        high: liveCandle.h,
        low: liveCandle.l,
        close: liveCandle.c,
        color: col,
        wickColor: col,
      });
    } catch {}
  }, [liveCandle]);

  return <div ref={containerRef} className="w-full h-full" />;
};
