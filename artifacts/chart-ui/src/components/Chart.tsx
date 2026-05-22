import { useEffect, useRef } from "react";
import { createChart, ColorType, CrosshairMode } from "lightweight-charts";
import type { Candle } from "@workspace/api-client-react";

export interface LiveCandleData {
  t: number;  // Unix ms (UTC)
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

// Convert ms timestamp to chart time (seconds, raw UTC).
// Do NOT shift here — apply UTC+5 only inside display formatters.
function toChartTime(ms: number) {
  return Math.floor(ms / 1000) as unknown as import("lightweight-charts").Time;
}

// Format a UTC-seconds timestamp as UTC+5 string
function fmtUtc5(timeSec: number, short = false): string {
  const ms = (timeSec + 5 * 3600) * 1000;   // add 5h offset
  const d  = new Date(ms);
  const h  = String(d.getUTCHours()).padStart(2, "0");
  const m  = String(d.getUTCMinutes()).padStart(2, "0");
  if (short) return `${h}:${m}`;
  const Y  = d.getUTCFullYear();
  const Mo = String(d.getUTCMonth() + 1).padStart(2, "0");
  const D  = String(d.getUTCDate()).padStart(2, "0");
  return `${Y}-${Mo}-${D}  ${h}:${m}`;
}

function candleColor(c: number, o: number, isLive = false) {
  const bull = isLive ? "#089981" : "#089981";
  const bear = isLive ? "#f23645" : "#f23645";
  return c >= o ? bull : bear;
}

export const Chart = ({ candles, liveCandle }: ChartProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<ReturnType<ReturnType<typeof createChart>["addCandlestickSeries"]> | null>(null);
  const dataLoadedRef  = useRef(false);
  const lastDataKeyRef = useRef<string>("");

  // ── Create chart ────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#131722" },
        textColor: "#b2b5be",
        fontSize: 11,
        fontFamily: "'Trebuchet MS', Roboto, Ubuntu, sans-serif",
      },
      grid: {
        vertLines: { color: "#1e222d" },
        horzLines: { color: "#1e222d" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: "#758696",
          labelBackgroundColor: "#2a2e39",
          width: 1,
          style: 3,       // dashed
        },
        horzLine: {
          color: "#758696",
          labelBackgroundColor: "#2a2e39",
          width: 1,
          style: 3,
        },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: "#2a2e39",
        rightOffset: 12,
        barSpacing: 6,
        minBarSpacing: 1,
        // Show UTC+5 time on axis labels
        tickMarkFormatter: (timeSec: number) => fmtUtc5(timeSec, true),
      },
      localization: {
        // Show UTC+5 time in the crosshair price tooltip
        timeFormatter: (timeSec: number) => fmtUtc5(timeSec),
      },
      rightPriceScale: {
        borderColor: "#2a2e39",
        scaleMargins: { top: 0.08, bottom: 0.06 },
        minimumWidth: 72,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    });
    chartRef.current = chart;

    const series = chart.addCandlestickSeries({
      upColor:       "#089981",
      downColor:     "#f23645",
      borderVisible: false,
      wickUpColor:   "#089981",
      wickDownColor: "#f23645",
    });
    seriesRef.current = series;

    return () => {
      chart.remove();
      chartRef.current     = null;
      seriesRef.current    = null;
      dataLoadedRef.current    = false;
      lastDataKeyRef.current   = "";
    };
  }, []);

  // ── Full candle dataset ─────────────────────────────────────────────────────
  useEffect(() => {
    if (!seriesRef.current || !candles || candles.length === 0) return;

    const first = candles[0].t;
    const last  = candles[candles.length - 1].t;
    const key   = `${first}-${last}-${candles.length}`;
    if (key === lastDataKeyRef.current) return;
    lastDataKeyRef.current = key;

    const formatted = candles.map((c) => ({
      time:      toChartTime(c.t),
      open:      c.o,
      high:      c.h,
      low:       c.l,
      close:     c.c,
      color:     c.isClosed ? undefined : candleColor(c.c, c.o, true),
      wickColor: c.isClosed ? undefined : candleColor(c.c, c.o, true),
    }));

    try {
      seriesRef.current.setData(formatted);
      dataLoadedRef.current = true;
      chartRef.current?.timeScale().fitContent();
    } catch {}
  }, [candles]);

  // ── Live tick — update open OR just-closed candle ───────────────────────────
  useEffect(() => {
    if (!seriesRef.current || !liveCandle || !dataLoadedRef.current) return;
    try {
      // Open candle → bright live highlight so it stands out
      // Closed candle (period just rolled) → undefined = series default up/down color
      const isOpen = !liveCandle.isClosed;
      const col    = isOpen ? candleColor(liveCandle.c, liveCandle.o, true) : undefined;
      seriesRef.current.update({
        time:      toChartTime(liveCandle.t),
        open:      liveCandle.o,
        high:      liveCandle.h,
        low:       liveCandle.l,
        close:     liveCandle.c,
        color:     col,
        wickColor: col,
      });
    } catch {}
  }, [liveCandle]);

  return <div ref={containerRef} className="w-full h-full" />;
};
