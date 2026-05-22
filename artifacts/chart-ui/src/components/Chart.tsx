import { useEffect, useRef } from "react";
import { createChart, ColorType } from "lightweight-charts";
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

const UTC5_SHIFT_SEC = 5 * 3600;

function toChartTime(ms: number) {
  return (Math.floor(ms / 1000) + UTC5_SHIFT_SEC) as unknown as import("lightweight-charts").Time;
}

export const Chart = ({ candles, liveCandle }: ChartProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<ReturnType<ReturnType<typeof createChart>["addCandlestickSeries"]> | null>(null);
  const lastInitTs = useRef<number>(0);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8b95a1",
      },
      grid: {
        vertLines: { color: "#151d2b" },
        horzLines: { color: "#151d2b" },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: "#1e2a3a",
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
          const M = String(d.getUTCMonth() + 1).padStart(2, "0");
          const D = String(d.getUTCDate()).padStart(2, "0");
          const h = String(d.getUTCHours()).padStart(2, "0");
          const m = String(d.getUTCMinutes()).padStart(2, "0");
          return `${Y}-${M}-${D} ${h}:${m} UTC+5`;
        },
      },
      rightPriceScale: {
        borderColor: "#1e2a3a",
        scaleMargins: { top: 0.08, bottom: 0.08 },
      },
      crosshair: {
        vertLine: { color: "#334155", labelBackgroundColor: "#1e2a3a" },
        horzLine: { color: "#334155", labelBackgroundColor: "#1e2a3a" },
      },
      width: containerRef.current.clientWidth,
      height: containerRef.current.clientHeight,
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

    const observer = new ResizeObserver(() => {
      if (chartRef.current && containerRef.current) {
        chartRef.current.applyOptions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        });
      }
    });
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      lastInitTs.current = 0;
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current || !candles || candles.length === 0) return;

    const latestTs = candles[candles.length - 1].t;

    const shouldInit =
      lastInitTs.current === 0 ||
      latestTs > lastInitTs.current + 90_000;

    if (!shouldInit) return;

    lastInitTs.current = latestTs;

    const formatted = candles
      .map((c) => ({
        time: toChartTime(c.t),
        open: c.o,
        high: c.h,
        low: c.l,
        close: c.c,
        color: !c.isClosed ? (c.c >= c.o ? "#00e5b3" : "#ff5c7a") : undefined,
        wickColor: !c.isClosed ? (c.c >= c.o ? "#00e5b3" : "#ff5c7a") : undefined,
      }))
      .sort((a, b) => (a.time as number) - (b.time as number));

    seriesRef.current.setData(formatted);
    chartRef.current?.timeScale().fitContent();
  }, [candles]);

  useEffect(() => {
    if (!seriesRef.current || !liveCandle || lastInitTs.current === 0) return;

    try {
      seriesRef.current.update({
        time: toChartTime(liveCandle.t),
        open: liveCandle.o,
        high: liveCandle.h,
        low: liveCandle.l,
        close: liveCandle.c,
        color: liveCandle.c >= liveCandle.o ? "#00e5b3" : "#ff5c7a",
        wickColor: liveCandle.c >= liveCandle.o ? "#00e5b3" : "#ff5c7a",
      });
    } catch {}
  }, [liveCandle]);

  return <div ref={containerRef} className="w-full h-full" />;
};
