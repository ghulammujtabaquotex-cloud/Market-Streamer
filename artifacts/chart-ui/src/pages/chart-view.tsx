import { useState, useEffect, useRef, useCallback } from "react";
import { useParams, Link } from "wouter";
import { useGetCandles, getGetCandlesQueryKey, useListInstruments } from "@workspace/api-client-react";
import type { CandlesResponse } from "@workspace/api-client-react";
import { Chart, type LiveCandleData } from "@/components/Chart";
import { ArrowLeft, Activity, Clock, AlertCircle, Wifi, WifiOff, TrendingUp, Ban, CheckCircle2 } from "lucide-react";
import { formatPrice, formatChange, formatPercent, cn } from "@/lib/utils";

// ── Live WebSocket hook ─────────────────────────────────────────────────────
function useChartWs(symbol: string | undefined) {
  const [liveCandle, setLiveCandle] = useState<LiveCandleData | null>(null);
  const [livePrice,  setLivePrice]  = useState<number | null>(null);
  const [connected,  setConnected]  = useState(false);
  const wsRef           = useRef<WebSocket | null>(null);
  const reconnectTimer  = useRef<ReturnType<typeof setTimeout> | null>(null);
  const unmounted       = useRef(false);

  const connect = useCallback(() => {
    if (!symbol || unmounted.current) return;
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url   = `${proto}//${window.location.host}/api/ws?symbol=${encodeURIComponent(symbol)}`;
    const ws    = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      if (unmounted.current) { ws.close(); return; }
      setConnected(true);
    };
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data as string) as {
          type: string;
          price?: number;
          candle?: LiveCandleData;
        };
        if (msg.type === "tick" || msg.type === "candle") {
          if (msg.candle) setLiveCandle({ ...msg.candle, isClosed: false });
          if (msg.price != null) setLivePrice(msg.price);
        } else if (msg.type === "candle_closed" && msg.candle) {
          setLiveCandle({ ...msg.candle, isClosed: true });
        }
      } catch {}
    };
    ws.onclose = () => {
      setConnected(false);
      if (!unmounted.current) reconnectTimer.current = setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();
  }, [symbol]);

  useEffect(() => {
    unmounted.current = false;
    setLiveCandle(null);
    setLivePrice(null);
    setConnected(false);
    connect();
    return () => {
      unmounted.current = true;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return { liveCandle, livePrice, connected };
}

// ── Gap Status Badge ─────────────────────────────────────────────────────────
function GapBadge({ data }: { data: CandlesResponse }) {
  const { gap_after_min, market_closed, tick_count, tick_candles_filled, gap_before_min } = data;

  if (market_closed) {
    return (
      <div className="flex items-center gap-1.5 text-[10px] font-mono px-2 py-1 rounded bg-[#2a2e39] text-[#758696]" title={data.gap_status}>
        <Ban className="h-3 w-3 flex-none" />
        <span className="hidden md:inline tracking-wider font-bold">CLOSED</span>
      </div>
    );
  }

  if (gap_after_min <= 1) {
    return (
      <div
        className="flex items-center gap-1.5 text-[10px] font-mono px-2 py-1 rounded bg-[#089981]/10 text-[#089981]"
        title={`${tick_count} ticks · ${tick_candles_filled} candles filled · gap was ${gap_before_min}min`}
      >
        <CheckCircle2 className="h-3 w-3 flex-none" />
        <span className="hidden md:inline tracking-wider font-bold">NO GAP</span>
        {tick_candles_filled > 0 && (
          <span className="hidden lg:inline text-[#089981]/70">+{tick_candles_filled}</span>
        )}
      </div>
    );
  }

  return (
    <div
      className="flex items-center gap-1.5 text-[10px] font-mono px-2 py-1 rounded bg-[#f5a623]/10 text-[#f5a623]"
      title={data.gap_status}
    >
      <TrendingUp className="h-3 w-3 flex-none animate-pulse" />
      <span className="hidden md:inline tracking-wider font-bold">{gap_after_min}MIN GAP</span>
    </div>
  );
}

// ── Page ────────────────────────────────────────────────────────────────────
export default function ChartView() {
  const { symbol } = useParams<{ symbol: string }>();

  const { data: instrumentsResponse } = useListInstruments({
    query: { refetchInterval: 10000, queryKey: ["listInstruments"] },
  });
  const instrument = instrumentsResponse?.instruments.find((i) => i.symbol === symbol);

  const { liveCandle, livePrice, connected } = useChartWs(symbol);

  const { data: candlesResponse, isLoading: candlesLoading, isError } = useGetCandles(
    { symbol: symbol ?? "" },
    {
      query: {
        enabled:              !!symbol && connected,
        queryKey:             getGetCandlesQueryKey({ symbol: symbol ?? "" }),
        staleTime:            0,
        refetchOnWindowFocus: false,
        refetchInterval:      30_000,
      },
    },
  );

  const hasCandles    = (candlesResponse?.candles?.length ?? 0) > 0;
  const displayPrice  = livePrice ?? instrument?.currentPrice ?? null;
  const isLoadingData = !connected || (connected && candlesLoading);

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-[#131722]">
      {/* ── Header ── */}
      <header className="flex-none h-14 border-b border-[#2a2e39] bg-[#1e222d] px-4 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <Link
            href="/"
            className="p-1.5 hover:bg-[#2a2e39] rounded text-[#758696] hover:text-[#b2b5be] transition-colors flex-none"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>

          <div className="flex items-center gap-2 min-w-0">
            <h1 className="font-bold text-base leading-none whitespace-nowrap text-[#d1d4dc]">
              {instrument?.displayName ?? symbol}
            </h1>
            {instrument && (
              instrument.isOpen ? (
                <span className="inline-flex items-center gap-1 text-[9px] font-bold tracking-widest text-[#089981] bg-[#089981]/10 px-1.5 py-0.5 rounded uppercase">
                  <span className="h-1.5 w-1.5 rounded-full bg-[#089981] animate-pulse" />
                  LIVE
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 text-[9px] font-bold tracking-widest text-[#758696] bg-[#2a2e39] px-1.5 py-0.5 rounded uppercase">
                  <Clock className="h-2.5 w-2.5" />
                  CLOSED
                </span>
              )
            )}
          </div>

          {instrument && (
            <span className="text-[11px] text-[#758696] hidden sm:block">
              {instrument.category.toUpperCase()} · {instrument.groupName}
            </span>
          )}
        </div>

        <div className="flex items-center gap-3 flex-none">
          {instrument && (
            <>
              <div className="flex flex-col items-end font-mono">
                <span className="text-[9px] text-[#758696] tracking-wider mb-0.5">PRICE</span>
                <span className="text-base font-bold leading-none tabular-nums text-[#d1d4dc]">
                  {formatPrice(displayPrice, instrument.precision)}
                </span>
              </div>

              <div className="flex flex-col items-end font-mono">
                <span className="text-[9px] text-[#758696] tracking-wider mb-0.5">24H</span>
                <span
                  className={cn(
                    "text-sm font-semibold leading-none",
                    instrument.changePercent24h > 0 ? "text-[#089981]"
                    : instrument.changePercent24h < 0 ? "text-[#f23645]"
                    : "text-[#758696]",
                  )}
                >
                  {formatChange(instrument.changePercent24h)}
                </span>
              </div>

              <div className="flex items-center gap-3 font-mono text-sm border-l border-[#2a2e39] pl-3">
                <div className="flex flex-col items-center">
                  <span className="text-[9px] text-[#758696] tracking-wider mb-0.5">TURBO</span>
                  <span className="text-[#2962ff] font-bold">{formatPercent(instrument.turboPayoutRate)}</span>
                </div>
                <div className="h-6 w-px bg-[#2a2e39]" />
                <div className="flex flex-col items-center">
                  <span className="text-[9px] text-[#758696] tracking-wider mb-0.5">BLITZ</span>
                  <span className="text-[#2962ff] font-bold">{formatPercent(instrument.blitzPayoutRate)}</span>
                </div>
              </div>
            </>
          )}

          {/* Gap status */}
          {candlesResponse && (
            <div className="border-l border-[#2a2e39] pl-3">
              <GapBadge data={candlesResponse} />
            </div>
          )}

          {/* WS status */}
          <div
            className={cn(
              "flex items-center gap-1.5 text-[10px] font-mono px-2 py-1 rounded",
              connected
                ? "text-[#089981] bg-[#089981]/10"
                : "text-[#758696] bg-[#2a2e39]/50",
            )}
            title={connected ? "Live feed connected" : "Connecting…"}
          >
            {connected
              ? <Wifi className="h-3.5 w-3.5" />
              : <WifiOff className="h-3.5 w-3.5 animate-pulse" />
            }
            <span className="hidden sm:inline tracking-wider font-bold">
              {connected ? "LIVE" : "…"}
            </span>
          </div>
        </div>
      </header>

      {/* ── Gap info bar (when data loaded) ── */}
      {candlesResponse && !candlesResponse.market_closed && (
        <div className="flex-none border-b border-[#2a2e39]/50 bg-[#1a1e2d] px-4 py-1.5 flex items-center gap-4 text-[10px] font-mono text-[#758696]">
          <span className="flex items-center gap-1.5">
            <span className="text-[#4a5568] uppercase tracking-wider">REST</span>
            <span className="text-[#d1d4dc]">{candlesResponse.count - candlesResponse.tick_candles_filled}</span>
          </span>
          <span className="text-[#2a2e39]">·</span>
          <span className="flex items-center gap-1.5">
            <span className="text-[#4a5568] uppercase tracking-wider">Ticks</span>
            <span className="text-[#d1d4dc]">{candlesResponse.tick_count.toLocaleString()}</span>
          </span>
          <span className="text-[#2a2e39]">·</span>
          <span className="flex items-center gap-1.5">
            <span className="text-[#4a5568] uppercase tracking-wider">Filled</span>
            <span className={cn(
              candlesResponse.tick_candles_filled > 0 ? "text-[#089981]" : "text-[#758696]"
            )}>
              {candlesResponse.tick_candles_filled} candles
            </span>
          </span>
          <span className="text-[#2a2e39]">·</span>
          <span className="flex items-center gap-1.5">
            <span className="text-[#4a5568] uppercase tracking-wider">Gap</span>
            <span className={cn(
              candlesResponse.gap_before_min > 0 ? "text-[#f5a623]" : "text-[#758696]"
            )}>
              {candlesResponse.gap_before_min}m
            </span>
            <span className="text-[#758696]">→</span>
            <span className={cn(
              candlesResponse.gap_after_min <= 1 ? "text-[#089981]" : "text-[#f5a623]"
            )}>
              {candlesResponse.gap_after_min}m
            </span>
          </span>
          <span className="text-[#2a2e39]">·</span>
          <span className="flex-1 truncate text-[#d1d4dc]/60" title={candlesResponse.gap_status}>
            {candlesResponse.gap_status}
          </span>
        </div>
      )}

      {/* ── Chart area ── */}
      <main className="flex-1 relative overflow-hidden bg-[#131722]">
        {/* Loading overlay */}
        {isLoadingData && !isError && (
          <div className="absolute inset-0 flex items-center justify-center bg-[#131722]/95 z-10">
            <div className="flex flex-col items-center gap-3">
              <Activity className="h-6 w-6 text-[#2962ff] animate-pulse" />
              <p className="font-mono text-xs tracking-widest text-[#758696] animate-pulse">
                {!connected ? "CONNECTING TO LIVE FEED…" : "LOADING MARKET DATA…"}
              </p>
            </div>
          </div>
        )}

        {/* Error */}
        {isError && !hasCandles && (
          <div className="absolute inset-0 flex items-center justify-center z-10">
            <div className="flex flex-col items-center gap-3 p-6 bg-[#1e222d] border border-[#f23645]/20 rounded-lg">
              <AlertCircle className="h-6 w-6 text-[#f23645]" />
              <p className="text-sm font-medium text-[#d1d4dc]">Failed to load market data</p>
              <p className="text-xs text-[#758696]">Check your connection and try again</p>
            </div>
          </div>
        )}

        {hasCandles && (
          <div className="w-full h-full">
            <Chart candles={candlesResponse!.candles!} liveCandle={liveCandle} />
          </div>
        )}
      </main>
    </div>
  );
}
