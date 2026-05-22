import { useState, useEffect, useRef, useCallback } from "react";
import { useParams, Link } from "wouter";
import { useGetCandles, getGetCandlesQueryKey, useListInstruments } from "@workspace/api-client-react";
import { Chart, type LiveCandleData } from "@/components/Chart";
import { ArrowLeft, Activity, Clock, AlertCircle, Wifi, WifiOff } from "lucide-react";
import { formatPrice, formatPercent, formatChange, cn } from "@/lib/utils";

function useChartWs(symbol: string | undefined) {
  const [liveCandle, setLiveCandle] = useState<LiveCandleData | null>(null);
  const [livePrice, setLivePrice] = useState<number | null>(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const unmounted = useRef(false);

  const connect = useCallback(() => {
    if (!symbol || unmounted.current) return;

    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${window.location.host}/ws?symbol=${encodeURIComponent(symbol)}`;

    const ws = new WebSocket(url);
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
          timestamp?: number;
          candle?: LiveCandleData;
        };

        if ((msg.type === "tick" || msg.type === "candle") && msg.candle) {
          setLiveCandle(msg.candle);
          if (msg.price != null) setLivePrice(msg.price);
        }
      } catch {}
    };

    ws.onclose = () => {
      setConnected(false);
      if (!unmounted.current) {
        reconnectTimer.current = setTimeout(connect, 3000);
      }
    };

    ws.onerror = () => ws.close();
  }, [symbol]);

  useEffect(() => {
    unmounted.current = false;
    connect();
    return () => {
      unmounted.current = true;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return { liveCandle, livePrice, connected };
}

export default function ChartView() {
  const { symbol } = useParams<{ symbol: string }>();

  const { data: instrumentsResponse } = useListInstruments({
    query: { refetchInterval: 10000, queryKey: ["listInstruments"] },
  });
  const instrument = instrumentsResponse?.instruments.find((i) => i.symbol === symbol);

  const { data: candlesResponse, isLoading, isError } = useGetCandles(
    { symbol: symbol ?? "" },
    {
      query: {
        enabled: !!symbol,
        queryKey: getGetCandlesQueryKey({ symbol: symbol ?? "" }),
        refetchInterval: 30000,
        staleTime: 0,
        refetchOnWindowFocus: true,
      },
    },
  );

  const { liveCandle, livePrice, connected } = useChartWs(symbol);

  const displayPrice = livePrice ?? instrument?.currentPrice ?? null;

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-background">
      <header className="flex-none h-14 border-b border-border bg-card px-4 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <Link
            href="/"
            className="p-1.5 hover:bg-accent rounded text-muted-foreground hover:text-foreground transition-colors flex-none"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>

          <div className="flex items-center gap-2 min-w-0">
            <h1 className="font-bold text-lg leading-none whitespace-nowrap">
              {instrument?.displayName ?? symbol}
            </h1>
            {instrument && (
              instrument.isOpen ? (
                <span className="inline-flex items-center gap-1 text-[9px] font-bold tracking-widest text-emerald-400 bg-emerald-400/10 px-1.5 py-0.5 rounded uppercase">
                  <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                  LIVE
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 text-[9px] font-bold tracking-widest text-muted-foreground bg-muted px-1.5 py-0.5 rounded uppercase">
                  <Clock className="h-2.5 w-2.5" />
                  CLOSED
                </span>
              )
            )}
          </div>

          {instrument && (
            <span className="text-xs text-muted-foreground hidden sm:block">
              {instrument.category.toUpperCase()} • {instrument.groupName}
            </span>
          )}
        </div>

        <div className="flex items-center gap-5 flex-none">
          {instrument && (
            <>
              <div className="flex flex-col items-end font-mono">
                <span className="text-[9px] text-muted-foreground tracking-wider mb-0.5">PRICE</span>
                <span className="text-base font-bold leading-none tabular-nums">
                  {formatPrice(displayPrice, instrument.precision)}
                </span>
              </div>

              <div className="flex flex-col items-end font-mono">
                <span className="text-[9px] text-muted-foreground tracking-wider mb-0.5">24H</span>
                <span
                  className={cn(
                    "text-sm font-semibold leading-none",
                    instrument.changePercent24h > 0
                      ? "text-emerald-400"
                      : instrument.changePercent24h < 0
                      ? "text-red-400"
                      : "text-muted-foreground",
                  )}
                >
                  {formatChange(instrument.changePercent24h)}
                </span>
              </div>

              <div className="flex items-center gap-3 font-mono text-sm border-l border-border pl-5">
                <div className="flex flex-col items-center">
                  <span className="text-[9px] text-muted-foreground tracking-wider mb-0.5">TURBO</span>
                  <span className="text-cyan-400 font-bold">{formatPercent(instrument.turboPayoutRate)}</span>
                </div>
                <div className="h-6 w-px bg-border" />
                <div className="flex flex-col items-center">
                  <span className="text-[9px] text-muted-foreground tracking-wider mb-0.5">BLITZ</span>
                  <span className="text-cyan-400 font-bold">{formatPercent(instrument.blitzPayoutRate)}</span>
                </div>
              </div>
            </>
          )}

          <div
            className={cn(
              "flex items-center gap-1 text-xs font-mono",
              connected ? "text-emerald-400" : "text-muted-foreground",
            )}
            title={connected ? "Live feed connected" : "Connecting..."}
          >
            {connected ? <Wifi className="h-3.5 w-3.5" /> : <WifiOff className="h-3.5 w-3.5" />}
          </div>
        </div>
      </header>

      <main className="flex-1 relative overflow-hidden">
        {isLoading && !candlesResponse && (
          <div className="absolute inset-0 flex items-center justify-center bg-background/80 backdrop-blur-sm z-10">
            <div className="flex flex-col items-center text-cyan-400">
              <Activity className="h-7 w-7 animate-pulse mb-3" />
              <p className="font-mono text-sm animate-pulse tracking-wider">Loading market data...</p>
            </div>
          </div>
        )}

        {isError && !candlesResponse && (
          <div className="absolute inset-0 flex items-center justify-center z-10">
            <div className="flex flex-col items-center text-red-400 p-6 bg-card border border-red-400/20 rounded-lg">
              <AlertCircle className="h-7 w-7 mb-3" />
              <p className="font-medium">Failed to load market data</p>
              <p className="text-xs text-muted-foreground mt-1">Check your connection and try again</p>
            </div>
          </div>
        )}

        {candlesResponse?.candles && candlesResponse.candles.length > 0 ? (
          <div className="w-full h-full">
            <Chart candles={candlesResponse.candles} liveCandle={liveCandle} />
          </div>
        ) : (
          !isLoading && (
            <div className="w-full h-full flex items-center justify-center text-muted-foreground font-mono text-sm">
              No candle data available
            </div>
          )
        )}
      </main>
    </div>
  );
}
