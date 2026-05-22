import React, { useState } from "react";
import { Link } from "wouter";
import { useListInstruments } from "@workspace/api-client-react";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { formatPrice, formatPercent, formatChange, cn } from "@/lib/utils";
import { Search, Activity, Clock, Percent } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

export default function InstrumentList() {
  const { data: response, isLoading, isError } = useListInstruments({
    query: { refetchInterval: 2000, queryKey: ["listInstruments"] },
  });
  const [search, setSearch] = useState("");
  const [showAll, setShowAll] = useState(false);

  if (isError) {
    return (
      <div className="p-6 text-center text-destructive">
        <p>Failed to load instruments.</p>
      </div>
    );
  }

  const instruments = response?.instruments || [];

  const filtered = instruments.filter(inst => {
    if (!showAll && !inst.isOpen) return false;
    if (search && !inst.displayName.toLowerCase().includes(search.toLowerCase()) && !inst.symbol.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="max-w-7xl mx-auto p-4 md:p-6 space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-foreground">Markets</h1>
          <p className="text-muted-foreground text-sm">Real-time forex and crypto instruments</p>
        </div>
        
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            <Switch id="show-all" checked={showAll} onCheckedChange={setShowAll} />
            <Label htmlFor="show-all" className="text-sm font-medium cursor-pointer">Show Closed</Label>
          </div>
          
          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input 
              placeholder="Search pairs..." 
              className="pl-9 bg-card border-card-border"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-1">
        <div className="grid grid-cols-12 gap-4 px-4 py-2 text-xs font-mono text-muted-foreground border-b border-border uppercase tracking-wider">
          <div className="col-span-3">Instrument</div>
          <div className="col-span-2 text-right">Price</div>
          <div className="col-span-2 text-right">24h Change</div>
          <div className="col-span-3 text-center">Payout (Turbo/Blitz)</div>
          <div className="col-span-2 text-right">Status</div>
        </div>

        {isLoading ? (
          Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="h-16 bg-card/50 animate-pulse rounded border border-card-border" />
          ))
        ) : (
          filtered.map((inst) => (
            <Link 
              key={inst.symbol} 
              href={`/chart/${inst.symbol}`}
              className={cn(
                "grid grid-cols-12 gap-4 items-center p-4 rounded bg-card border border-card-border transition-colors hover:bg-accent/50 cursor-pointer",
                !inst.isOpen && "opacity-60 grayscale-[0.5]"
              )}
            >
              <div className="col-span-3 flex flex-col">
                <span className="font-bold text-foreground text-base leading-tight">{inst.displayName}</span>
                <span className="text-xs text-muted-foreground">{inst.category.toUpperCase()}</span>
              </div>
              
              <div className="col-span-2 text-right font-mono">
                <span className="text-foreground">{formatPrice(inst.currentPrice, inst.precision)}</span>
              </div>
              
              <div className="col-span-2 text-right font-mono flex flex-col items-end">
                <span className={cn(
                  "text-sm font-medium",
                  inst.changePercent24h > 0 ? "text-bullish" : inst.changePercent24h < 0 ? "text-bearish" : "text-muted-foreground"
                )}>
                  {formatChange(inst.changePercent24h)}
                </span>
              </div>
              
              <div className="col-span-3 flex justify-center gap-3 font-mono text-sm">
                <div className="bg-primary/10 text-primary px-2 py-0.5 rounded flex items-center gap-1">
                  <Activity className="h-3 w-3" />
                  {formatPercent(inst.turboPayoutRate)}
                </div>
                <div className="bg-primary/10 text-primary px-2 py-0.5 rounded flex items-center gap-1">
                  <Percent className="h-3 w-3" />
                  {formatPercent(inst.blitzPayoutRate)}
                </div>
              </div>
              
              <div className="col-span-2 text-right flex justify-end">
                {inst.isOpen ? (
                  <span className="inline-flex items-center gap-1.5 text-xs font-medium text-bullish bg-bullish/10 px-2.5 py-1 rounded-full">
                    <span className="h-1.5 w-1.5 rounded-full bg-bullish animate-pulse"></span>
                    OPEN
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground bg-muted px-2.5 py-1 rounded-full">
                    <Clock className="h-3 w-3" />
                    CLOSED
                  </span>
                )}
              </div>
            </Link>
          ))
        )}
        
        {!isLoading && filtered.length === 0 && (
          <div className="text-center p-12 text-muted-foreground border border-dashed border-border rounded">
            No instruments found matching your criteria.
          </div>
        )}
      </div>
    </div>
  );
}
