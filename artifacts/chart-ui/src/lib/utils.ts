import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatPrice(price: number | null | undefined, precision = 5) {
  if (price === null || price === undefined) return "—";
  return price.toFixed(precision);
}

export function formatPercent(rate: number) {
  return `${(rate * 100).toFixed(0)}%`;
}

export function formatChange(change: number) {
  const prefix = change > 0 ? "+" : "";
  return `${prefix}${change.toFixed(2)}%`;
}
