// ── Configuration for the finance example ──

import type { AlertRule } from "./types.ts";

/** Discovery server port */
export const DISCOVERY_PORT = 19877;

/** Minimum worker nodes */
export const MIN_NODES = 2;

/** Maximum worker nodes */
export const MAX_NODES = 4;

/** Extra overflow nodes for nested spawn deadlock prevention */
export const OVERFLOW_MAX = 4;

/** Tick interval in milliseconds per symbol */
export const TICK_INTERVAL_MS = 500;

/** Candle period in milliseconds (how many ticks form one candle) */
export const CANDLE_PERIOD_MS = 15_000; // 15 seconds for demo

/** Minimum close prices required before computing indicators */
export const MIN_PRICES_FOR_INDICATORS = 10;

/** Initial cash for portfolio */
export const INITIAL_CASH = 100_000;

/** Symbols to monitor */
export const WATCH_SYMBOLS = [
  "BTC",
  "ETH",
  "SOL",
  "AAPL",
  "TSLA",
  "NVDA",
];

/** Default alert rules */
export const DEFAULT_ALERT_RULES: AlertRule[] = [
  {
    id: "btc-high",
    symbol: "BTC",
    condition: { type: "price_above", threshold: 105_000 },
    message: "BTC exceeded $105,000",
    enabled: true,
  },
  {
    id: "btc-low",
    symbol: "BTC",
    condition: { type: "price_below", threshold: 95_000 },
    message: "BTC dropped below $95,000",
    enabled: true,
  },
  {
    id: "eth-high",
    symbol: "ETH",
    condition: { type: "price_above", threshold: 4_000 },
    message: "ETH exceeded $4,000",
    enabled: true,
  },
  {
    id: "rsi-overbought",
    symbol: "*",
    condition: { type: "rsi_overbought", threshold: 70 },
    message: "RSI overbought signal",
    enabled: true,
  },
  {
    id: "rsi-oversold",
    symbol: "*",
    condition: { type: "rsi_oversold", threshold: 30 },
    message: "RSI oversold signal",
    enabled: true,
  },
  {
    id: "bollinger-breakout",
    symbol: "*",
    condition: { type: "bollinger_breakout" },
    message: "Bollinger Band breakout detected",
    enabled: true,
  },
  {
    id: "macd-crossover",
    symbol: "*",
    condition: { type: "macd_crossover" },
    message: "MACD crossover signal",
    enabled: true,
  },
];

/** How often to display portfolio snapshot (in ms) */
export const PORTFOLIO_DISPLAY_INTERVAL_MS = 30_000;

/** Run duration before auto-shutdown (in ms). 0 = run until Ctrl+C */
export const RUN_DURATION_MS = 0;
