// ── Common type definitions for the finance example ──

/** Raw price tick from a data source */
export interface Tick {
  symbol: string;
  price: number;
  volume: number;
  timestamp: Date;
}

/** OHLCV candlestick */
export interface OHLCV {
  symbol: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  startTime: Date;
  endTime: Date;
}

/** Simple Moving Average result */
export interface MAResult {
  period: number;
  values: number[];
  current: number;
}

/** RSI result */
export interface RSIResult {
  period: number;
  values: number[];
  current: number;
}

/** Bollinger Bands result */
export interface BollingerResult {
  period: number;
  stdDev: number;
  upper: number[];
  middle: number[];
  lower: number[];
  currentUpper: number;
  currentMiddle: number;
  currentLower: number;
  bandwidth: number;
}

/** MACD result */
export interface MACDResult {
  macdLine: number[];
  signalLine: number[];
  histogram: number[];
  currentMACD: number;
  currentSignal: number;
  currentHistogram: number;
}

/** Bundle of all indicators for a symbol */
export interface IndicatorBundle {
  symbol: string;
  timestamp: Date;
  sma20: MAResult;
  sma50: MAResult;
  ema12: MAResult;
  ema26: MAResult;
  rsi14: RSIResult;
  bollinger: BollingerResult;
  macd: MACDResult;
}

/** Alert condition types */
export type AlertCondition =
  | { type: "price_above"; threshold: number }
  | { type: "price_below"; threshold: number }
  | { type: "rsi_overbought"; threshold: number }
  | { type: "rsi_oversold"; threshold: number }
  | { type: "bollinger_breakout" }
  | { type: "macd_crossover" };

/** Alert rule definition */
export interface AlertRule {
  id: string;
  symbol: string;
  condition: AlertCondition;
  message: string;
  enabled: boolean;
}

/** Fired alert event */
export interface AlertEvent {
  ruleId: string;
  symbol: string;
  message: string;
  price: number;
  timestamp: Date;
}

/** Portfolio position */
export interface Position {
  symbol: string;
  quantity: number;
  avgCost: number;
  currentPrice: number;
  unrealizedPnL: number;
  unrealizedPnLPercent: number;
}

/** Portfolio snapshot */
export interface PortfolioSnapshot {
  cash: number;
  totalValue: number;
  totalUnrealizedPnL: number;
  positions: Position[];
  timestamp: Date;
}

/** Trade record */
export interface Trade {
  symbol: string;
  side: "buy" | "sell";
  quantity: number;
  price: number;
  timestamp: Date;
}

/** Data source interface */
export interface DataSource {
  start(symbols: string[], onTick: (tick: Tick) => void): void;
  stop(): void;
}

/** Pipeline progress event */
export interface PipelineProgress {
  type: "tick" | "candle" | "indicators" | "alert" | "portfolio";
  symbol?: string;
  message: string;
  timestamp: Date;
}
