// ── Technical indicator pure functions ──

import type {
  BollingerResult,
  MACDResult,
  MAResult,
  RSIResult,
} from "../types.ts";

/**
 * Simple Moving Average (SMA)
 * @param prices Close prices (oldest first)
 * @param period Number of periods
 */
export function sma(prices: number[], period: number): MAResult {
  const values: number[] = [];
  for (let i = period - 1; i < prices.length; i++) {
    let sum = 0;
    for (let j = i - period + 1; j <= i; j++) {
      sum += prices[j]!;
    }
    values.push(sum / period);
  }
  return {
    period,
    values,
    current: values.length > 0 ? values[values.length - 1]! : 0,
  };
}

/**
 * Exponential Moving Average (EMA)
 * @param prices Close prices (oldest first)
 * @param period Number of periods
 */
export function ema(prices: number[], period: number): MAResult {
  if (prices.length < period) {
    return { period, values: [], current: 0 };
  }

  const multiplier = 2 / (period + 1);
  const values: number[] = [];

  // First EMA value is SMA of first `period` prices
  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += prices[i]!;
  }
  let prev = sum / period;
  values.push(prev);

  // Subsequent values use EMA formula
  for (let i = period; i < prices.length; i++) {
    const current = (prices[i]! - prev) * multiplier + prev;
    values.push(current);
    prev = current;
  }

  return {
    period,
    values,
    current: values.length > 0 ? values[values.length - 1]! : 0,
  };
}

/**
 * Relative Strength Index (RSI)
 * @param prices Close prices (oldest first)
 * @param period Number of periods (typically 14)
 */
export function rsi(prices: number[], period: number): RSIResult {
  if (prices.length < period + 1) {
    return { period, values: [], current: 50 };
  }

  const values: number[] = [];
  let avgGain = 0;
  let avgLoss = 0;

  // Calculate initial average gain/loss
  for (let i = 1; i <= period; i++) {
    const change = prices[i]! - prices[i - 1]!;
    if (change > 0) avgGain += change;
    else avgLoss += Math.abs(change);
  }
  avgGain /= period;
  avgLoss /= period;

  // First RSI value
  const firstRS = avgLoss === 0 ? 100 : avgGain / avgLoss;
  values.push(100 - 100 / (1 + firstRS));

  // Subsequent values using smoothed averages
  for (let i = period + 1; i < prices.length; i++) {
    const change = prices[i]! - prices[i - 1]!;
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? Math.abs(change) : 0;

    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;

    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    values.push(100 - 100 / (1 + rs));
  }

  return {
    period,
    values,
    current: values.length > 0 ? values[values.length - 1]! : 50,
  };
}

/**
 * Bollinger Bands
 * @param prices Close prices (oldest first)
 * @param period Number of periods (typically 20)
 * @param stdDevMultiplier Standard deviation multiplier (typically 2)
 */
export function bollingerBands(
  prices: number[],
  period: number,
  stdDevMultiplier = 2,
): BollingerResult {
  const smaResult = sma(prices, period);
  const upper: number[] = [];
  const middle: number[] = [];
  const lower: number[] = [];

  for (let i = period - 1; i < prices.length; i++) {
    const smaIdx = i - period + 1;
    const mean = smaResult.values[smaIdx]!;

    // Calculate standard deviation for this window
    let sumSqDiff = 0;
    for (let j = i - period + 1; j <= i; j++) {
      sumSqDiff += (prices[j]! - mean) ** 2;
    }
    const stdDev = Math.sqrt(sumSqDiff / period);

    middle.push(mean);
    upper.push(mean + stdDevMultiplier * stdDev);
    lower.push(mean - stdDevMultiplier * stdDev);
  }

  const lastIdx = middle.length - 1;
  const currentMiddle = lastIdx >= 0 ? middle[lastIdx]! : 0;
  const currentUpper = lastIdx >= 0 ? upper[lastIdx]! : 0;
  const currentLower = lastIdx >= 0 ? lower[lastIdx]! : 0;
  const bandwidth =
    currentMiddle > 0
      ? (currentUpper - currentLower) / currentMiddle
      : 0;

  return {
    period,
    stdDev: stdDevMultiplier,
    upper,
    middle,
    lower,
    currentUpper,
    currentMiddle,
    currentLower,
    bandwidth,
  };
}

/**
 * MACD (Moving Average Convergence Divergence)
 * @param prices Close prices (oldest first)
 * @param fastPeriod Fast EMA period (typically 12)
 * @param slowPeriod Slow EMA period (typically 26)
 * @param signalPeriod Signal line period (typically 9)
 */
export function macd(
  prices: number[],
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9,
): MACDResult {
  const fastEMA = ema(prices, fastPeriod);
  const slowEMA = ema(prices, slowPeriod);

  // MACD Line = Fast EMA - Slow EMA
  // Align indices: slow EMA starts later
  const offset = slowPeriod - fastPeriod;
  const macdLine: number[] = [];
  for (let i = 0; i < slowEMA.values.length; i++) {
    macdLine.push(fastEMA.values[i + offset]! - slowEMA.values[i]!);
  }

  // Signal Line = EMA of MACD Line
  const signalEMA = ema(macdLine, signalPeriod);
  const signalLine = signalEMA.values;

  // Histogram = MACD Line - Signal Line
  const histogramOffset = signalPeriod - 1;
  const histogram: number[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    histogram.push(macdLine[i + histogramOffset]! - signalLine[i]!);
  }

  const lastMACD = macdLine.length > 0 ? macdLine[macdLine.length - 1]! : 0;
  const lastSignal =
    signalLine.length > 0 ? signalLine[signalLine.length - 1]! : 0;
  const lastHistogram =
    histogram.length > 0 ? histogram[histogram.length - 1]! : 0;

  return {
    macdLine,
    signalLine,
    histogram,
    currentMACD: lastMACD,
    currentSignal: lastSignal,
    currentHistogram: lastHistogram,
  };
}
