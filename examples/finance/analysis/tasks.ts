// ── Distributed tasks: individual indicator computations + fan-out orchestrator ──

import type { Context } from "../../../src/types.ts";
import type {
  BollingerResult,
  IndicatorBundle,
  MACDResult,
  MAResult,
  RSIResult,
} from "../types.ts";
import {
  bollingerBands,
  ema,
  macd,
  rsi,
  sma,
} from "./indicators.ts";

/**
 * Compute SMA for given prices and period.
 * Distributed task — runs on a worker node.
 */
export async function compute_sma(
  _ctx: Context,
  prices: number[],
  period: number,
): Promise<MAResult> {
  return sma(prices, period);
}

/**
 * Compute EMA for given prices and period.
 */
export async function compute_ema(
  _ctx: Context,
  prices: number[],
  period: number,
): Promise<MAResult> {
  return ema(prices, period);
}

/**
 * Compute RSI for given prices and period.
 */
export async function compute_rsi(
  _ctx: Context,
  prices: number[],
  period: number,
): Promise<RSIResult> {
  return rsi(prices, period);
}

/**
 * Compute Bollinger Bands.
 */
export async function compute_bollinger(
  _ctx: Context,
  prices: number[],
  period: number,
  stdDev: number,
): Promise<BollingerResult> {
  return bollingerBands(prices, period, stdDev);
}

/**
 * Compute MACD.
 */
export async function compute_macd(
  _ctx: Context,
  prices: number[],
  fastPeriod: number,
  slowPeriod: number,
  signalPeriod: number,
): Promise<MACDResult> {
  return macd(prices, fastPeriod, slowPeriod, signalPeriod);
}

/**
 * Fan-out orchestrator: spawns 7 indicator computations in parallel
 * via nested ctx.spawn(), collects results with Promise.all.
 *
 * This task runs on a worker node and uses nested spawn to distribute
 * the 7 sub-computations across the cluster.
 */
export async function compute_all_indicators(
  ctx: Context,
  prices: number[],
  symbol: string,
): Promise<IndicatorBundle> {
  // Fan-out: spawn 7 parallel sub-tasks
  const sma20Ch = ctx.spawn("compute_sma", [prices, 20]);
  const sma50Ch = ctx.spawn("compute_sma", [prices, 50]);
  const ema12Ch = ctx.spawn("compute_ema", [prices, 12]);
  const ema26Ch = ctx.spawn("compute_ema", [prices, 26]);
  const rsi14Ch = ctx.spawn("compute_rsi", [prices, 14]);
  const bollingerCh = ctx.spawn("compute_bollinger", [prices, 20, 2]);
  const macdCh = ctx.spawn("compute_macd", [prices, 12, 26, 9]);

  // Fan-in: wait for all results
  const [sma20, sma50, ema12, ema26, rsi14, bollinger, macdResult] =
    await Promise.all([
      sma20Ch.join(),
      sma50Ch.join(),
      ema12Ch.join(),
      ema26Ch.join(),
      rsi14Ch.join(),
      bollingerCh.join(),
      macdCh.join(),
    ]) as [
      MAResult,
      MAResult,
      MAResult,
      MAResult,
      RSIResult,
      BollingerResult,
      MACDResult,
    ];

  return {
    symbol,
    timestamp: new Date(),
    sma20,
    sma50,
    ema12,
    ema26,
    rsi14,
    bollinger,
    macd: macdResult,
  };
}

/**
 * Batch analysis: process multiple symbols in parallel (2-level fan-out).
 * Each symbol spawns its own compute_all_indicators task.
 */
export async function analyze_batch(
  ctx: Context,
  symbolPrices: Array<{ symbol: string; prices: number[] }>,
): Promise<IndicatorBundle[]> {
  const channels = symbolPrices.map(({ symbol, prices }) =>
    ctx.spawn("compute_all_indicators", [prices, symbol])
  );

  const results = await Promise.all(
    channels.map((ch) => ch.join()),
  );

  return results as IndicatorBundle[];
}
