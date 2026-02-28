// ── PriceFeedActor: Tick→OHLCV conversion, price history management ──

import type { OHLCV, Tick } from "../types.ts";
import { CANDLE_PERIOD_MS } from "../config.ts";

/**
 * PriceFeedActor accumulates ticks and produces OHLCV candles.
 *
 * Methods:
 *   processTick(tick) → OHLCV | null  (returns candle when period completes)
 *   getClosePrices(symbol, count) → number[]
 *   getHistory(symbol, count) → OHLCV[]
 *   getAllLatestPrices() → Record<string, number>
 */
export class PriceFeedActor {
  /** Completed candle history per symbol */
  private history = new Map<string, OHLCV[]>();

  /** In-progress candle being built per symbol */
  private building = new Map<string, {
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    startTime: Date;
    tickCount: number;
  }>();

  /** Latest known price per symbol */
  private latestPrices = new Map<string, number>();

  /**
   * Process an incoming tick.
   * Returns a completed OHLCV candle if the period has elapsed, else null.
   *
   * Note: Date objects are converted to ISO strings during RPC serialization
   * (Date.toJSON() runs before the Extended JSON replacer), so we accept
   * both Date and string for timestamp.
   */
  processTick(tick: Tick): OHLCV | null {
    // Ensure timestamp is a Date (may arrive as ISO string from RPC)
    const timestamp = tick.timestamp instanceof Date
      ? tick.timestamp
      : new Date(tick.timestamp as unknown as string);

    this.latestPrices.set(tick.symbol, tick.price);

    const building = this.building.get(tick.symbol);

    if (!building) {
      // Start a new candle
      this.building.set(tick.symbol, {
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price,
        volume: tick.volume,
        startTime: timestamp,
        tickCount: 1,
      });
      return null;
    }

    // Update current candle
    building.high = Math.max(building.high, tick.price);
    building.low = Math.min(building.low, tick.price);
    building.close = tick.price;
    building.volume += tick.volume;
    building.tickCount++;

    // Check if candle period has elapsed
    const elapsed = timestamp.getTime() - building.startTime.getTime();
    if (elapsed >= CANDLE_PERIOD_MS) {
      const candle: OHLCV = {
        symbol: tick.symbol,
        open: building.open,
        high: building.high,
        low: building.low,
        close: building.close,
        volume: building.volume,
        startTime: building.startTime,
        endTime: timestamp,
      };

      // Store in history
      const hist = this.history.get(tick.symbol) ?? [];
      hist.push(candle);
      // Keep last 200 candles per symbol
      if (hist.length > 200) hist.shift();
      this.history.set(tick.symbol, hist);

      // Start new candle
      this.building.delete(tick.symbol);

      return candle;
    }

    return null;
  }

  /**
   * Get close prices for a symbol (oldest first).
   */
  getClosePrices(symbol: string, count: number): number[] {
    const hist = this.history.get(symbol) ?? [];
    const slice = hist.slice(-count);
    return slice.map((c) => c.close);
  }

  /**
   * Get candle history for a symbol.
   */
  getHistory(symbol: string, count: number): OHLCV[] {
    const hist = this.history.get(symbol) ?? [];
    return hist.slice(-count);
  }

  /**
   * Get latest prices for all symbols.
   */
  getAllLatestPrices(): Record<string, number> {
    const result: Record<string, number> = {};
    for (const [symbol, price] of this.latestPrices) {
      result[symbol] = price;
    }
    return result;
  }
}
