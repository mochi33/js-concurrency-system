// ── Data sources: MockDataSource with geometric Brownian motion ──

import type { DataSource, Tick } from "../types.ts";
import { DEFAULT_SYMBOLS, type SymbolInfo } from "./symbols.ts";
import { TICK_INTERVAL_MS } from "../config.ts";

/**
 * MockDataSource generates realistic price ticks using
 * geometric Brownian motion (GBM) simulation.
 *
 * Each symbol gets an independent random walk starting from its base price.
 */
export class MockDataSource implements DataSource {
  private timers: number[] = [];
  private currentPrices = new Map<string, number>();
  private symbolMap = new Map<string, SymbolInfo>();

  constructor(symbols?: SymbolInfo[]) {
    for (const info of symbols ?? DEFAULT_SYMBOLS) {
      this.symbolMap.set(info.symbol, info);
      this.currentPrices.set(info.symbol, info.basePrice);
    }
  }

  start(symbols: string[], onTick: (tick: Tick) => void): void {
    for (const symbol of symbols) {
      const info = this.symbolMap.get(symbol);
      if (!info) continue;

      // Stagger start times slightly to avoid all ticks arriving simultaneously
      const offset = Math.random() * 100;

      const timer = setTimeout(() => {
        const interval = setInterval(() => {
          const tick = this.generateTick(info);
          onTick(tick);
        }, TICK_INTERVAL_MS);
        this.timers.push(interval);
      }, offset);
      this.timers.push(timer);
    }
  }

  stop(): void {
    for (const timer of this.timers) {
      clearInterval(timer);
      clearTimeout(timer);
    }
    this.timers = [];
  }

  private generateTick(info: SymbolInfo): Tick {
    const currentPrice = this.currentPrices.get(info.symbol)!;

    // GBM: dS = μ·S·dt + σ·S·dW
    // For sub-second intervals, use small dt
    const dt = TICK_INTERVAL_MS / (365 * 24 * 3600 * 1000);
    const drift = 0; // zero drift for simulation
    const diffusion = info.volatility * Math.sqrt(dt) * gaussianRandom();

    const newPrice = currentPrice * Math.exp(drift * dt + diffusion);
    // Ensure price stays positive and reasonable
    const clampedPrice = Math.max(newPrice, info.basePrice * 0.5);
    this.currentPrices.set(info.symbol, clampedPrice);

    // Volume: random around typical with some variance
    const volume = info.typicalVolume * (0.5 + Math.random());

    return {
      symbol: info.symbol,
      price: roundPrice(clampedPrice, info.type === "crypto" ? 2 : 2),
      volume: Math.round(volume),
      timestamp: new Date(),
    };
  }
}

/**
 * Placeholder for a real API adapter.
 * Implement this to connect to actual market data feeds.
 */
export class RealApiAdapter implements DataSource {
  start(_symbols: string[], _onTick: (tick: Tick) => void): void {
    throw new Error(
      "RealApiAdapter is a placeholder. Implement with your preferred API " +
        "(e.g., Binance WebSocket, Alpaca, Polygon.io)",
    );
  }

  stop(): void {
    // no-op
  }
}

/** Box-Muller transform for standard normal random variable */
function gaussianRandom(): number {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

function roundPrice(price: number, decimals: number): number {
  const factor = Math.pow(10, decimals);
  return Math.round(price * factor) / factor;
}
