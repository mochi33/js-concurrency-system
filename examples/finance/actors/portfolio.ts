// ── PortfolioActor: Position management, P&L calculation ──

import type { Position, PortfolioSnapshot, Trade } from "../types.ts";
import { INITIAL_CASH } from "../config.ts";

/**
 * PortfolioActor manages positions and calculates P&L.
 *
 * Methods:
 *   buy(symbol, quantity, price) → Trade
 *   sell(symbol, quantity, price) → Trade
 *   updatePrices(prices: Record<string, number>) → void
 *   getSnapshot() → PortfolioSnapshot
 */
export class PortfolioActor {
  private cash = INITIAL_CASH;
  private positions = new Map<string, { quantity: number; avgCost: number }>();
  private currentPrices = new Map<string, number>();
  private trades: Trade[] = [];

  /**
   * Buy shares of a symbol.
   */
  buy(symbol: string, quantity: number, price: number): Trade {
    const cost = quantity * price;
    if (cost > this.cash) {
      throw new Error(
        `Insufficient cash: need $${cost.toFixed(2)}, have $${this.cash.toFixed(2)}`,
      );
    }

    this.cash -= cost;
    const pos = this.positions.get(symbol);
    if (pos) {
      const totalQty = pos.quantity + quantity;
      pos.avgCost = (pos.avgCost * pos.quantity + price * quantity) / totalQty;
      pos.quantity = totalQty;
    } else {
      this.positions.set(symbol, { quantity, avgCost: price });
    }
    this.currentPrices.set(symbol, price);

    const trade: Trade = {
      symbol,
      side: "buy",
      quantity,
      price,
      timestamp: new Date(),
    };
    this.trades.push(trade);
    return trade;
  }

  /**
   * Sell shares of a symbol.
   */
  sell(symbol: string, quantity: number, price: number): Trade {
    const pos = this.positions.get(symbol);
    if (!pos || pos.quantity < quantity) {
      throw new Error(
        `Insufficient position: ${symbol} have ${pos?.quantity ?? 0}, want to sell ${quantity}`,
      );
    }

    this.cash += quantity * price;
    pos.quantity -= quantity;
    if (pos.quantity === 0) {
      this.positions.delete(symbol);
    }
    this.currentPrices.set(symbol, price);

    const trade: Trade = {
      symbol,
      side: "sell",
      quantity,
      price,
      timestamp: new Date(),
    };
    this.trades.push(trade);
    return trade;
  }

  /**
   * Update current market prices for P&L calculation.
   */
  updatePrices(prices: Record<string, number>): void {
    for (const [symbol, price] of Object.entries(prices)) {
      this.currentPrices.set(symbol, price);
    }
  }

  /**
   * Get portfolio snapshot with current P&L.
   */
  getSnapshot(): PortfolioSnapshot {
    const positionsList: Position[] = [];
    let totalValue = this.cash;

    for (const [symbol, pos] of this.positions) {
      const currentPrice = this.currentPrices.get(symbol) ?? pos.avgCost;
      const marketValue = pos.quantity * currentPrice;
      const costBasis = pos.quantity * pos.avgCost;
      const unrealizedPnL = marketValue - costBasis;
      const unrealizedPnLPercent =
        costBasis > 0 ? (unrealizedPnL / costBasis) * 100 : 0;

      totalValue += marketValue;

      positionsList.push({
        symbol,
        quantity: pos.quantity,
        avgCost: pos.avgCost,
        currentPrice,
        unrealizedPnL,
        unrealizedPnLPercent,
      });
    }

    const totalUnrealizedPnL = positionsList.reduce(
      (sum, p) => sum + p.unrealizedPnL,
      0,
    );

    return {
      cash: this.cash,
      totalValue,
      totalUnrealizedPnL,
      positions: positionsList,
      timestamp: new Date(),
    };
  }
}
