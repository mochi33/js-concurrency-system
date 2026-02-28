// ── AlertManagerActor: Rule management, condition evaluation ──

import type {
  AlertCondition,
  AlertEvent,
  AlertRule,
  IndicatorBundle,
} from "../types.ts";

/**
 * AlertManagerActor evaluates alert rules against price/indicator data.
 *
 * Methods:
 *   addRule(rule) → void
 *   removeRule(ruleId) → void
 *   evaluate(symbol, price, indicators?) → AlertEvent[]
 *   getRules() → AlertRule[]
 */
export class AlertManagerActor {
  private rules: AlertRule[] = [];
  /** Track previous MACD histogram values for crossover detection */
  private prevHistogram = new Map<string, number>();

  /**
   * Add an alert rule.
   */
  addRule(rule: AlertRule): void {
    // Replace if same id exists
    this.rules = this.rules.filter((r) => r.id !== rule.id);
    this.rules.push(rule);
  }

  /**
   * Remove an alert rule by id.
   */
  removeRule(ruleId: string): void {
    this.rules = this.rules.filter((r) => r.id !== ruleId);
  }

  /**
   * Evaluate all matching rules for a symbol.
   * Returns fired alerts.
   */
  evaluate(
    symbol: string,
    price: number,
    indicators?: IndicatorBundle | null,
  ): AlertEvent[] {
    const events: AlertEvent[] = [];
    const matchingRules = this.rules.filter(
      (r) => r.enabled && (r.symbol === symbol || r.symbol === "*"),
    );

    for (const rule of matchingRules) {
      if (this.checkCondition(rule.condition, symbol, price, indicators)) {
        events.push({
          ruleId: rule.id,
          symbol,
          message: `[${symbol}] ${rule.message} (price: $${price.toFixed(2)})`,
          price,
          timestamp: new Date(),
        });
      }
    }

    // Update previous histogram for MACD crossover detection
    if (indicators) {
      this.prevHistogram.set(symbol, indicators.macd.currentHistogram);
    }

    return events;
  }

  /**
   * Get all rules.
   */
  getRules(): AlertRule[] {
    return [...this.rules];
  }

  private checkCondition(
    condition: AlertCondition,
    symbol: string,
    price: number,
    indicators?: IndicatorBundle | null,
  ): boolean {
    switch (condition.type) {
      case "price_above":
        return price > condition.threshold;

      case "price_below":
        return price < condition.threshold;

      case "rsi_overbought":
        if (!indicators) return false;
        return indicators.rsi14.current > condition.threshold;

      case "rsi_oversold":
        if (!indicators) return false;
        return indicators.rsi14.current < condition.threshold;

      case "bollinger_breakout":
        if (!indicators) return false;
        return price > indicators.bollinger.currentUpper ||
          price < indicators.bollinger.currentLower;

      case "macd_crossover": {
        if (!indicators) return false;
        const prevHist = this.prevHistogram.get(symbol);
        if (prevHist === undefined) return false;
        const currHist = indicators.macd.currentHistogram;
        // Crossover: histogram changes sign
        return (prevHist < 0 && currHist >= 0) ||
          (prevHist >= 0 && currHist < 0);
      }

      default:
        return false;
    }
  }
}
