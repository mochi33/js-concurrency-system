// ── Pipeline orchestrator: DataSource → Actor → Task → Alert ──

import { ProcessNode } from "../../../src/node.ts";
import { Logger } from "../../../src/logger.ts";
import type { ActorHandle } from "../../../src/types.ts";
import type {
  AlertEvent,
  DataSource,
  IndicatorBundle,
  PortfolioSnapshot,
  Tick,
} from "../types.ts";
import { DEFAULT_ALERT_RULES } from "../config.ts";
import {
  MIN_PRICES_FOR_INDICATORS,
  PORTFOLIO_DISPLAY_INTERVAL_MS,
} from "../config.ts";

export class Orchestrator {
  private priceFeed: ActorHandle | null = null;
  private portfolio: ActorHandle | null = null;
  private alertManager: ActorHandle | null = null;
  private running = false;
  private tickCount = 0;
  private candleCount = 0;
  private alertCount = 0;
  private portfolioTimer: number | null = null;
  private log: Logger;

  constructor(
    private node: ProcessNode,
    private dataSource: DataSource,
    private symbols: string[],
  ) {
    this.log = new Logger({
      fields: { component: "orchestrator" },
    });
  }

  /**
   * Initialize actors and start the pipeline.
   */
  async start(): Promise<void> {
    this.running = true;
    this.log.info("Initializing actors...");

    // Create actors on worker nodes
    this.priceFeed = await this.node.createActor("PriceFeedActor");
    this.log.info("PriceFeedActor created");

    this.portfolio = await this.node.createActor("PortfolioActor");
    this.log.info("PortfolioActor created");

    this.alertManager = await this.node.createActor("AlertManagerActor");
    this.log.info("AlertManagerActor created");

    // Load default alert rules
    for (const rule of DEFAULT_ALERT_RULES) {
      await this.alertManager.call("addRule", rule);
    }
    this.log.info(`Loaded ${DEFAULT_ALERT_RULES.length} alert rules`);

    // Start periodic portfolio display
    this.portfolioTimer = setInterval(() => {
      this.displayPortfolio();
    }, PORTFOLIO_DISPLAY_INTERVAL_MS);

    // Start data source — ticks flow into onTick
    this.log.info(`Starting data feed for ${this.symbols.length} symbols...`);
    this.dataSource.start(this.symbols, (tick) => {
      if (this.running) {
        this.onTick(tick).catch((err) => {
          this.log.error(`Tick processing error: ${err.message}`);
        });
      }
    });

    this.log.info("Pipeline started");
  }

  /**
   * Process a single tick through the pipeline.
   */
  private async onTick(tick: Tick): Promise<void> {
    if (!this.running || !this.priceFeed || !this.portfolio || !this.alertManager) {
      return;
    }

    this.tickCount++;

    // 1. Feed tick to PriceFeedActor → may produce a completed candle
    const candle = await this.priceFeed.call("processTick", tick);

    // 2. If candle completed, run indicator analysis
    if (candle !== null) {
      this.candleCount++;
      this.log.info(
        `Candle completed: ${tick.symbol} O=${(candle as { open: number }).open} H=${(candle as { high: number }).high} L=${(candle as { low: number }).low} C=${(candle as { close: number }).close}`,
        { symbol: tick.symbol },
      );

      // Get close prices for indicator calculation
      const closePrices = (await this.priceFeed.call(
        "getClosePrices",
        tick.symbol,
        100,
      )) as number[];

      if (closePrices.length >= MIN_PRICES_FOR_INDICATORS) {
        await this.computeAndEvaluate(tick.symbol, tick.price, closePrices);
      } else {
        this.log.info(
          `${tick.symbol}: ${closePrices.length} candles (need ${MIN_PRICES_FOR_INDICATORS} for indicators)`,
        );

        // Still evaluate price-only alerts
        await this.evaluateAlerts(tick.symbol, tick.price);
      }
    }

    // 3. Update portfolio prices periodically (every 20 ticks)
    if (this.tickCount % 20 === 0) {
      const prices = (await this.priceFeed.call(
        "getAllLatestPrices",
      )) as Record<string, number>;
      await this.portfolio.call("updatePrices", prices);
    }
  }

  /**
   * Compute indicators via distributed spawn and evaluate alerts.
   */
  private async computeAndEvaluate(
    symbol: string,
    price: number,
    closePrices: number[],
  ): Promise<void> {
    try {
      // Store prices in ObjectStore for efficient sharing
      const ref = this.node.put(closePrices);

      // Spawn distributed indicator computation (fan-out/fan-in)
      const channel = this.node.spawn("compute_all_indicators", [
        ref,
        symbol,
      ]);

      // Stream progress (if any) and get final result
      const indicators = (await channel.join()) as IndicatorBundle;

      this.log.info(
        `Indicators computed: ${symbol} SMA20=${indicators.sma20.current.toFixed(2)} RSI=${indicators.rsi14.current.toFixed(1)} MACD=${indicators.macd.currentHistogram.toFixed(4)}`,
        { symbol },
      );

      // Evaluate alerts with full indicator data
      await this.evaluateAlerts(symbol, price, indicators);
    } catch (err) {
      this.log.error(
        `Indicator computation failed for ${symbol}: ${(err as Error).message}`,
      );
      // Fall back to price-only alerts
      await this.evaluateAlerts(symbol, price);
    }
  }

  /**
   * Evaluate alert rules and display fired alerts.
   */
  private async evaluateAlerts(
    symbol: string,
    price: number,
    indicators?: IndicatorBundle,
  ): Promise<void> {
    if (!this.alertManager) return;

    const events = (await this.alertManager.call(
      "evaluate",
      symbol,
      price,
      indicators ?? null,
    )) as AlertEvent[];

    for (const event of events) {
      this.alertCount++;
      console.log(
        `\n  ⚠  ALERT: ${event.message}`,
      );
    }
  }

  /**
   * Display portfolio snapshot.
   */
  private async displayPortfolio(): Promise<void> {
    if (!this.portfolio || !this.running) return;

    try {
      const snapshot = (await this.portfolio.call(
        "getSnapshot",
      )) as PortfolioSnapshot;

      console.log("\n┌─────────────── Portfolio Snapshot ───────────────┐");
      console.log(
        `│ Cash: $${snapshot.cash.toFixed(2).padStart(12)}                       │`,
      );
      console.log(
        `│ Total Value: $${snapshot.totalValue.toFixed(2).padStart(12)}                  │`,
      );
      console.log(
        `│ Unrealized P&L: $${snapshot.totalUnrealizedPnL.toFixed(2).padStart(12)}               │`,
      );

      if (snapshot.positions.length > 0) {
        console.log("│─────────────────────────────────────────────────│");
        for (const pos of snapshot.positions) {
          const pnlStr = pos.unrealizedPnL >= 0
            ? `+$${pos.unrealizedPnL.toFixed(2)}`
            : `-$${Math.abs(pos.unrealizedPnL).toFixed(2)}`;
          console.log(
            `│ ${pos.symbol.padEnd(6)} ${String(pos.quantity).padStart(6)} @ $${pos.currentPrice.toFixed(2).padStart(10)} ${pnlStr.padStart(12)} │`,
          );
        }
      }

      console.log("└─────────────────────────────────────────────────┘");
    } catch (err) {
      this.log.error(`Portfolio display error: ${(err as Error).message}`);
    }
  }

  /**
   * Stop the pipeline and destroy actors.
   */
  async stop(): Promise<void> {
    this.running = false;
    this.log.info("Stopping pipeline...");

    // Stop data source
    this.dataSource.stop();

    // Clear portfolio display timer
    if (this.portfolioTimer !== null) {
      clearInterval(this.portfolioTimer);
      this.portfolioTimer = null;
    }

    // Display final portfolio
    await this.displayPortfolio();

    // Destroy actors
    if (this.alertManager) {
      await this.alertManager.destroy();
      this.log.info("AlertManagerActor destroyed");
    }
    if (this.portfolio) {
      await this.portfolio.destroy();
      this.log.info("PortfolioActor destroyed");
    }
    if (this.priceFeed) {
      await this.priceFeed.destroy();
      this.log.info("PriceFeedActor destroyed");
    }

    console.log("\n── Pipeline Summary ──");
    console.log(`  Ticks processed:  ${this.tickCount}`);
    console.log(`  Candles produced: ${this.candleCount}`);
    console.log(`  Alerts fired:     ${this.alertCount}`);
  }
}
