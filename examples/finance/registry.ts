// ── Registry: register all tasks and actors for the finance example ──

import { Registry } from "../../src/registry.ts";

// Task functions
import {
  analyze_batch,
  compute_all_indicators,
  compute_bollinger,
  compute_ema,
  compute_macd,
  compute_rsi,
  compute_sma,
} from "./analysis/tasks.ts";

// Actor classes
import { PriceFeedActor } from "./actors/price_feed.ts";
import { PortfolioActor } from "./actors/portfolio.ts";
import { AlertManagerActor } from "./actors/alert_manager.ts";

const registry = new Registry();

// ── Register 8 distributed tasks ──
registry.register("compute_sma", compute_sma);
registry.register("compute_ema", compute_ema);
registry.register("compute_rsi", compute_rsi);
registry.register("compute_bollinger", compute_bollinger);
registry.register("compute_macd", compute_macd);
registry.register("compute_all_indicators", compute_all_indicators);
registry.register("analyze_batch", analyze_batch);

// ── Register 3 actors ──
registry.registerActor("PriceFeedActor", PriceFeedActor);
registry.registerActor("PortfolioActor", PortfolioActor);
registry.registerActor("AlertManagerActor", AlertManagerActor);

export default registry;
