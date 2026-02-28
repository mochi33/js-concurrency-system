// ── Entry point: Discovery → Node → Orchestrator → graceful shutdown ──

import { Discovery } from "../../src/discovery.ts";
import { ProcessNode } from "../../src/node.ts";
import { Registry } from "../../src/registry.ts";
import { Logger } from "../../src/logger.ts";
import type { DiscoveryConfig } from "../../src/types.ts";
import { MockDataSource } from "./data/sources.ts";
import { Orchestrator } from "./pipeline/orchestrator.ts";
import {
  DISCOVERY_PORT,
  MAX_NODES,
  MIN_NODES,
  OVERFLOW_MAX,
  RUN_DURATION_MS,
  WATCH_SYMBOLS,
} from "./config.ts";

const log = new Logger({ fields: { component: "main" } });

async function main(): Promise<void> {
  console.log("╔══════════════════════════════════════════════════════════╗");
  console.log("║    Real-Time Financial Data Processing System           ║");
  console.log("║    Powered by JS Concurrency Framework                  ║");
  console.log("╚══════════════════════════════════════════════════════════╝");
  console.log();

  // ── 1. Start Discovery with auto-scaling ──
  const registryPath = new URL("./registry.ts", import.meta.url).href;

  const discoveryConfig: DiscoveryConfig = {
    port: DISCOVERY_PORT,
    registry: registryPath,
    min: MIN_NODES,
    max: MAX_NODES,
    overflowMax: OVERFLOW_MAX,
    idleTimeout: 60_000,
  };

  log.info("Starting Discovery server...");
  const discovery = new Discovery(discoveryConfig);
  await discovery.start();
  log.info(`Discovery running on port ${DISCOVERY_PORT}`);

  // Wait for managed worker nodes to register
  log.info(`Waiting for ${MIN_NODES} worker nodes to start...`);
  await new Promise<void>((r) => setTimeout(r, 3000));

  // ── 2. Connect caller node ──
  log.info("Connecting caller node...");
  const callerRegistry = new Registry();
  const caller = new ProcessNode(
    {
      discoveryHost: "127.0.0.1",
      discoveryPort: DISCOVERY_PORT,
      listenHost: "127.0.0.1",
      listenPort: 0,
    },
    callerRegistry,
  );
  await caller.start();
  log.info("Caller node connected");

  // ── 3. Create data source ──
  const dataSource = new MockDataSource();

  // ── 4. Create and start orchestrator ──
  const orchestrator = new Orchestrator(caller, dataSource, WATCH_SYMBOLS);

  console.log(`\nMonitoring ${WATCH_SYMBOLS.length} symbols: ${WATCH_SYMBOLS.join(", ")}`);
  console.log("Press Ctrl+C to stop\n");

  await orchestrator.start();

  // ── 5. Graceful shutdown on signals ──
  const shutdown = async () => {
    log.info("Shutdown signal received");
    await orchestrator.stop();
    await caller.close();
    await discovery.shutdown();
    // Wait for subprocesses to exit
    await new Promise<void>((r) => setTimeout(r, 1000));
    log.info("Shutdown complete");
  };

  // Handle Ctrl+C
  const abortController = new AbortController();
  const signalHandler = () => {
    abortController.abort();
  };
  Deno.addSignalListener("SIGINT", signalHandler);

  try {
    if (RUN_DURATION_MS > 0) {
      // Auto-shutdown after configured duration
      await Promise.race([
        new Promise<void>((r) => setTimeout(r, RUN_DURATION_MS)),
        new Promise<void>((_, reject) => {
          abortController.signal.addEventListener("abort", () =>
            reject(new Error("interrupted"))
          );
        }),
      ]).catch(() => {});
    } else {
      // Run until Ctrl+C
      await new Promise<void>((_, reject) => {
        abortController.signal.addEventListener("abort", () =>
          reject(new Error("interrupted"))
        );
      }).catch(() => {});
    }
  } finally {
    try {
      Deno.removeSignalListener("SIGINT", signalHandler);
    } catch {
      // already removed
    }
    await shutdown();
  }
}

main().catch((e) => {
  console.error("Fatal error:", e);
  Deno.exit(1);
});
