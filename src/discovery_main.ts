import { Discovery } from "./discovery.ts";
import { Logger } from "./logger.ts";
import type { DiscoveryConfig } from "./types.ts";

const log = new Logger({ fields: { component: "discovery_main" } });

function parseCliArgs(args: string[]): DiscoveryConfig {
  const parsed: Record<string, string> = {};
  for (const arg of args) {
    const match = arg.match(/^--([a-z-]+)=(.+)$/);
    if (match) {
      parsed[match[1]!] = match[2]!;
    }
  }

  const port = parseInt(parsed["port"] ?? "9876", 10);
  const host = parsed["host"] ?? "127.0.0.1";
  const registry = parsed["registry"] ?? "./examples/tasks.ts";
  const min = parseInt(parsed["min"] ?? "2", 10);
  const max = parseInt(
    parsed["max"] ?? String(navigator.hardwareConcurrency ?? 4),
    10,
  );
  const overflowMax = parseInt(parsed["overflow-max"] ?? String(max), 10);
  const idleTimeout = parseInt(parsed["idle-timeout"] ?? "30000", 10);
  const metricsPort = parsed["metrics-port"]
    ? parseInt(parsed["metrics-port"], 10)
    : undefined;

  return { port, host, registry, min, max, overflowMax, idleTimeout, metricsPort };
}

async function main(): Promise<void> {
  const config = parseCliArgs(Deno.args);
  log.info(`Config: port=${config.port} min=${config.min} max=${config.max}`);

  const discovery = new Discovery(config);
  await discovery.start();

  const onSignal = async () => {
    log.info("Received signal, shutting down");
    await discovery.shutdown();
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", () => {
    onSignal();
  });
  Deno.addSignalListener("SIGTERM", () => {
    onSignal();
  });
}

main().catch((e) => {
  log.error(`Fatal error: ${e}`);
  Deno.exit(1);
});
