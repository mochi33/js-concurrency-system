import { ProcessNode } from "./node.ts";
import { Registry } from "./registry.ts";
import type { NodeConfig } from "./types.ts";

function parseCliArgs(args: string[]): NodeConfig & {
  registry: string;
  processId?: string;
} {
  const parsed: Record<string, string> = {};
  for (const arg of args) {
    const match = arg.match(/^--([a-z-]+)=(.+)$/);
    if (match) {
      parsed[match[1]!] = match[2]!;
    }
  }

  return {
    discoveryHost: parsed["discovery-host"] ?? "127.0.0.1",
    discoveryPort: parseInt(parsed["discovery-port"] ?? "9876", 10),
    listenHost: parsed["listen-host"] ?? "127.0.0.1",
    listenPort: parseInt(parsed["listen-port"] ?? "0", 10),
    maxConcurrency: parsed["max-concurrency"]
      ? parseInt(parsed["max-concurrency"], 10)
      : undefined,
    registry: parsed["registry"] ?? "./examples/tasks.ts",
    processId: parsed["process-id"],
  };
}

async function main(): Promise<void> {
  const config = parseCliArgs(Deno.args);
  console.log(`[NodeMain] Starting with config:`, config);

  // Dynamic import of the registry file
  let registry: Registry;
  try {
    const registryPath = config.registry.startsWith(".")
      ? new URL(config.registry, `file://${Deno.cwd()}/`).href
      : config.registry;
    const mod = await import(registryPath);
    registry = mod.default as Registry;
  } catch (e) {
    console.error(`[NodeMain] Failed to load registry "${config.registry}":`, e);
    Deno.exit(1);
  }

  const node = new ProcessNode(
    {
      discoveryHost: config.discoveryHost,
      discoveryPort: config.discoveryPort,
      listenHost: config.listenHost,
      listenPort: config.listenPort,
      maxConcurrency: config.maxConcurrency,
    },
    registry,
    config.processId,
  );

  await node.start();

  const onSignal = async () => {
    console.log(`\n[NodeMain] Received signal, shutting down...`);
    await node.close();
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
  console.error("[NodeMain] Fatal error:", e);
  Deno.exit(1);
});
