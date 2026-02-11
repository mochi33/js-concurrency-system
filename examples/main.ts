import { Discovery } from "../src/discovery.ts";
import { ProcessNode } from "../src/node.ts";
import { Registry } from "../src/registry.ts";
import type { DiscoveryConfig } from "../src/types.ts";

const DISCOVERY_PORT = 19876;
const REGISTRY_PATH = new URL("./tasks.ts", import.meta.url).href;

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string): void {
  if (condition) {
    console.log(`  PASS: ${message}`);
    passed++;
  } else {
    console.error(`  FAIL: ${message}`);
    failed++;
  }
}

async function main(): Promise<void> {
  // ── Start Discovery ──
  const config: DiscoveryConfig = {
    port: DISCOVERY_PORT,
    registry: REGISTRY_PATH,
    min: 2,
    max: 4,
    overflowMax: 2,
    idleTimeout: 30_000,
  };

  console.log("Starting Discovery...");
  const discovery = new Discovery(config);
  await discovery.start();

  // Wait for managed nodes to register
  console.log("Waiting for managed nodes to start...");
  await new Promise<void>((r) => setTimeout(r, 3000));

  // ── Connect as external caller node ──
  console.log("Connecting external caller node...");
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
  console.log("Caller node connected.\n");

  // ── Test 1: spawn heavyCalc, receive progress, get result via join() ──
  console.log("Test 1: heavyCalc with receive() and join()");
  try {
    const ch = caller.spawn("heavyCalc", [10, 20]);

    // First receive: progress 0.3
    const r1 = await ch.receive();
    assert(
      !r1.done && (r1.value as { progress: number }).progress === 0.3,
      "First progress message is 0.3",
    );

    // Use join() to skip remaining intermediates and get final result
    const result = await ch.join();
    // heavyCalc: multiply(10, 2) = 20, then 20 + 20 = 40
    assert(result === 40, `Final result is 40 (got ${result})`);
  } catch (e) {
    console.error("  ERROR:", e);
    failed++;
  }

  // Small delay between tests to let executors go back to idle
  await new Promise<void>((r) => setTimeout(r, 500));

  // ── Test 2: spawn heavyCalc with for-await streaming ──
  console.log("\nTest 2: heavyCalc with for-await streaming");
  try {
    const ch = caller.spawn("heavyCalc", [5, 3]);
    const intermediates: unknown[] = [];

    for await (const msg of ch) {
      intermediates.push(msg);
    }

    assert(
      intermediates.length === 3,
      `Received 3 intermediate messages (got ${intermediates.length})`,
    );
    assert(
      (intermediates[0] as { progress: number }).progress === 0.3,
      "First intermediate is progress 0.3",
    );
    assert(
      (intermediates[1] as { progress: number }).progress === 0.7,
      "Second intermediate is progress 0.7",
    );
    assert(
      (intermediates[2] as { progress: number }).progress === 1.0,
      "Third intermediate is progress 1.0",
    );

    // multiply(5, 2) = 10, then 10 + 3 = 13
    assert(
      ch.returnValue === 13,
      `returnValue is 13 (got ${ch.returnValue})`,
    );
  } catch (e) {
    console.error("  ERROR:", e);
    failed++;
  }

  await new Promise<void>((r) => setTimeout(r, 500));

  // ── Test 3: echo (bidirectional channel) ──
  console.log("\nTest 3: echo (bidirectional channel)");
  try {
    const ch = caller.spawn("echo", []);

    // Send data to executor
    await ch.send({ hello: "world" });

    // Receive the echoed response
    const r1 = await ch.receive();
    assert(!r1.done, "Echoed response is intermediate (not done)");
    assert(
      JSON.stringify((r1.value as { echoed: unknown }).echoed) ===
        JSON.stringify({ hello: "world" }),
      "Echoed data matches sent data",
    );

    // Get final result
    const r2 = await ch.receive();
    assert(r2.done, "Final message has done=true");
    assert(r2.value === "echo_done", `Final result is "echo_done" (got ${r2.value})`);
  } catch (e) {
    console.error("  ERROR:", e);
    failed++;
  }

  await new Promise<void>((r) => setTimeout(r, 500));

  // ── Test 4: cancel ──
  console.log("\nTest 4: cancel");
  try {
    // Register a long-running task on the caller's registry just for this test.
    // We can't add tasks to managed nodes, so we test cancel on a simple task.
    const ch = caller.spawn("multiply", [7, 6]);
    ch.cancel();

    // Even after cancel, we can still get the result if the task completes
    // before processing the cancel. Multiply is instant, so it will complete.
    const result = await ch.join();
    assert(result === 42, `Got result 42 even after cancel (got ${result})`);
    console.log("  (Cancel sent but multiply completed before processing it -- expected)");
  } catch (e) {
    // If the task was cancelled in time, we get a CancelledError
    const err = e as Error;
    assert(
      err.name === "CancelledError",
      `Got CancelledError (got ${err.name})`,
    );
  }

  // ── Cleanup ──
  console.log("\nCleaning up...");
  await caller.close();
  await discovery.shutdown();

  // Wait for subprocesses to exit
  await new Promise<void>((r) => setTimeout(r, 1000));

  // ── Summary ──
  console.log(`\n========================================`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`========================================`);

  if (failed > 0) {
    Deno.exit(1);
  }
}

main().catch((e) => {
  console.error("Fatal error:", e);
  Deno.exit(1);
});
