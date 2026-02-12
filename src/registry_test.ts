import { Registry } from "./registry.ts";
import type { TaskRecord } from "./registry.ts";
import type { Context } from "./types.ts";

// Simple test harness
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

function assertThrows(fn: () => void, expectedMsg: string, message: string): void {
  try {
    fn();
    console.error(`  FAIL: ${message} (no error thrown)`);
    failed++;
  } catch (e) {
    const msg = (e as Error).message;
    if (msg.includes(expectedMsg)) {
      console.log(`  PASS: ${message}`);
      passed++;
    } else {
      console.error(`  FAIL: ${message} (got: "${msg}", expected to contain: "${expectedMsg}")`);
      failed++;
    }
  }
}

// Dummy context for testing (never actually called)
const dummyCtx = {} as Context;

// ── Test 1: Registry.from with Record creates a working Registry ──
console.log("\nTest 1: Registry.from — Record form creates a working Registry");
{
  const record: TaskRecord = {
    add: async (_ctx: Context, ...args: unknown[]) => {
      return (args[0] as number) + (args[1] as number);
    },
    greet: async (_ctx: Context, ...args: unknown[]) => {
      return `hello ${args[0]}`;
    },
  };

  const registry = Registry.from(record);

  assert(
    JSON.stringify(registry.list().sort()) === JSON.stringify(["add", "greet"]),
    "list() returns [add, greet]",
  );
  assert(typeof registry.get("add") === "function", "get('add') is a function");
  assert(typeof registry.get("greet") === "function", "get('greet') is a function");
  assert(registry.get("nonexistent") === undefined, "get('nonexistent') is undefined");
}

// ── Test 2: Record form functions execute correctly ──
console.log("\nTest 2: Registry.from — Record form functions execute correctly");
{
  const record: TaskRecord = {
    multiply: async (_ctx: Context, ...args: unknown[]) => {
      return (args[0] as number) * (args[1] as number);
    },
  };

  const registry = Registry.from(record);
  const fn = registry.get("multiply")!;
  const result = await fn(dummyCtx, 3, 7);
  assert(result === 21, `multiply(3, 7) = 21 (got ${result})`);
}

// ── Test 3: Registry instance returns same instance ──
console.log("\nTest 3: Registry.from — Registry instance returns same instance");
{
  const original = new Registry();
  original.register("foo", async () => 42);

  const result = Registry.from(original);

  assert(result === original, "returns same reference");
  assert(
    JSON.stringify(result.list()) === JSON.stringify(["foo"]),
    "list() returns ['foo']",
  );
}

// ── Test 4: Empty Record ──
console.log("\nTest 4: Registry.from — empty Record creates empty Registry");
{
  const registry = Registry.from({});
  assert(registry.list().length === 0, "list() is empty");
}

// ── Test 5: Record with multiple entries ──
console.log("\nTest 5: Registry.from — Record with multiple entries");
{
  const record: TaskRecord = {
    fn1: async () => 1,
    fn2: async () => 2,
    fn3: async () => 3,
  };

  const registry = Registry.from(record);
  assert(registry.list().length === 3, "list() has 3 entries");
  assert(
    JSON.stringify(registry.list().sort()) === JSON.stringify(["fn1", "fn2", "fn3"]),
    "list() returns [fn1, fn2, fn3]",
  );
}

// ── Test 6: Backward compatibility — register() still works ──
console.log("\nTest 6: Registry — register() still works as before");
{
  const registry = new Registry();
  registry.register("a", async () => 1);
  registry.register("b", async () => 2);

  assert(
    JSON.stringify(registry.list().sort()) === JSON.stringify(["a", "b"]),
    "list() returns [a, b]",
  );
}

// ── Test 7: register() rejects duplicate names ──
console.log("\nTest 7: Registry — register() rejects duplicate names");
{
  const registry = new Registry();
  registry.register("dup", async () => 1);

  assertThrows(
    () => registry.register("dup", async () => 2),
    'Function "dup" is already registered',
    "throws on duplicate registration",
  );
}

// ── Test 8: Works with sync functions in Record ──
console.log("\nTest 8: Registry.from — works with sync functions in Record");
{
  const record: TaskRecord = {
    syncAdd: (_ctx: Context, ...args: unknown[]) => {
      return (args[0] as number) + (args[1] as number);
    },
  };

  const registry = Registry.from(record);
  assert(
    JSON.stringify(registry.list()) === JSON.stringify(["syncAdd"]),
    "list() returns ['syncAdd']",
  );
  const fn = registry.get("syncAdd")!;
  const result = fn(dummyCtx, 10, 20);
  assert(result === 30, `syncAdd(10, 20) = 30 (got ${result})`);
}

// ── Results ──
console.log(`\n========================================`);
console.log(`Registry Tests: ${passed} passed, ${failed} failed`);
console.log(`========================================`);

if (failed > 0) {
  Deno.exit(1);
}
