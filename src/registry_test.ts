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

// ── Test 9: Rejects non-function values in Record ──
console.log("\nTest 9: Registry.from — rejects non-function values");
{
  // deno-lint-ignore no-explicit-any
  const bad = { multiply: async () => 1, VERSION: "1.0.0" as any };
  assertThrows(
    () => Registry.from(bad),
    '"VERSION" is not a function',
    "throws on string property",
  );

  // deno-lint-ignore no-explicit-any
  const bad2 = { count: 42 as any };
  assertThrows(
    () => Registry.from(bad2),
    '"count" is not a function',
    "throws on number property",
  );
}

// ── Test 10: Rejects non-object sources ──
console.log("\nTest 10: Registry.from — rejects non-object sources");
{
  // deno-lint-ignore no-explicit-any
  assertThrows(
    () => Registry.from(null as any),
    "expected a Registry or Record",
    "throws on null",
  );

  // deno-lint-ignore no-explicit-any
  assertThrows(
    () => Registry.from("hello" as any),
    "expected a Registry or Record",
    "throws on string",
  );

  // deno-lint-ignore no-explicit-any
  assertThrows(
    () => Registry.from(42 as any),
    "expected a Registry or Record",
    "throws on number",
  );

  // deno-lint-ignore no-explicit-any
  assertThrows(
    () => Registry.from(undefined as any),
    "expected a Registry or Record",
    "throws on undefined",
  );
}

// ── Test 11: lazy() registers name and shows in list() ──
console.log("\nTest 11: Registry — lazy() registers name and shows in list()");
{
  const registry = new Registry();
  registry.lazy("lazyFn", async () => async () => 42);

  assert(registry.list().includes("lazyFn"), "list() includes lazyFn");
  assert(registry.has("lazyFn"), "has('lazyFn') is true");
  assert(registry.get("lazyFn") === undefined, "get('lazyFn') is undefined before resolve");
}

// ── Test 12: resolve() loads and caches lazy entry ──
console.log("\nTest 12: Registry — resolve() loads and caches lazy entry");
{
  let loadCount = 0;
  const registry = new Registry();
  registry.lazy("lazyMul", async () => {
    loadCount++;
    return async (_ctx: Context, ...args: unknown[]) =>
      (args[0] as number) * (args[1] as number);
  });

  const fn = await registry.resolve("lazyMul");
  assert(typeof fn === "function", "resolve returns a function");
  assert(loadCount === 1, "loader called once");

  const result = await fn!(dummyCtx, 5, 6);
  assert(result === 30, `lazyMul(5, 6) = 30 (got ${result})`);

  // Second resolve returns cached — loader not called again
  const fn2 = await registry.resolve("lazyMul");
  assert(fn === fn2, "second resolve returns same reference");
  assert(loadCount === 1, "loader still called only once");

  // After resolve, get() also works
  assert(registry.get("lazyMul") === fn, "get() works after resolve");
}

// ── Test 13: resolve() works with module-style default export ──
console.log("\nTest 13: Registry — resolve() works with { default: fn } return");
{
  const registry = new Registry();
  const taskFn = async (_ctx: Context, ...args: unknown[]) =>
    (args[0] as number) + 1;

  registry.lazy("inc", async () => ({ default: taskFn }));

  const resolved = await registry.resolve("inc");
  assert(resolved === taskFn, "resolved to the default export function");
}

// ── Test 14: resolve() for eager entries works ──
console.log("\nTest 14: Registry — resolve() works for eager entries too");
{
  const registry = new Registry();
  const fn = async () => 99;
  registry.register("eager", fn);

  const resolved = await registry.resolve("eager");
  assert(resolved === fn, "resolve returns the eager function");
}

// ── Test 15: resolve() returns undefined for unknown names ──
console.log("\nTest 15: Registry — resolve() returns undefined for unknown names");
{
  const registry = new Registry();
  const resolved = await registry.resolve("nope");
  assert(resolved === undefined, "resolve returns undefined");
}

// ── Test 16: lazy() rejects duplicates ──
console.log("\nTest 16: Registry — lazy() rejects duplicate names");
{
  const registry = new Registry();
  registry.lazy("dup", async () => async () => 1);

  assertThrows(
    () => registry.lazy("dup", async () => async () => 2),
    'Function "dup" is already registered',
    "throws on duplicate lazy registration",
  );

  // Also conflicts with eager registration
  const registry2 = new Registry();
  registry2.register("x", async () => 1);

  assertThrows(
    () => registry2.lazy("x", async () => async () => 2),
    'Function "x" is already registered',
    "throws when lazy conflicts with eager",
  );
}

// ── Test 17: list() returns both eager and lazy names ──
console.log("\nTest 17: Registry — list() returns both eager and lazy names");
{
  const registry = new Registry();
  registry.register("eager1", async () => 1);
  registry.lazy("lazy1", async () => async () => 2);
  registry.register("eager2", async () => 3);
  registry.lazy("lazy2", async () => async () => 4);

  const names = registry.list().sort();
  assert(
    JSON.stringify(names) === JSON.stringify(["eager1", "eager2", "lazy1", "lazy2"]),
    "list() returns all 4 names",
  );
}

// ── Test 18: has() works for both eager and lazy ──
console.log("\nTest 18: Registry — has() works for both eager and lazy");
{
  const registry = new Registry();
  registry.register("a", async () => 1);
  registry.lazy("b", async () => async () => 2);

  assert(registry.has("a"), "has('a') is true (eager)");
  assert(registry.has("b"), "has('b') is true (lazy)");
  assert(!registry.has("c"), "has('c') is false (not registered)");
}

// ── Results ──
console.log(`\n========================================`);
console.log(`Registry Tests: ${passed} passed, ${failed} failed`);
console.log(`========================================`);

if (failed > 0) {
  Deno.exit(1);
}
