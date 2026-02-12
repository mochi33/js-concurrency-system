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

// ── Test 11: resolve() for eager entries works ──
console.log("\nTest 11: Registry — resolve() works for eager entries too");
{
  const registry = new Registry();
  const fn = async () => 99;
  registry.register("eager", fn);

  const resolved = await registry.resolve("eager");
  assert(resolved === fn, "resolve returns the eager function");
}

// ── Test 12: resolve() returns undefined for unknown names ──
console.log("\nTest 12: Registry — resolve() returns undefined for unknown names");
{
  const registry = new Registry();
  const resolved = await registry.resolve("nope");
  assert(resolved === undefined, "resolve returns undefined");
}

// ── Test 13: fromDirectory() scans directory and registers lazily ──
console.log("\nTest 13: Registry — fromDirectory() scans directory and registers lazily");
{
  // Create a temp directory with task files
  const tmpDir = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${tmpDir}/add.ts`,
    `export default function(_ctx: any, ...args: any[]) { return (args[0] as number) + (args[1] as number); }`,
  );
  await Deno.writeTextFile(
    `${tmpDir}/greet.ts`,
    `export default function(_ctx: any, ...args: any[]) { return "hello " + args[0]; }`,
  );
  // Non-ts file should be ignored
  await Deno.writeTextFile(`${tmpDir}/README.md`, `# ignore me`);

  const registry = await Registry.fromDirectory(new URL(`file://${tmpDir}`));

  const names = registry.list().sort();
  assert(
    JSON.stringify(names) === JSON.stringify(["add", "greet"]),
    `list() returns [add, greet] (got ${JSON.stringify(names)})`,
  );
  assert(registry.has("add"), "has('add') is true");
  assert(registry.get("add") === undefined, "get('add') is undefined before resolve (lazy)");

  const addFn = await registry.resolve("add");
  assert(typeof addFn === "function", "resolve('add') returns a function");
  const result = addFn!(dummyCtx, 3, 4);
  assert(result === 7, `add(3, 4) = 7 (got ${result})`);

  await Deno.remove(tmpDir, { recursive: true });
}

// ── Results ──
console.log(`\n========================================`);
console.log(`Registry Tests: ${passed} passed, ${failed} failed`);
console.log(`========================================`);

if (failed > 0) {
  Deno.exit(1);
}
