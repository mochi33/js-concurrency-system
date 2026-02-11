import { Registry } from "../../src/registry.ts";
import type { Context } from "../../src/types.ts";

const registry = new Registry();

// ── fibonacci: CPU-intensive with progress updates ──
registry.register(
  "fibonacci",
  async (ctx: Context, ...args: unknown[]) => {
    const n = args[0] as number;
    if (n <= 1) return n;

    let a = 0n;
    let b = 1n;
    const step = Math.max(1, Math.floor(n / 10));

    for (let i = 2; i <= n; i++) {
      [a, b] = [b, a + b];
      if (i % step === 0) {
        await ctx.send({ progress: i / n });
      }
    }

    return b.toString();
  },
);

// ── prime_check: check if a number is prime ──
registry.register(
  "prime_check",
  async (_ctx: Context, ...args: unknown[]) => {
    const n = args[0] as number;
    if (n < 2) return { n, isPrime: false };
    if (n < 4) return { n, isPrime: true };
    if (n % 2 === 0) return { n, isPrime: false };

    for (let i = 3; i * i <= n; i += 2) {
      if (n % i === 0) return { n, isPrime: false };
    }
    return { n, isPrime: true };
  },
);

// ── text_analyze: text analysis ──
registry.register(
  "text_analyze",
  async (_ctx: Context, ...args: unknown[]) => {
    const text = args[0] as string;
    const words = text.split(/\s+/).filter((w) => w.length > 0);
    const freq: Record<string, number> = {};
    for (const w of words) {
      const lower = w.toLowerCase().replace(/[^a-z0-9]/g, "");
      if (lower) freq[lower] = (freq[lower] || 0) + 1;
    }

    const sorted = Object.entries(freq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20);

    return {
      charCount: text.length,
      wordCount: words.length,
      uniqueWords: Object.keys(freq).length,
      topWords: Object.fromEntries(sorted),
    };
  },
);

// ── multiply: simple sub-task ──
registry.register(
  "multiply",
  async (_ctx: Context, ...args: unknown[]) => {
    const a = args[0] as number;
    const b = args[1] as number;
    return a * b;
  },
);

// ── sum_reduce: sum an array of numbers ──
registry.register(
  "sum_reduce",
  async (_ctx: Context, ...args: unknown[]) => {
    const numbers = args[0] as number[];
    return numbers.reduce((s, n) => s + n, 0);
  },
);

// ── batch_multiply: processes items via nested spawns ──
registry.register(
  "batch_multiply",
  async (ctx: Context, ...args: unknown[]) => {
    const items = args[0] as { a: number; b: number }[];
    const results: number[] = [];

    for (const item of items) {
      const ch = ctx.spawn("multiply", [item.a, item.b]);
      const result = await ch.join();
      results.push(result as number);
      await ctx.send({
        progress: results.length / items.length,
        completed: results.length,
        total: items.length,
      });
    }

    return results;
  },
);

// ── data_pipeline: multi-step processing with nested spawns ──
registry.register(
  "data_pipeline",
  async (ctx: Context, ...args: unknown[]) => {
    const data = args[0] as number[];

    // Step 1: Square each number
    await ctx.send({ step: 1, name: "square", status: "running" });
    const squared = data.map((n) => n * n);
    await ctx.send({ step: 1, name: "square", status: "done" });

    // Step 2: Sum via sub-task
    await ctx.send({ step: 2, name: "sum", status: "running" });
    const sumCh = ctx.spawn("sum_reduce", [squared]);
    const sum = (await sumCh.join()) as number;
    await ctx.send({ step: 2, name: "sum", status: "done" });

    // Step 3: Find primes up to sqrt(sum) via sub-task
    await ctx.send({ step: 3, name: "prime_analysis", status: "running" });
    const sqrtVal = Math.floor(Math.sqrt(sum));
    const primeCh = ctx.spawn("prime_check", [sqrtVal]);
    const primeResult = await primeCh.join();
    await ctx.send({ step: 3, name: "prime_analysis", status: "done" });

    return {
      input: data,
      squared,
      sum,
      sqrt: sqrtVal,
      sqrtIsPrime: (primeResult as { isPrime: boolean }).isPrime,
    };
  },
);

// ── slow_task: simulate slow I/O (for timeout testing) ──
registry.register(
  "slow_task",
  async (ctx: Context, ...args: unknown[]) => {
    const ms = args[0] as number;
    const steps = 5;
    const stepMs = ms / steps;

    for (let i = 1; i <= steps; i++) {
      if (ctx.signal.aborted) throw new Error("Task cancelled");
      await new Promise<void>((r) => setTimeout(r, stepMs));
      await ctx.send({ progress: i / steps });
    }

    return { elapsed: ms, message: "completed" };
  },
);

export default registry;
