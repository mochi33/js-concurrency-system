const BASE = "http://localhost:8080";

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

async function post(path: string, body: unknown): Promise<{ status: number; data: unknown }> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  return { status: res.status, data };
}

async function get(path: string): Promise<{ status: number; data: unknown }> {
  const res = await fetch(`${BASE}${path}`);
  const data = await res.json();
  return { status: res.status, data };
}

// ── Test 1: Health check ──
async function testHealth(): Promise<void> {
  console.log("\nTest 1: Health check");
  const { status, data } = await get("/health");
  assert(status === 200, "Status 200");
  assert((data as { status: string }).status === "ok", "Health OK");
}

// ── Test 2: Fibonacci ──
async function testFibonacci(): Promise<void> {
  console.log("\nTest 2: Fibonacci");
  const { status, data } = await post("/api/fibonacci", { n: 50 });
  assert(status === 200, "Status 200");
  assert(
    (data as { result: string }).result === "12586269025",
    `fib(50) = 12586269025 (got ${(data as { result: string }).result})`,
  );
}

// ── Test 3: Fibonacci with invalid input ──
async function testFibonacciError(): Promise<void> {
  console.log("\nTest 3: Fibonacci invalid input");
  const { status } = await post("/api/fibonacci", { n: -1 });
  assert(status === 400, "Status 400 for invalid input");
}

// ── Test 4: Prime check ──
async function testPrime(): Promise<void> {
  console.log("\nTest 4: Prime check");
  const { status, data } = await post("/api/prime", { n: 97 });
  const result = data as { n: number; isPrime: boolean };
  assert(status === 200, "Status 200");
  assert(result.isPrime === true, "97 is prime");

  const { data: data2 } = await post("/api/prime", { n: 100 });
  const result2 = data2 as { n: number; isPrime: boolean };
  assert(result2.isPrime === false, "100 is not prime");
}

// ── Test 5: Text analysis ──
async function testTextAnalyze(): Promise<void> {
  console.log("\nTest 5: Text analysis");
  const text =
    "The quick brown fox jumps over the lazy dog. The dog barked at the fox.";
  const { status, data } = await post("/api/text-analyze", { text });
  const result = data as {
    charCount: number;
    wordCount: number;
    uniqueWords: number;
    topWords: Record<string, number>;
  };
  assert(status === 200, "Status 200");
  assert(result.wordCount === 15, `Word count is 15 (got ${result.wordCount})`);
  assert(result.topWords["the"] === 4, `"the" appears 4 times (got ${result.topWords["the"]})`);
}

// ── Test 6: Batch multiply (nested spawns) ──
async function testBatch(): Promise<void> {
  console.log("\nTest 6: Batch multiply (nested spawns)");
  const items = [
    { a: 3, b: 7 },
    { a: 5, b: 5 },
    { a: 10, b: 4 },
    { a: 2, b: 8 },
  ];
  const { status, data } = await post("/api/batch", { items });
  const result = data as { results: number[]; progressUpdates: number };
  assert(status === 200, "Status 200");
  assert(
    JSON.stringify(result.results) === JSON.stringify([21, 25, 40, 16]),
    `Results: [21,25,40,16] (got ${JSON.stringify(result.results)})`,
  );
  assert(result.progressUpdates === 4, `4 progress updates (got ${result.progressUpdates})`);
}

// ── Test 7: Data pipeline (multi-step with nested spawns) ──
async function testPipeline(): Promise<void> {
  console.log("\nTest 7: Data pipeline (nested spawns)");
  const { status, data } = await post("/api/pipeline", {
    data: [1, 2, 3, 4, 5],
  });
  const result = data as {
    result: {
      input: number[];
      squared: number[];
      sum: number;
      sqrt: number;
      sqrtIsPrime: boolean;
    };
    pipelineSteps: unknown[];
  };
  assert(status === 200, "Status 200");
  // 1^2+2^2+3^2+4^2+5^2 = 1+4+9+16+25 = 55
  assert(result.result.sum === 55, `Sum of squares = 55 (got ${result.result.sum})`);
  assert(result.result.sqrt === 7, `sqrt(55) floored = 7 (got ${result.result.sqrt})`);
  assert(result.result.sqrtIsPrime === true, "7 is prime");
  assert(result.pipelineSteps.length === 6, `6 pipeline steps (got ${result.pipelineSteps.length})`);
}

// ── Test 8: SSE streaming ──
async function testSSE(): Promise<void> {
  console.log("\nTest 8: SSE fibonacci streaming");
  const res = await fetch(`${BASE}/api/fibonacci/stream?n=100`);
  assert(res.status === 200, "Status 200");

  const text = await res.text();
  const events = text
    .split("\n\n")
    .filter((e) => e.startsWith("data: "))
    .map((e) => JSON.parse(e.replace("data: ", "")));

  const progressEvents = events.filter(
    (e) => "progress" in e,
  );
  const resultEvent = events.find((e) => e.type === "result");

  assert(progressEvents.length >= 5, `Got progress events (${progressEvents.length})`);
  assert(resultEvent !== undefined, "Got result event");
  assert(
    resultEvent?.value === "354224848179261915075",
    `fib(100) correct (got ${resultEvent?.value})`,
  );
}

// ── Test 9: Exec timeout ──
async function testTimeout(): Promise<void> {
  console.log("\nTest 9: Exec timeout");
  const { status, data } = await post("/api/slow", { ms: 5000, timeout: 500 });
  const result = data as { error: string; name: string };
  assert(status === 408, `Status 408 (got ${status})`);
  assert(result.name === "ExecTimeoutError", `Got ExecTimeoutError (got ${result.name})`);
}

// ── Test 10: Concurrent requests ──
async function testConcurrent(): Promise<void> {
  console.log("\nTest 10: Concurrent task execution");
  const tasks = [
    { func: "prime_check", args: [7919] },
    { func: "prime_check", args: [7920] },
    { func: "multiply", args: [123, 456] },
    { func: "fibonacci", args: [30] },
    { func: "multiply", args: [99, 99] },
    { func: "prime_check", args: [104729] },
  ];
  const { status, data } = await post("/api/concurrent", { tasks });
  const result = data as {
    totalTasks: number;
    elapsed_ms: number;
    results: { func: string; result: unknown; error: string | null }[];
  };
  assert(status === 200, "Status 200");
  assert(result.totalTasks === 6, `6 tasks submitted`);

  const primeResults = result.results.filter((r) => r.func === "prime_check");
  assert(
    (primeResults[0]!.result as { isPrime: boolean }).isPrime === true,
    "7919 is prime",
  );
  assert(
    (primeResults[1]!.result as { isPrime: boolean }).isPrime === false,
    "7920 is not prime",
  );

  const mulResult = result.results.find(
    (r) => r.func === "multiply" && (r.result as number) === 56088,
  );
  assert(mulResult !== undefined, "123*456 = 56088");

  console.log(`  (All 6 tasks completed in ${result.elapsed_ms}ms)`);
}

// ── Test 11: Multiple HTTP requests in parallel ──
async function testParallelHTTP(): Promise<void> {
  console.log("\nTest 11: Parallel HTTP requests");
  const start = performance.now();

  const requests = Array.from({ length: 10 }, (_, i) =>
    post("/api/prime", { n: 10007 + i * 2 })
  );

  const responses = await Promise.all(requests);
  const elapsed = Math.round(performance.now() - start);

  assert(
    responses.every((r) => r.status === 200),
    `All 10 requests returned 200`,
  );
  console.log(`  (10 parallel requests completed in ${elapsed}ms)`);
}

// ── Test 12: 404 ──
async function test404(): Promise<void> {
  console.log("\nTest 12: 404 handling");
  const res = await fetch(`${BASE}/api/nonexistent`);
  assert(res.status === 404, "Status 404");
  await res.json();
}

// ── Run all tests ──
async function main(): Promise<void> {
  console.log("=== API Server Integration Tests ===");
  console.log(`Target: ${BASE}\n`);

  // Quick health check before starting
  try {
    await fetch(`${BASE}/health`);
  } catch {
    console.error("ERROR: Server is not running. Start it with:");
    console.error(
      "  deno run --allow-net --allow-run --allow-read examples/api_server/server.ts",
    );
    Deno.exit(1);
  }

  await testHealth();
  await testFibonacci();
  await testFibonacciError();
  await testPrime();
  await testTextAnalyze();
  await testBatch();
  await testPipeline();
  await testSSE();
  await testTimeout();
  await testConcurrent();
  await testParallelHTTP();
  await test404();

  console.log(`\n========================================`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`========================================`);

  Deno.exit(failed > 0 ? 1 : 0);
}

main();
