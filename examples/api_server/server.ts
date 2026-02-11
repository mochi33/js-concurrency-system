import { Discovery } from "../../src/discovery.ts";
import { ProcessNode } from "../../src/node.ts";
import { Registry } from "../../src/registry.ts";
import type { DiscoveryConfig } from "../../src/types.ts";

const DISCOVERY_PORT = 19877;
const HTTP_PORT = 8080;
const REGISTRY_PATH = new URL("./tasks.ts", import.meta.url).href;

// ── JSON response helper ──
function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function errorResponse(message: string, status = 500): Response {
  return json({ error: message }, status);
}

// ── Parse JSON body ──
async function parseBody(req: Request): Promise<unknown> {
  try {
    return await req.json();
  } catch {
    return null;
  }
}

// ── Route handler type ──
type Handler = (caller: ProcessNode, req: Request) => Promise<Response>;

// ── Handlers ──

const handleFibonacci: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as { n?: number } | null;
  if (!body || typeof body.n !== "number" || body.n < 0 || body.n > 10000) {
    return errorResponse("Invalid body: { n: 0..10000 }", 400);
  }

  const ch = caller.spawn("fibonacci", [body.n]);
  const result = await ch.join();
  return json({ n: body.n, result });
};

const handleFibonacciStream: Handler = async (caller, req) => {
  const url = new URL(req.url);
  const n = parseInt(url.searchParams.get("n") ?? "0", 10);
  if (isNaN(n) || n < 0 || n > 10000) {
    return errorResponse("Invalid n: 0..10000", 400);
  }

  const ch = caller.spawn("fibonacci", [n]);
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        for await (const msg of ch) {
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify(msg)}\n\n`),
          );
        }
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({ type: "result", value: ch.returnValue })}\n\n`,
          ),
        );
        controller.close();
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({ type: "error", error: err })}\n\n`,
          ),
        );
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    },
  });
};

const handleBatch: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as {
    items?: { a: number; b: number }[];
  } | null;
  if (!body || !Array.isArray(body.items) || body.items.length === 0) {
    return errorResponse(
      'Invalid body: { items: [{ a: number, b: number }, ...] }',
      400,
    );
  }

  const ch = caller.spawn("batch_multiply", [body.items]);
  const intermediates: unknown[] = [];
  for await (const msg of ch) {
    intermediates.push(msg);
  }

  return json({
    results: ch.returnValue,
    progressUpdates: intermediates.length,
  });
};

const handlePipeline: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as { data?: number[] } | null;
  if (!body || !Array.isArray(body.data) || body.data.length === 0) {
    return errorResponse("Invalid body: { data: number[] }", 400);
  }

  const ch = caller.spawn("data_pipeline", [body.data]);
  const steps: unknown[] = [];
  for await (const msg of ch) {
    steps.push(msg);
  }

  return json({
    result: ch.returnValue,
    pipelineSteps: steps,
  });
};

const handlePrimeCheck: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as { n?: number } | null;
  if (!body || typeof body.n !== "number" || body.n < 0) {
    return errorResponse("Invalid body: { n: number }", 400);
  }

  const ch = caller.spawn("prime_check", [body.n]);
  const result = await ch.join();
  return json(result);
};

const handleTextAnalyze: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as { text?: string } | null;
  if (!body || typeof body.text !== "string" || body.text.length === 0) {
    return errorResponse("Invalid body: { text: string }", 400);
  }

  const ch = caller.spawn("text_analyze", [body.text]);
  const result = await ch.join();
  return json(result);
};

const handleSlowTask: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as {
    ms?: number;
    timeout?: number;
  } | null;
  if (!body || typeof body.ms !== "number") {
    return errorResponse("Invalid body: { ms: number, timeout?: number }", 400);
  }

  const ch = caller.spawn("slow_task", [body.ms], {
    execTimeout: body.timeout,
  });

  try {
    const result = await ch.join();
    return json(result);
  } catch (e) {
    const err = e instanceof Error ? e : new Error(String(e));
    return json(
      { error: err.message, name: err.name },
      err.name === "ExecTimeoutError" ? 408 : 500,
    );
  }
};

// ── Concurrent requests: spawn N fibonacci tasks at once ──
const handleConcurrent: Handler = async (caller, req) => {
  const body = (await parseBody(req)) as {
    tasks?: { func: string; args: unknown[] }[];
  } | null;
  if (!body || !Array.isArray(body.tasks) || body.tasks.length === 0) {
    return errorResponse(
      'Invalid body: { tasks: [{ func: string, args: unknown[] }, ...] }',
      400,
    );
  }

  const start = performance.now();
  const promises = body.tasks.map(async (task) => {
    const ch = caller.spawn(task.func, task.args);
    try {
      const result = await ch.join();
      return { func: task.func, args: task.args, result, error: null };
    } catch (e) {
      const err = e instanceof Error ? e.message : String(e);
      return { func: task.func, args: task.args, result: null, error: err };
    }
  });

  const results = await Promise.all(promises);
  const elapsed = Math.round(performance.now() - start);

  return json({
    totalTasks: results.length,
    elapsed_ms: elapsed,
    results,
  });
};

// ── Main ──

async function main(): Promise<void> {
  console.log("=== Data Processing API Server ===\n");

  // Start Discovery
  const discoveryConfig: DiscoveryConfig = {
    port: DISCOVERY_PORT,
    registry: REGISTRY_PATH,
    min: 3,
    max: 6,
    overflowMax: 2,
    idleTimeout: 60_000,
  };
  console.log("Starting Discovery...");
  const discovery = new Discovery(discoveryConfig);
  await discovery.start();

  // Wait for managed nodes to register
  console.log("Waiting for worker nodes...");
  await new Promise<void>((r) => setTimeout(r, 3000));

  // Connect caller node (HTTP server acts as a caller)
  console.log("Connecting API caller node...");
  const caller = new ProcessNode(
    {
      discoveryHost: "127.0.0.1",
      discoveryPort: DISCOVERY_PORT,
      listenHost: "127.0.0.1",
      listenPort: 0,
    },
    new Registry(),
  );
  await caller.start();

  // Start HTTP server
  console.log(`\nAPI Server ready at http://localhost:${HTTP_PORT}\n`);
  console.log("Endpoints:");
  console.log("  POST /api/fibonacci        { n: number }");
  console.log("  GET  /api/fibonacci/stream  ?n=number (SSE)");
  console.log("  POST /api/prime             { n: number }");
  console.log("  POST /api/text-analyze      { text: string }");
  console.log("  POST /api/batch             { items: [{a,b},...] }");
  console.log("  POST /api/pipeline          { data: number[] }");
  console.log("  POST /api/slow              { ms: number, timeout?: number }");
  console.log("  POST /api/concurrent        { tasks: [{func,args},...] }");
  console.log("  GET  /health");
  console.log("");

  Deno.serve({ port: HTTP_PORT }, async (req: Request) => {
    const url = new URL(req.url);
    const method = req.method;
    const path = url.pathname;
    const start = performance.now();

    try {
      let response: Response;

      if (path === "/health" && method === "GET") {
        response = json({ status: "ok", timestamp: new Date().toISOString() });
      } else if (path === "/api/fibonacci" && method === "POST") {
        response = await handleFibonacci(caller, req);
      } else if (path === "/api/fibonacci/stream" && method === "GET") {
        response = await handleFibonacciStream(caller, req);
      } else if (path === "/api/prime" && method === "POST") {
        response = await handlePrimeCheck(caller, req);
      } else if (path === "/api/text-analyze" && method === "POST") {
        response = await handleTextAnalyze(caller, req);
      } else if (path === "/api/batch" && method === "POST") {
        response = await handleBatch(caller, req);
      } else if (path === "/api/pipeline" && method === "POST") {
        response = await handlePipeline(caller, req);
      } else if (path === "/api/slow" && method === "POST") {
        response = await handleSlowTask(caller, req);
      } else if (path === "/api/concurrent" && method === "POST") {
        response = await handleConcurrent(caller, req);
      } else {
        response = errorResponse("Not found", 404);
      }

      const elapsed = Math.round(performance.now() - start);
      console.log(`${method} ${path} -> ${response.status} (${elapsed}ms)`);
      return response;
    } catch (e) {
      const elapsed = Math.round(performance.now() - start);
      const err = e instanceof Error ? e.message : String(e);
      console.error(`${method} ${path} -> 500 (${elapsed}ms): ${err}`);
      return errorResponse(err);
    }
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\nShutting down...");
    await caller.close();
    await discovery.shutdown();
    Deno.exit(0);
  };
  Deno.addSignalListener("SIGINT", () => shutdown());
  Deno.addSignalListener("SIGTERM", () => shutdown());
}

main().catch((e) => {
  console.error("Fatal:", e);
  Deno.exit(1);
});
