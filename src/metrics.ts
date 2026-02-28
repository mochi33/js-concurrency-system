import type { MetricsSnapshot } from "./types.ts";

const MAX_SAMPLES = 1000;

/**
 * Simple bounded array for storing latency samples.
 * Stores up to MAX_SAMPLES values, overwriting oldest when full.
 */
class SampleBuffer {
  private samples: number[] = [];
  private pos = 0;
  private _count = 0;
  private _sum = 0;

  add(value: number): void {
    this._count++;
    this._sum += value;
    if (this.samples.length < MAX_SAMPLES) {
      this.samples.push(value);
    } else {
      this.samples[this.pos] = value;
    }
    this.pos = (this.pos + 1) % MAX_SAMPLES;
  }

  percentile(p: number): number {
    const len = this.samples.length;
    if (len === 0) return 0;
    const sorted = this.samples.slice().sort((a, b) => a - b);
    const idx = Math.ceil((p / 100) * len) - 1;
    return sorted[Math.max(0, idx)]!;
  }

  get count(): number {
    return this._count;
  }

  get sum(): number {
    return this._sum;
  }
}

export class MetricsCollector {
  private _tasksSpawned = 0;
  private _tasksCompleted = 0;
  private _tasksFailed = 0;
  private _tasksRejectedAtCapacity = 0;
  private _tasksRejectedUnknownFunc = 0;
  private _activeNodeCount = 0;
  private _activeTaskCount = 0;
  private _queueDepth = 0;
  private _processSpawnCount = 0;

  private latencyBuffer = new SampleBuffer();
  private spawnWaitBuffer = new SampleBuffer();

  recordTaskSpawned(): void {
    this._tasksSpawned++;
  }

  recordTaskCompleted(): void {
    this._tasksCompleted++;
  }

  recordTaskFailed(): void {
    this._tasksFailed++;
  }

  recordRejection(reason: "at_capacity" | "unknown_func"): void {
    if (reason === "at_capacity") {
      this._tasksRejectedAtCapacity++;
    } else {
      this._tasksRejectedUnknownFunc++;
    }
  }

  recordTaskLatency(durationMs: number): void {
    this.latencyBuffer.add(durationMs);
  }

  recordSpawnWait(durationMs: number): void {
    this.spawnWaitBuffer.add(durationMs);
  }

  recordProcessSpawn(): void {
    this._processSpawnCount++;
  }

  setActiveNodeCount(count: number): void {
    this._activeNodeCount = count;
  }

  setActiveTaskCount(count: number): void {
    this._activeTaskCount = count;
  }

  setQueueDepth(depth: number): void {
    this._queueDepth = depth;
  }

  getMetrics(): MetricsSnapshot {
    return {
      tasksSpawned: this._tasksSpawned,
      tasksCompleted: this._tasksCompleted,
      tasksFailed: this._tasksFailed,
      tasksRejectedAtCapacity: this._tasksRejectedAtCapacity,
      tasksRejectedUnknownFunc: this._tasksRejectedUnknownFunc,
      activeNodeCount: this._activeNodeCount,
      activeTaskCount: this._activeTaskCount,
      queueDepth: this._queueDepth,
      processSpawnCount: this._processSpawnCount,
      latencyP50: this.latencyBuffer.percentile(50),
      latencyP95: this.latencyBuffer.percentile(95),
      latencyP99: this.latencyBuffer.percentile(99),
      latencyCount: this.latencyBuffer.count,
      latencySum: this.latencyBuffer.sum,
      spawnWaitP50: this.spawnWaitBuffer.percentile(50),
      spawnWaitP95: this.spawnWaitBuffer.percentile(95),
      spawnWaitP99: this.spawnWaitBuffer.percentile(99),
      spawnWaitCount: this.spawnWaitBuffer.count,
      spawnWaitSum: this.spawnWaitBuffer.sum,
    };
  }

  toPrometheus(): string {
    const m = this.getMetrics();
    const lines: string[] = [];

    lines.push("# HELP task_total Total tasks processed");
    lines.push("# TYPE task_total counter");
    lines.push(`task_total{status="spawned"} ${m.tasksSpawned}`);
    lines.push(`task_total{status="completed"} ${m.tasksCompleted}`);
    lines.push(`task_total{status="failed"} ${m.tasksFailed}`);
    lines.push(
      `task_total{status="rejected_at_capacity"} ${m.tasksRejectedAtCapacity}`,
    );
    lines.push(
      `task_total{status="rejected_unknown_func"} ${m.tasksRejectedUnknownFunc}`,
    );

    lines.push("");
    lines.push("# HELP active_nodes Current number of active nodes");
    lines.push("# TYPE active_nodes gauge");
    lines.push(`active_nodes ${m.activeNodeCount}`);

    lines.push("");
    lines.push("# HELP active_tasks Current number of active tasks");
    lines.push("# TYPE active_tasks gauge");
    lines.push(`active_tasks ${m.activeTaskCount}`);

    lines.push("");
    lines.push("# HELP queue_depth Pending find requests in queue");
    lines.push("# TYPE queue_depth gauge");
    lines.push(`queue_depth ${m.queueDepth}`);

    lines.push("");
    lines.push("# HELP process_spawn_total Total processes spawned");
    lines.push("# TYPE process_spawn_total counter");
    lines.push(`process_spawn_total ${m.processSpawnCount}`);

    lines.push("");
    lines.push("# HELP task_latency_seconds Task execution latency in seconds");
    lines.push("# TYPE task_latency_seconds summary");
    lines.push(`task_latency_seconds{quantile="0.5"} ${m.latencyP50 / 1000}`);
    lines.push(`task_latency_seconds{quantile="0.95"} ${m.latencyP95 / 1000}`);
    lines.push(`task_latency_seconds{quantile="0.99"} ${m.latencyP99 / 1000}`);
    lines.push(`task_latency_seconds_count ${m.latencyCount}`);
    lines.push(`task_latency_seconds_sum ${m.latencySum / 1000}`);

    lines.push("");
    lines.push(
      "# HELP spawn_wait_seconds Time from find request to task acceptance in seconds",
    );
    lines.push("# TYPE spawn_wait_seconds summary");
    lines.push(
      `spawn_wait_seconds{quantile="0.5"} ${m.spawnWaitP50 / 1000}`,
    );
    lines.push(
      `spawn_wait_seconds{quantile="0.95"} ${m.spawnWaitP95 / 1000}`,
    );
    lines.push(
      `spawn_wait_seconds{quantile="0.99"} ${m.spawnWaitP99 / 1000}`,
    );
    lines.push(`spawn_wait_seconds_count ${m.spawnWaitCount}`);
    lines.push(`spawn_wait_seconds_sum ${m.spawnWaitSum / 1000}`);

    return lines.join("\n") + "\n";
  }
}
