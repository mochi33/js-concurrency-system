import { FramedConnection } from "./protocol.ts";
import { assertNever } from "./schemas.ts";
import { MetricsCollector } from "./metrics.ts";
import { Logger } from "./logger.ts";
import type {
  CapacityChangeMessage,
  DiscoveryConfig,
  DiscoveryMessage,
  FindMessage,
  MetricsSnapshot,
  PeerState,
  RegisterMessage,
} from "./types.ts";

interface PendingFind {
  func: string;
  exclude: string[];
  conn: FramedConnection;
  processId: string;
  startTime: number;
}

export class Discovery {
  private peers = new Map<string, PeerState>();
  private peersByConn = new Map<FramedConnection, string>();
  private peerFc = new Map<string, FramedConnection>();
  private queue: PendingFind[] = [];
  private listener: Deno.TcpListener | null = null;
  private managedProcesses = new Map<string, Deno.ChildProcess>();
  private overflowIds = new Set<string>();
  private heartbeatTimer: number | undefined = undefined;
  private scaleDownTimer: number | undefined = undefined;
  private running = false;
  private connectionLoops: Promise<void>[] = [];
  private metricsCollector = new MetricsCollector();
  private metricsServer: Deno.HttpServer | null = null;
  private log = new Logger({ fields: { component: "discovery" } });

  constructor(private config: DiscoveryConfig) {}

  async start(): Promise<void> {
    this.running = true;
    const host = this.config.host ?? "127.0.0.1";
    this.listener = Deno.listen({ hostname: host, port: this.config.port });
    this.log.info(`Listening on ${host}:${this.config.port}`);

    // Spawn initial min processes
    for (let i = 0; i < this.config.min; i++) {
      await this.spawnProcess(false);
    }

    // Start heartbeat loop
    this.heartbeatTimer = setInterval(() => this.heartbeatLoop(), 15_000);

    // Start scale-down loop
    this.scaleDownTimer = setInterval(() => this.scaleDownLoop(), 10_000);

    // Start optional metrics HTTP server
    if (this.config.metricsPort) {
      this.startMetricsServer(this.config.metricsPort);
    }

    // Accept connections
    this.acceptLoop();
  }

  getMetrics(): MetricsSnapshot {
    this.updateGauges();
    return this.metricsCollector.getMetrics();
  }

  getMetricsPrometheus(): string {
    this.updateGauges();
    return this.metricsCollector.toPrometheus();
  }

  private updateGauges(): void {
    this.metricsCollector.setActiveNodeCount(this.peers.size);
    let totalActive = 0;
    for (const [, peer] of this.peers) {
      totalActive += peer.activeTasks;
    }
    this.metricsCollector.setActiveTaskCount(totalActive);
    this.metricsCollector.setQueueDepth(this.queue.length);
  }

  private startMetricsServer(port: number): void {
    const host = this.config.host ?? "127.0.0.1";
    this.metricsServer = Deno.serve(
      { hostname: host, port, onListen: () => {
        this.log.info(`Metrics server on ${host}:${port}`);
      }},
      (_req: Request) => {
        const body = this.getMetricsPrometheus();
        return new Response(body, {
          headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
        });
      },
    );
  }

  async shutdown(drainTimeout = 30_000): Promise<void> {
    this.running = false;

    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    if (this.scaleDownTimer !== undefined) {
      clearInterval(this.scaleDownTimer);
      this.scaleDownTimer = undefined;
    }

    // Send shutdown to all peers — they will drain and disconnect
    for (const [processId] of this.peers) {
      const fc = this.peerFc.get(processId);
      if (fc) {
        try {
          await fc.writeMessage({ type: "shutdown" });
        } catch {
          // peer may already be gone
        }
      }
    }

    // Wait for peers to drain and disconnect gracefully
    if (this.peers.size > 0) {
      this.log.info(`Waiting for ${this.peers.size} peer(s) to drain`);
      const deadline = Date.now() + drainTimeout;
      while (this.peers.size > 0 && Date.now() < deadline) {
        await new Promise<void>((r) => setTimeout(r, 200));
      }
      if (this.peers.size > 0) {
        this.log.warn(`Drain timeout reached, ${this.peers.size} peer(s) still connected`);
      }
    }

    // Kill all remaining managed processes
    for (const [, proc] of this.managedProcesses) {
      try {
        proc.kill("SIGTERM");
      } catch {
        // already dead
      }
    }

    // Close listener
    if (this.listener) {
      try {
        this.listener.close();
      } catch {
        // already closed
      }
      this.listener = null;
    }

    // Wait for connection loops to finish
    await Promise.allSettled(this.connectionLoops);

    // Close all remaining peer connections
    for (const [, peer] of this.peers) {
      try {
        peer.conn.close();
      } catch {
        // already closed
      }
    }
    this.peers.clear();
    this.peersByConn.clear();
    this.peerFc.clear();
    this.managedProcesses.clear();
    this.overflowIds.clear();
    this.queue = [];

    // Shut down metrics server
    if (this.metricsServer) {
      await this.metricsServer.shutdown();
      this.metricsServer = null;
    }

    this.log.info("Shut down");
  }

  private async acceptLoop(): Promise<void> {
    while (this.running && this.listener) {
      try {
        const conn = await this.listener.accept();
        const fc = new FramedConnection(conn);
        const loop = this.handleConnection(fc);
        this.connectionLoops.push(loop);
      } catch {
        // listener closed or error
        if (!this.running) break;
      }
    }
  }

  private async handleConnection(fc: FramedConnection): Promise<void> {
    try {
      while (this.running && !fc.isClosed) {
        const msg = await fc.readDiscoveryMessage();
        if (msg === null) break;
        await this.handleMessage(fc, msg);
      }
    } catch {
      // connection error (includes validation errors from malformed messages)
    } finally {
      this.handleDisconnect(fc);
    }
  }

  private async handleMessage(
    fc: FramedConnection,
    msg: DiscoveryMessage,
  ): Promise<void> {
    switch (msg.type) {
      case "register":
        await this.handleRegister(fc, msg);
        break;
      case "capacity_change":
        this.handleCapacityChange(fc, msg);
        break;
      case "find":
        await this.handleFind(fc, msg);
        break;
      case "request_spawn":
        await this.handleRequestSpawn(fc, msg);
        break;
      case "heartbeat_ack":
        this.handleHeartbeatAck(fc);
        break;
      case "bye":
        this.handleBye(fc);
        break;
      // Discovery receives these types but doesn't need to handle them
      // (they are sent by Discovery, not received from peers in normal flow)
      case "registered":
      case "found":
      case "spawn_result":
      case "heartbeat":
      case "shutdown":
        break;
      default:
        assertNever(msg);
    }
  }

  private async handleRegister(
    fc: FramedConnection,
    msg: RegisterMessage,
  ): Promise<void> {
    const now = Date.now();
    const peer: PeerState = {
      processId: msg.processId,
      conn: fc.raw,
      host: msg.host,
      port: msg.port,
      funcs: msg.funcs,
      activeTasks: 0,
      maxConcurrency: msg.maxConcurrency,
      managed: this.managedProcesses.has(msg.processId),
      overflow: this.overflowIds.has(msg.processId),
      lastHeartbeat: now,
      idleSince: now,
    };
    this.peers.set(msg.processId, peer);
    this.peersByConn.set(fc, msg.processId);
    this.peerFc.set(msg.processId, fc);

    await fc.writeMessage({ type: "registered", processId: msg.processId });
    this.metricsCollector.setActiveNodeCount(this.peers.size);
    this.log.info(
      `Registered peer ${msg.processId.slice(0, 8)} at ${msg.host}:${msg.port} (funcs: ${msg.funcs.join(", ")})`,
    );

    // Check if any pending find requests can now be fulfilled
    this.drainQueue();
  }

  private handleCapacityChange(
    fc: FramedConnection,
    msg: CapacityChangeMessage,
  ): void {
    const processId = this.peersByConn.get(fc);
    if (!processId) return;
    const peer = this.peers.get(processId);
    if (!peer) return;

    peer.activeTasks = msg.activeTasks;
    peer.maxConcurrency = msg.maxConcurrency;

    if (msg.activeTasks === 0) {
      peer.idleSince = Date.now();
    }

    // If peer now has capacity, drain the queue
    if (msg.activeTasks < msg.maxConcurrency) {
      this.drainQueue();
    }
  }

  private async handleFind(
    fc: FramedConnection,
    msg: FindMessage,
  ): Promise<void> {
    const findStart = Date.now();
    const requesterId = this.peersByConn.get(fc);
    const exclude = new Set(msg.exclude ?? []);

    // Search for peers with available capacity that can execute the requested function
    const candidates = this.findAvailablePeers(msg.func, exclude);

    if (candidates.length > 0) {
      this.metricsCollector.recordTaskSpawned();
      this.metricsCollector.recordSpawnWait(Date.now() - findStart);
      await fc.writeMessage({
        type: "found",
        peers: candidates.map((p) => ({
          processId: p.processId,
          host: p.host,
          port: p.port,
        })),
      });
      return;
    }

    // No idle peer found
    const managedCount = this.managedNonOverflowCount();
    const requester = requesterId ? this.peers.get(requesterId) : undefined;

    if (managedCount < this.config.max) {
      // Can spawn a new process within normal limits
      const spawned = await this.spawnProcess(false);
      if (spawned) {
        // Queue the request - the newly spawned process will register and drain the queue
        this.queue.push({
          func: msg.func,
          exclude: [...exclude],
          conn: fc,
          processId: requesterId ?? "",
          startTime: findStart,
        });
        this.metricsCollector.setQueueDepth(this.queue.length);
        return;
      }
    }

    if (requester && requester.activeTasks > 0) {
      // Requester is BUSY - allow overflow spawn to prevent deadlock
      const totalManaged = this.managedProcesses.size;
      if (totalManaged < this.config.max + this.config.overflowMax) {
        const spawned = await this.spawnProcess(true);
        if (spawned) {
          this.queue.push({
            func: msg.func,
            exclude: [...exclude],
            conn: fc,
            processId: requesterId ?? "",
            startTime: findStart,
          });
          this.metricsCollector.setQueueDepth(this.queue.length);
          return;
        }
      }
    }

    // Cannot spawn or no room - return empty
    this.metricsCollector.recordRejection("at_capacity");
    await fc.writeMessage({ type: "found", peers: [] });
  }

  private async handleRequestSpawn(
    fc: FramedConnection,
    msg: { type: "request_spawn"; reason: "no_idle" | "overflow" },
  ): Promise<void> {
    const isOverflow = msg.reason === "overflow";
    const managedCount = isOverflow
      ? this.managedProcesses.size
      : this.managedNonOverflowCount();
    const limit = isOverflow
      ? this.config.max + this.config.overflowMax
      : this.config.max;

    if (managedCount < limit) {
      const spawned = await this.spawnProcess(isOverflow);
      if (spawned) {
        // We don't know the new process's details yet - it will register.
        // For now return success=true but without details.
        await fc.writeMessage({
          type: "spawn_result",
          success: true,
        });
        return;
      }
    }

    await fc.writeMessage({ type: "spawn_result", success: false });
  }

  private handleHeartbeatAck(fc: FramedConnection): void {
    const processId = this.peersByConn.get(fc);
    if (!processId) return;
    const peer = this.peers.get(processId);
    if (!peer) return;
    peer.lastHeartbeat = Date.now();
  }

  private handleBye(fc: FramedConnection): void {
    const processId = this.peersByConn.get(fc);
    if (processId) {
      this.log.info(`Peer ${processId.slice(0, 8)} sent bye`);
      this.removePeer(processId);
    }
    fc.close();
  }

  private handleDisconnect(fc: FramedConnection): void {
    const processId = this.peersByConn.get(fc);
    if (processId) {
      this.log.info(`Peer ${processId.slice(0, 8)} disconnected`);
      this.removePeer(processId);
    }
    this.peersByConn.delete(fc);
    fc.close();
  }

  private removePeer(processId: string): void {
    const peer = this.peers.get(processId);
    this.peers.delete(processId);
    this.peerFc.delete(processId);
    this.metricsCollector.setActiveNodeCount(this.peers.size);
    if (peer) {
      for (const [fc, pid] of this.peersByConn) {
        if (pid === processId) {
          this.peersByConn.delete(fc);
          break;
        }
      }
    }

    // Remove from managed processes and kill if needed
    const proc = this.managedProcesses.get(processId);
    if (proc) {
      this.managedProcesses.delete(processId);
      this.overflowIds.delete(processId);
      try {
        proc.kill("SIGTERM");
      } catch {
        // already dead
      }
    }

    // Remove pending queue entries for this process
    this.queue = this.queue.filter((q) => q.processId !== processId);

    // Re-spawn if managed count dropped below min
    if (this.running && this.managedNonOverflowCount() < this.config.min) {
      this.spawnProcess(false).catch(() => {
        // spawn failed
      });
    }
  }

  private findAvailablePeers(
    func: string,
    exclude: Set<string>,
  ): PeerState[] {
    const results: PeerState[] = [];
    for (const [, peer] of this.peers) {
      if (
        peer.activeTasks < peer.maxConcurrency &&
        !exclude.has(peer.processId) &&
        peer.funcs.includes(func)
      ) {
        results.push(peer);
      }
    }
    return results;
  }

  private drainQueue(): void {
    const remaining: PendingFind[] = [];
    for (const pending of this.queue) {
      const exclude = new Set(pending.exclude);
      const candidates = this.findAvailablePeers(pending.func, exclude);
      if (candidates.length > 0 && !pending.conn.isClosed) {
        this.metricsCollector.recordTaskSpawned();
        this.metricsCollector.recordSpawnWait(Date.now() - pending.startTime);
        pending.conn
          .writeMessage({
            type: "found",
            peers: candidates.map((p) => ({
              processId: p.processId,
              host: p.host,
              port: p.port,
            })),
          })
          .catch(() => {
            // connection may have closed
          });
      } else {
        remaining.push(pending);
      }
    }
    this.queue = remaining;
    this.metricsCollector.setQueueDepth(this.queue.length);
  }

  private async spawnProcess(overflow: boolean): Promise<boolean> {
    const processId = crypto.randomUUID();
    const nodeMainPath = new URL("./node_main.ts", import.meta.url).pathname;

    const cmd = new Deno.Command("deno", {
      args: [
        "run",
        "--allow-net",
        "--allow-read",
        nodeMainPath,
        `--discovery-host=127.0.0.1`,
        `--discovery-port=${this.config.port}`,
        `--listen-port=0`,
        `--registry=${this.config.registry}`,
        `--process-id=${processId}`,
      ],
      stdout: "inherit",
      stderr: "inherit",
    });

    try {
      const child = cmd.spawn();
      this.managedProcesses.set(processId, child);
      if (overflow) {
        this.overflowIds.add(processId);
      }
      this.metricsCollector.recordProcessSpawn();
      this.log.info(
        `Spawned ${overflow ? "overflow " : ""}process ${processId.slice(0, 8)}`,
      );
      return true;
    } catch (e) {
      this.log.error(`Failed to spawn process: ${e}`);
      return false;
    }
  }

  private managedNonOverflowCount(): number {
    let count = 0;
    for (const [id] of this.managedProcesses) {
      if (!this.overflowIds.has(id)) {
        count++;
      }
    }
    return count;
  }

  private heartbeatLoop(): void {
    const now = Date.now();
    const deadPeers: string[] = [];

    for (const [processId, peer] of this.peers) {
      if (now - peer.lastHeartbeat > 45_000) {
        this.log.warn(`Peer ${processId.slice(0, 8)} heartbeat timeout, removing`);
        deadPeers.push(processId);
        continue;
      }

      const fc = this.peerFc.get(processId);
      if (fc) {
        fc.writeMessage({ type: "heartbeat" }).catch(() => {
          // connection error - will be caught by disconnect handler
        });
      }
    }

    for (const id of deadPeers) {
      const peer = this.peers.get(id);
      if (peer) {
        try {
          peer.conn.close();
        } catch {
          // already closed
        }
      }
      this.removePeer(id);
    }
  }

  private scaleDownLoop(): void {
    const now = Date.now();

    for (const [processId, peer] of this.peers) {
      if (!peer.managed) continue;
      if (peer.activeTasks > 0) continue;

      if (peer.overflow) {
        // Overflow processes are shut down immediately when idle
        this.log.info(`Shutting down idle overflow process ${processId.slice(0, 8)}`);
        this.sendShutdown(peer);
        continue;
      }

      // Normal managed process - check idleTimeout
      if (
        this.managedNonOverflowCount() > this.config.min &&
        now - peer.idleSince > this.config.idleTimeout
      ) {
        this.log.info(`Shutting down idle process ${processId.slice(0, 8)} (idle for ${now - peer.idleSince}ms)`);
        this.sendShutdown(peer);
      }
    }
  }

  private sendShutdown(peer: PeerState): void {
    const fc = this.peerFc.get(peer.processId);
    if (fc) {
      fc.writeMessage({ type: "shutdown" }).catch(() => {
        // connection error - peer will be removed by disconnect handler
      });
    }
  }
}
