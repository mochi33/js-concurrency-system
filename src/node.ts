import { FramedConnection } from "./protocol.ts";
import { Multiplexer } from "./multiplexer.ts";
import { createChannel } from "./channel.ts";
import { createContext } from "./context.ts";
import { Registry } from "./registry.ts";
import { ActorInstance, requestActorCreation } from "./actor.ts";
import { Logger } from "./logger.ts";
import { assertNever } from "./schemas.ts";
import { ObjectRef, ObjectStore } from "./object_store.ts";
import type {
  ActorCallMessage,
  ActorCreateMessage,
  ActorDestroyMessage,
  ActorHandle,
  Channel,
  DiscoveryMessage,
  ExecMessage,
  FoundMessage,
  NodeConfig,
  ObjectFetchMessage,
  P2PMessage,
  PeerInfo,
  ReceiveResult,
  SpawnOptions,
  SpawnResultMessage,
  TaskFunction,
  TaskRecord,
} from "./types.ts";
import { ExecTimeoutError, SpawnError, SpawnTimeoutError } from "./types.ts";
import { MetricsCollector } from "./metrics.ts";
import type { MetricsSnapshot } from "./types.ts";

const DEFAULT_SPAWN_TIMEOUT = 30_000;
const DEFAULT_MAX_CONCURRENCY = 4;
const MAX_RETRY = 3;
const MEMORY_THRESHOLD = 512 * 1024 * 1024; // 512 MiB
const RECONNECT_INITIAL_DELAY = 1_000; // 1 second
const RECONNECT_MAX_DELAY = 30_000; // 30 seconds

/**
 * Waiter for a discovery response message of a specific type.
 * The discoveryLoop reads all messages and dispatches to pending waiters.
 */
interface DiscoveryWaiter {
  types: string[];
  resolve: (msg: DiscoveryMessage) => void;
  reject: (err: Error) => void;
}

export class ProcessNode {
  private processId: string;
  private activeTasks = 0;
  private maxConcurrency: number;
  private discoveryConn: FramedConnection | null = null;
  private listener: Deno.TcpListener | null = null;
  private running = false;
  private listenHost: string;
  private listenPort: number;
  private acceptLoopPromise: Promise<void> | null = null;
  private discoveryLoopPromise: Promise<void> | null = null;
  private shutdownRequested = false;
  private draining = false;
  private drainResolvers: (() => void)[] = [];
  private discoveryWaiters: DiscoveryWaiter[] = [];
  private connectionPool = new Map<string, Multiplexer>();
  private incomingMuxes: Set<Multiplexer> = new Set();
  private reconnecting = false;
  private reconnectedPromise: Promise<void> | null = null;
  private reconnectedResolve: (() => void) | null = null;
  private reconnectLoopPromise: Promise<void> | null = null;
  private actorInstances = new Map<string, ActorInstance>();
  private nodeMetrics = new MetricsCollector();
  private objectStore: ObjectStore;
  private log: Logger;

  constructor(
    private config: NodeConfig,
    private registry: Registry,
    processId?: string,
  ) {
    this.processId = processId ?? crypto.randomUUID();
    this.listenHost = config.listenHost ?? "127.0.0.1";
    this.listenPort = config.listenPort ?? 0;
    this.maxConcurrency = config.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY;
    this.objectStore = new ObjectStore(this.listenHost, 0);
    this.log = new Logger({
      fields: { component: "node", node: this.processId.slice(0, 8) },
    });
  }

  async start(): Promise<void> {
    this.running = true;

    // Start TCP listener for incoming P2P connections
    this.listener = Deno.listen({
      hostname: this.listenHost,
      port: this.listenPort,
    });
    // Get the actual port if 0 was specified
    const addr = this.listener.addr as Deno.NetAddr;
    this.listenPort = addr.port;
    this.objectStore.setAddress(this.listenHost, this.listenPort);
    this.log.info(`Listening on ${this.listenHost}:${this.listenPort}`);

    // Connect to Discovery
    const rawConn = await Deno.connect({
      hostname: this.config.discoveryHost,
      port: this.config.discoveryPort,
    });
    this.discoveryConn = new FramedConnection(rawConn);

    // Register with Discovery (include both task functions and actor names)
    await this.discoveryConn.writeMessage({
      type: "register",
      processId: this.processId,
      host: this.listenHost,
      port: this.listenPort,
      funcs: [...this.registry.list(), ...this.registry.listActors()],
      maxConcurrency: this.maxConcurrency,
    });

    // Wait for registered response (validated)
    const response = await this.discoveryConn.readDiscoveryMessage();
    if (response === null || response.type !== "registered") {
      throw new Error("Failed to register with Discovery");
    }
    this.log.info("Registered with Discovery");

    // Start discovery message loop (heartbeat, shutdown, and dispatch to waiters)
    this.discoveryLoopPromise = this.discoveryLoop();

    // Start accepting P2P connections
    this.acceptLoopPromise = this.acceptLoop();
  }

  async close(drainTimeout = 30_000): Promise<void> {
    this.draining = true;
    this.running = false;

    // Reject all pending discovery waiters
    for (const waiter of this.discoveryWaiters) {
      waiter.reject(new Error("Node is closing"));
    }
    this.discoveryWaiters = [];

    // Close listener — stop accepting new P2P connections
    if (this.listener) {
      try {
        this.listener.close();
      } catch {
        // already closed
      }
      this.listener = null;
    }

    // Send bye to Discovery so we won't be routed to anymore
    if (this.discoveryConn && !this.discoveryConn.isClosed) {
      try {
        await this.discoveryConn.writeMessage({ type: "bye" });
      } catch {
        // already closed
      }
    }

    // Wait for in-flight tasks to complete (graceful drain)
    if (this.activeTasks > 0) {
      this.log.info(`Draining ${this.activeTasks} active task(s)`);
      await this.waitForDrain(drainTimeout);
      if (this.activeTasks > 0) {
        this.log.warn(`Drain timeout reached, ${this.activeTasks} task(s) still active`);
      }
    }

    // Close discovery connection
    if (this.discoveryConn && !this.discoveryConn.isClosed) {
      this.discoveryConn.close();
    }

    // Close all pooled outgoing connections
    for (const [, mux] of this.connectionPool) {
      mux.close();
    }
    this.connectionPool.clear();

    // Close all incoming multiplexers
    for (const mux of this.incomingMuxes) {
      mux.close();
    }
    this.incomingMuxes.clear();

    // Destroy all actor instances
    for (const [, actor] of this.actorInstances) {
      actor.destroy();
    }
    this.actorInstances.clear();

    // Abort any in-progress reconnection
    if (this.reconnectedResolve) {
      this.reconnectedResolve();
      this.reconnectedPromise = null;
      this.reconnectedResolve = null;
    }

    // Wait for loops to finish
    await Promise.allSettled([
      this.acceptLoopPromise,
      this.discoveryLoopPromise,
      this.reconnectLoopPromise,
    ].filter(Boolean));

    this.log.info("Closed");
  }

  /**
   * Wait until all active tasks complete or the timeout expires.
   */
  private waitForDrain(timeout: number): Promise<void> {
    if (this.activeTasks === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, timeout);
      this.drainResolvers.push(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  /**
   * Spawn a task on a remote peer. Queries Discovery for an idle peer,
   * connects P2P, sends exec, handles reject+retry, returns a Channel.
   */
  spawn(func: string, args: unknown[], opts?: SpawnOptions): Channel {
    const timeout = opts?.timeout ?? DEFAULT_SPAWN_TIMEOUT;
    const execTimeout = opts?.execTimeout;
    const highWaterMark = opts?.highWaterMark;
    const taskId = crypto.randomUUID();

    return new PendingChannel(
      () => this.doSpawn(func, args, taskId, timeout, execTimeout, highWaterMark),
      taskId,
    );
  }

  /**
   * Create an actor on a remote peer. Queries Discovery for an available peer,
   * connects P2P, sends actor_create, and returns an ActorHandle.
   */
  async createActor(name: string): Promise<ActorHandle> {
    const peers = await this.findPeers(name, []);

    // If no peers found by actor name, try any peer that has capacity
    // Actor creation uses the same peer discovery as tasks
    if (peers.length === 0) {
      throw new SpawnError(`No peers available for actor "${name}"`);
    }

    for (const peer of peers) {
      try {
        const mux = await this.getMultiplexer(peer.host, peer.port);
        return await requestActorCreation(mux, name);
      } catch {
        // Try next peer
      }
    }

    throw new SpawnError(`Failed to create actor "${name}" on any peer`);
  }

  put(data: unknown): ObjectRef {
    return this.objectStore.put(data);
  }

  getObjectStore(): ObjectStore {
    return this.objectStore;
  }

  /**
   * Wait for a specific discovery response message type.
   * The discoveryLoop dispatches matching messages to these waiters.
   */
  private waitForDiscovery(...types: string[]): Promise<DiscoveryMessage> {
    return new Promise<DiscoveryMessage>((resolve, reject) => {
      this.discoveryWaiters.push({ types, resolve, reject });
    });
  }

  /**
   * Send a message to Discovery and wait for a response of a specific type.
   */
  private async discoveryRequest(
    msg: DiscoveryMessage,
    ...responseTypes: string[]
  ): Promise<DiscoveryMessage> {
    // Wait for reconnection to complete if in progress
    if (this.reconnecting && this.reconnectedPromise) {
      await this.reconnectedPromise;
    }
    if (!this.discoveryConn || this.discoveryConn.isClosed) {
      throw new SpawnError("Discovery connection not available");
    }
    const promise = this.waitForDiscovery(...responseTypes);
    await this.discoveryConn.writeMessage(msg);
    return promise;
  }

  private async doSpawn(
    func: string,
    args: unknown[],
    taskId: string,
    timeout: number,
    execTimeout: number | undefined,
    highWaterMark: number | undefined,
  ): Promise<Channel> {
    const deadline = Date.now() + timeout;
    const excluded: string[] = [];
    let retries = 0;

    while (true) {
      if (Date.now() >= deadline) {
        throw new SpawnTimeoutError(func, timeout);
      }

      // Ask Discovery for idle peers
      const peers = await this.findPeers(func, excluded);

      if (peers.length === 0) {
        // No peers available - request spawn from Discovery
        await this.requestSpawn();
        // Wait for the new process to register
        await new Promise<void>((r) => setTimeout(r, 500));
        continue;
      }

      // Try each candidate peer
      for (const peer of peers) {
        if (Date.now() >= deadline) {
          throw new SpawnTimeoutError(func, timeout);
        }

        try {
          const channel = await this.tryExecOnPeer(
            peer,
            func,
            args,
            taskId,
            execTimeout,
            highWaterMark,
          );
          if (channel) return channel;
        } catch {
          // Connection error - try next peer
        }

        // This peer rejected or failed - exclude it
        excluded.push(peer.processId);
        retries++;
      }

      if (retries >= MAX_RETRY) {
        // Request Discovery to spawn a new process, then retry
        await this.requestSpawn();
        // Wait for the new process to register
        await new Promise<void>((r) => setTimeout(r, 500));
        retries = 0; // reset retry counter for new round
      }
    }
  }

  private async findPeers(
    func: string,
    exclude: string[],
  ): Promise<PeerInfo[]> {
    const response = await this.discoveryRequest(
      {
        type: "find",
        func,
        exclude: exclude.length > 0 ? exclude : undefined,
      },
      "found",
    );
    const found = response as FoundMessage;
    return found.peers;
  }

  private async requestSpawn(): Promise<void> {
    const response = await this.discoveryRequest(
      {
        type: "request_spawn",
        reason: this.activeTasks > 0 ? "overflow" : "no_idle",
      },
      "spawn_result",
    );
    const result = response as SpawnResultMessage;
    if (!result.success) {
      // Spawn failed at Discovery, but we'll still retry finding peers
    }
  }

  /**
   * Get or create a pooled Multiplexer for a given host:port.
   */
  private async getMultiplexer(host: string, port: number): Promise<Multiplexer> {
    const key = `${host}:${port}`;
    let mux = this.connectionPool.get(key);
    if (mux && !mux.isClosed) return mux;

    const conn = await Deno.connect({ hostname: host, port });
    const fc = new FramedConnection(conn);
    mux = new Multiplexer(fc);
    mux.onClose(() => {
      this.connectionPool.delete(key);
    });
    mux.startReading();
    this.connectionPool.set(key, mux);
    return mux;
  }

  private async tryExecOnPeer(
    peer: PeerInfo,
    func: string,
    args: unknown[],
    taskId: string,
    execTimeout: number | undefined,
    highWaterMark: number | undefined,
  ): Promise<Channel | null> {
    const mux = await this.getMultiplexer(peer.host, peer.port);

    // Send exec request
    const execMsg: ExecMessage = {
      type: "exec",
      taskId,
      func,
      args,
    };
    if (execTimeout !== undefined) {
      execMsg.execTimeout = execTimeout;
    }
    await mux.writeMessage(execMsg);

    // Wait for accept, reject, or error (connection closed) via a temporary handler
    const response = await new Promise<P2PMessage>((resolve, reject) => {
      mux.registerTask(taskId, (msg: P2PMessage) => {
        if (msg.type === "accept" || msg.type === "reject") {
          mux.unregisterTask(taskId);
          resolve(msg);
        } else if (msg.type === "error") {
          // Connection closed - mux sends synthetic error on close
          mux.unregisterTask(taskId);
          reject(new Error("Connection closed"));
        }
      });
    });

    if (response.type === "accept") {
      return createChannel(mux, taskId, highWaterMark);
    }

    // Rejected - don't close the mux, it may have other tasks
    return null;
  }

  /**
   * Single reader loop for the discovery connection.
   * Handles heartbeats and shutdown directly, and dispatches
   * other messages (found, spawn_result) to pending waiters.
   */
  private async discoveryLoop(): Promise<void> {
    if (!this.discoveryConn) return;
    try {
      while (this.running && !this.discoveryConn.isClosed) {
        const msg = await this.discoveryConn.readDiscoveryMessage();
        if (msg === null) break;

        switch (msg.type) {
          case "heartbeat":
            if (!this.discoveryConn.isClosed) {
              await this.discoveryConn.writeMessage({ type: "heartbeat_ack" });
            }
            break;
          case "shutdown":
            this.log.info("Received shutdown from Discovery");
            this.shutdownRequested = true;
            this.running = false;
            await this.close();
            return;
          // Messages dispatched to pending waiters
          case "registered":
          case "found":
          case "spawn_result":
          case "heartbeat_ack":
            this.dispatchToWaiters(msg);
            break;
          // Messages that a Node sends (not expected from Discovery)
          case "register":
          case "capacity_change":
          case "find":
          case "request_spawn":
          case "bye":
            break;
          default:
            assertNever(msg);
        }
      }
    } catch (e) {
      if (this.running) {
        this.log.error(`Discovery connection lost: ${e instanceof Error ? e.message : String(e)}`);
      }
    } finally {
      if (!this.draining && !this.shutdownRequested && this.running) {
        // Trigger automatic reconnection
        this.startReconnect();
      } else {
        // Shutting down — reject any remaining waiters
        for (const waiter of this.discoveryWaiters) {
          waiter.reject(new Error("Discovery connection closed"));
        }
        this.discoveryWaiters = [];
      }
    }
  }

  private dispatchToWaiters(msg: DiscoveryMessage): void {
    const idx = this.discoveryWaiters.findIndex((w) =>
      w.types.includes(msg.type)
    );
    if (idx >= 0) {
      const waiter = this.discoveryWaiters[idx]!;
      this.discoveryWaiters.splice(idx, 1);
      waiter.resolve(msg);
    }
  }

  private startReconnect(): void {
    if (this.reconnecting) return;
    this.reconnecting = true;
    this.reconnectedPromise = new Promise<void>((resolve) => {
      this.reconnectedResolve = resolve;
    });
    this.reconnectLoopPromise = this.reconnectLoop();
  }

  private async reconnectLoop(): Promise<void> {
    let delay = RECONNECT_INITIAL_DELAY;

    while (this.running && !this.draining && !this.shutdownRequested) {
      this.log.info(`Reconnecting to Discovery in ${delay}ms`);
      await new Promise<void>((r) => setTimeout(r, delay));

      if (!this.running || this.draining || this.shutdownRequested) break;

      try {
        const rawConn = await Deno.connect({
          hostname: this.config.discoveryHost,
          port: this.config.discoveryPort,
        });
        const fc = new FramedConnection(rawConn);

        // Re-register with Discovery (include both task functions and actor names)
        await fc.writeMessage({
          type: "register",
          processId: this.processId,
          host: this.listenHost,
          port: this.listenPort,
          funcs: [...this.registry.list(), ...this.registry.listActors()],
          maxConcurrency: this.maxConcurrency,
        });

        const response = await fc.readDiscoveryMessage();
        if (response === null || response.type !== "registered") {
          fc.close();
          throw new Error("Failed to register with Discovery");
        }

        this.discoveryConn = fc;
        this.log.info("Reconnected to Discovery");

        // Report current capacity so Discovery has accurate state
        await this.discoveryConn.writeMessage({
          type: "capacity_change",
          activeTasks: this.activeTasks,
          maxConcurrency: this.maxConcurrency,
        });

        // Restart discovery message loop
        this.discoveryLoopPromise = this.discoveryLoop();

        // Signal reconnection success
        this.reconnecting = false;
        this.reconnectedResolve?.();
        this.reconnectedPromise = null;
        this.reconnectedResolve = null;
        return;
      } catch (e) {
        this.log.error(`Reconnection failed: ${e}`);
        delay = Math.min(delay * 2, RECONNECT_MAX_DELAY);
      }
    }

    // Reconnection aborted (shutting down)
    this.reconnecting = false;
    for (const waiter of this.discoveryWaiters) {
      waiter.reject(new Error("Discovery reconnection aborted"));
    }
    this.discoveryWaiters = [];
    this.reconnectedResolve?.();
    this.reconnectedPromise = null;
    this.reconnectedResolve = null;
  }

  private async acceptLoop(): Promise<void> {
    while (this.running && this.listener) {
      try {
        const conn = await this.listener.accept();
        this.handleIncomingConnection(conn);
      } catch {
        if (!this.running) break;
      }
    }
  }

  /**
   * Handle an incoming P2P connection by creating a persistent Multiplexer.
   * The unrouted handler receives new exec messages.
   */
  private handleIncomingConnection(conn: Deno.Conn): void {
    const fc = new FramedConnection(conn);
    const mux = new Multiplexer(fc);
    this.incomingMuxes.add(mux);
    mux.onClose(() => {
      this.incomingMuxes.delete(mux);
    });
    mux.setUnroutedHandler((msg: P2PMessage) => {
      switch (msg.type) {
        case "exec":
          this.handleExecRequest(mux, msg);
          break;
        case "actor_create":
          this.handleActorCreate(mux, msg);
          break;
        case "actor_call":
          this.handleActorCall(mux, msg);
          break;
        case "actor_destroy":
          this.handleActorDestroy(mux, msg);
          break;
        case "object_fetch":
          this.handleObjectFetch(mux, msg);
          break;
      }
    });
    mux.startReading();
  }

  /**
   * Handle an incoming exec request. Synchronous accept/reject check
   * (no await between state check and state change) to prevent races.
   */
  private handleExecRequest(mux: Multiplexer, msg: ExecMessage): void {
    if (this.draining || this.activeTasks >= this.maxConcurrency) {
      this.nodeMetrics.recordRejection("at_capacity");
      mux.writeMessage({
        type: "reject",
        taskId: msg.taskId,
        reason: "at_capacity",
      }).catch(() => {});
      return;
    }

    const taskFn = this.registry.get(msg.func);
    if (!taskFn) {
      this.nodeMetrics.recordRejection("unknown_func");
      mux.writeMessage({
        type: "reject",
        taskId: msg.taskId,
        reason: "unknown_func",
      }).catch(() => {});
      return;
    }

    // Accept - increment synchronously before any await
    this.activeTasks++;
    this.executeTask(mux, msg, taskFn);
  }

  private async executeTask(
    mux: Multiplexer,
    msg: ExecMessage,
    taskFn: TaskFunction,
  ): Promise<void> {
    const taskLog = this.log.child({ task: msg.taskId.slice(0, 8), func: msg.func });
    this.notifyCapacityChange();

    try {
      await mux.writeMessage({
        type: "accept",
        taskId: msg.taskId,
      });
    } catch {
      this.activeTasks--;
      this.notifyCapacityChange();
      return;
    }

    this.nodeMetrics.recordTaskSpawned();
    taskLog.info("Executing task");

    // Resolve any ObjectRef arguments to actual data
    const resolvedArgs = await this.resolveObjectRefs(msg.args);
    const startTime = Date.now();

    const spawnFn = (func: string, args: unknown[]): Channel => {
      return this.spawn(func, args);
    };
    const ctx = createContext(mux, msg.taskId, spawnFn);

    try {
      let result: unknown;
      const execTimeout = msg.execTimeout;

      if (execTimeout && execTimeout > 0) {
        // Race task with timeout, clearing the timer on completion
        let timeoutId: number | undefined;
        try {
          result = await Promise.race([
            taskFn(ctx, ...resolvedArgs),
            new Promise<never>((_, reject) => {
              timeoutId = setTimeout(() => {
                reject(new ExecTimeoutError(msg.func, execTimeout));
              }, execTimeout);
            }),
          ]);
        } finally {
          if (timeoutId !== undefined) clearTimeout(timeoutId);
        }
      } else {
        result = await taskFn(ctx, ...resolvedArgs);
      }

      this.nodeMetrics.recordTaskCompleted();
      this.nodeMetrics.recordTaskLatency(Date.now() - startTime);
      taskLog.info("Task completed");
      await mux.writeMessage({
        type: "result",
        taskId: msg.taskId,
        value: result,
      });
    } catch (e: unknown) {
      const err = e instanceof Error ? e : new Error(String(e));
      this.nodeMetrics.recordTaskFailed();
      this.nodeMetrics.recordTaskLatency(Date.now() - startTime);
      taskLog.error(`Task failed: ${err.message}`);
      try {
        await mux.writeMessage({
          type: "error",
          taskId: msg.taskId,
          error: {
            message: err.message,
            name: err.name,
            stack: err.stack,
          },
        });
      } catch {
        // Connection already closed
      }
    } finally {
      ctx.dispose();
      this.activeTasks--;
      this.notifyCapacityChange();

      // If draining and no more active tasks, resolve drain waiters
      if (this.draining && this.activeTasks === 0) {
        for (const resolver of this.drainResolvers) {
          resolver();
        }
        this.drainResolvers = [];
      }

      this.healthCheck();
    }
  }

  private handleActorCreate(mux: Multiplexer, msg: ActorCreateMessage): void {
    const actorLog = this.log.child({ actor: msg.actorName });

    if (this.draining || this.activeTasks >= this.maxConcurrency) {
      actorLog.debug("Actor create rejected: at capacity");
      mux.writeMessage({
        type: "actor_create_result",
        taskId: msg.taskId,
        error: { message: "At capacity", name: "SpawnError" },
      }).catch(() => {});
      return;
    }

    const cls = this.registry.getActor(msg.actorName);
    if (!cls) {
      actorLog.debug("Actor create rejected: unknown actor");
      mux.writeMessage({
        type: "actor_create_result",
        taskId: msg.taskId,
        error: { message: `Unknown actor "${msg.actorName}"`, name: "SpawnError" },
      }).catch(() => {});
      return;
    }

    const actorId = crypto.randomUUID();
    const instance = new ActorInstance(actorId, cls);
    this.actorInstances.set(actorId, instance);
    this.activeTasks++;
    this.nodeMetrics.recordTaskSpawned();
    this.notifyCapacityChange();
    actorLog.info(`Actor created: ${actorId.slice(0, 8)}`);

    mux.writeMessage({
      type: "actor_create_result",
      taskId: msg.taskId,
      actorId,
    }).catch(() => {
      this.actorInstances.delete(actorId);
      instance.destroy();
      this.activeTasks--;
      this.notifyCapacityChange();
    });
  }

  private async handleActorCall(mux: Multiplexer, msg: ActorCallMessage): Promise<void> {
    const instance = this.actorInstances.get(msg.actorId);
    if (!instance) {
      this.log.debug(`Actor call failed: actor ${msg.actorId.slice(0, 8)} not found`);
      await mux.writeMessage({
        type: "actor_error",
        taskId: msg.taskId,
        error: { message: `Actor "${msg.actorId}" not found`, name: "Error" },
      }).catch(() => {});
      return;
    }
    try {
      const value = await instance.call(msg.method, msg.args);
      await mux.writeMessage({ type: "actor_result", taskId: msg.taskId, value });
    } catch (e: unknown) {
      const err = e instanceof Error ? e : new Error(String(e));
      this.log.warn(`Actor call failed: ${msg.actorId.slice(0, 8)}.${msg.method}: ${err.message}`);
      await mux.writeMessage({
        type: "actor_error",
        taskId: msg.taskId,
        error: { message: err.message, name: err.name, stack: err.stack },
      }).catch(() => {});
    }
  }

  private handleActorDestroy(mux: Multiplexer, msg: ActorDestroyMessage): void {
    const instance = this.actorInstances.get(msg.actorId);
    if (!instance) {
      mux.writeMessage({
        type: "actor_error",
        taskId: msg.taskId,
        error: { message: `Actor "${msg.actorId}" not found`, name: "Error" },
      }).catch(() => {});
      return;
    }

    instance.destroy();
    this.actorInstances.delete(msg.actorId);
    this.activeTasks--;
    this.nodeMetrics.recordTaskCompleted();
    this.notifyCapacityChange();
    this.log.info(`Actor destroyed: ${msg.actorId.slice(0, 8)}`);

    if (this.draining && this.activeTasks === 0) {
      for (const resolver of this.drainResolvers) { resolver(); }
      this.drainResolvers = [];
    }

    mux.writeMessage({ type: "actor_result", taskId: msg.taskId, value: null }).catch(() => {});
    this.healthCheck();
  }

  private handleObjectFetch(mux: Multiplexer, msg: ObjectFetchMessage): void {
    const found = this.objectStore.has(msg.objectId);
    const data = found ? this.objectStore.getById(msg.objectId) : null;
    this.log.debug(`Object fetch: ${msg.objectId.slice(0, 8)} found=${found}`);
    mux.writeMessage({
      type: "object_fetch_response",
      taskId: msg.taskId,
      objectId: msg.objectId,
      found,
      data,
    }).catch(() => {});
  }

  private async resolveObjectRefs(args: unknown[]): Promise<unknown[]> {
    const resolved: unknown[] = [];
    for (const arg of args) {
      if (arg instanceof ObjectRef) {
        if (this.objectStore.has(arg.id)) {
          resolved.push(this.objectStore.getById(arg.id));
        } else {
          const data = await this.fetchRemoteObject(arg);
          this.objectStore.cache(arg.id, data);
          resolved.push(data);
        }
      } else {
        resolved.push(arg);
      }
    }
    return resolved;
  }

  private async fetchRemoteObject(ref: ObjectRef): Promise<unknown> {
    // Connect directly to the owner node using its address
    try {
      const mux = await this.getMultiplexer(ref.ownerHost, ref.ownerPort);
      return await this.fetchObjectFromMux(mux, ref.id);
    } catch {
      // Owner node unreachable
    }
    // Fallback: try existing connections
    for (const [, mux] of this.connectionPool) {
      if (mux.isClosed) continue;
      try {
        return await this.fetchObjectFromMux(mux, ref.id);
      } catch {
        continue;
      }
    }
    for (const mux of this.incomingMuxes) {
      if (mux.isClosed) continue;
      try {
        return await this.fetchObjectFromMux(mux, ref.id);
      } catch {
        continue;
      }
    }
    throw new Error(`Failed to fetch object ${ref.id} from owner ${ref.ownerHost}:${ref.ownerPort}`);
  }

  private fetchObjectFromMux(mux: Multiplexer, objectId: string): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      const taskId = crypto.randomUUID();
      const timer = setTimeout(() => {
        mux.unregisterTask(taskId);
        reject(new Error("Object fetch timeout"));
      }, 10_000);
      mux.registerTask(taskId, (msg: P2PMessage) => {
        clearTimeout(timer);
        mux.unregisterTask(taskId);
        if (msg.type === "object_fetch_response" && msg.objectId === objectId) {
          if (msg.found) {
            resolve(msg.data);
          } else {
            reject(new Error(`Object ${objectId} not found on remote`));
          }
        } else {
          reject(new Error("Unexpected response"));
        }
      });
      mux.writeMessage({
        type: "object_fetch",
        taskId,
        objectId,
      }).catch((err) => {
        clearTimeout(timer);
        mux.unregisterTask(taskId);
        reject(err);
      });
    });
  }

  private healthCheck(): void {
    if (!this.checkHealth()) {
      this.log.error("Health check failed, shutting down");
      this.running = false;
      this.close();
    }
  }

  private checkHealth(): boolean {
    try {
      const mem = Deno.memoryUsage();
      if (mem.heapUsed > MEMORY_THRESHOLD) {
        this.log.warn(`Memory usage too high: ${mem.heapUsed} bytes`);
        return false;
      }
      return true;
    } catch {
      return false;
    }
  }

  private notifyCapacityChange(): void {
    if (this.discoveryConn && !this.discoveryConn.isClosed) {
      this.discoveryConn.writeMessage({
        type: "capacity_change",
        activeTasks: this.activeTasks,
        maxConcurrency: this.maxConcurrency,
      }).catch(() => {
        // Discovery connection may be lost
      });
    }
  }

  getMetrics(): MetricsSnapshot {
    this.nodeMetrics.setActiveTaskCount(this.activeTasks);
    return this.nodeMetrics.getMetrics();
  }

  get isShutdownRequested(): boolean {
    return this.shutdownRequested;
  }

  get id(): string {
    return this.processId;
  }

  get port(): number {
    return this.listenPort;
  }
}

// ============================================================
// PendingChannel: wraps async spawn negotiation into a Channel
// ============================================================

class PendingChannel implements Channel {
  private innerPromise: Promise<Channel> | null = null;
  private inner: Channel | null = null;
  returnValue: unknown = undefined;

  constructor(
    private factory: () => Promise<Channel>,
    private _taskId: string,
  ) {}

  private getInner(): Promise<Channel> {
    if (this.inner) return Promise.resolve(this.inner);
    if (!this.innerPromise) {
      this.innerPromise = this.factory().then((ch) => {
        this.inner = ch;
        return ch;
      });
    }
    return this.innerPromise;
  }

  async receive(): Promise<ReceiveResult> {
    const ch = await this.getInner();
    const result = await ch.receive();
    this.returnValue = ch.returnValue;
    return result;
  }

  async send(value: unknown): Promise<void> {
    const ch = await this.getInner();
    await ch.send(value);
  }

  cancel(): void {
    if (this.inner) {
      this.inner.cancel();
    } else {
      this.getInner().then((ch) => ch.cancel()).catch(() => {
        // spawn failed, nothing to cancel
      });
    }
  }

  async join(): Promise<unknown> {
    const ch = await this.getInner();
    const result = await ch.join();
    this.returnValue = ch.returnValue;
    return result;
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
    const ch = await this.getInner();
    for await (const value of ch) {
      yield value;
    }
    this.returnValue = ch.returnValue;
  }
}

// ============================================================
// Public factory: connect to Discovery and create a ProcessNode
// ============================================================

export function toRegistry(value: Registry | TaskRecord): Registry {
  if (value instanceof Registry) return value;
  return Registry.from(value);
}

export async function connect(
  opts: NodeConfig,
): Promise<ProcessNode> {
  let registry = new Registry();

  if (opts.registry) {
    const registryPath = opts.registry.startsWith(".")
      ? new URL(opts.registry, `file://${Deno.cwd()}/`).href
      : opts.registry;
    let mod;
    try {
      mod = await import(registryPath);
    } catch (e) {
      throw new Error(`Failed to load registry "${opts.registry}": ${e}`);
    }
    if (mod.default == null) {
      throw new Error(`Registry module "${opts.registry}" has no default export`);
    }
    registry = toRegistry(mod.default);
  }

  const node = new ProcessNode(opts, registry);
  await node.start();
  return node;
}
