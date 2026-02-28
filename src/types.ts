// ============================================================
// Error types
// ============================================================

export class ChannelClosedError extends Error {
  constructor() {
    super("Channel is closed");
    this.name = "ChannelClosedError";
  }
}

export class CancelledError extends Error {
  constructor() {
    super("Task was cancelled");
    this.name = "CancelledError";
  }
}

export class SpawnTimeoutError extends Error {
  constructor(func: string, timeout: number) {
    super(`Spawn for "${func}" timed out after ${timeout}ms`);
    this.name = "SpawnTimeoutError";
  }
}

export class ExecTimeoutError extends Error {
  constructor(func: string, timeout: number) {
    super(`Execution of "${func}" timed out after ${timeout}ms`);
    this.name = "ExecTimeoutError";
  }
}

export class SpawnError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SpawnError";
  }
}

// ============================================================
// Serialized error (for transport)
// ============================================================

export interface SerializedError {
  message: string;
  name: string;
  stack?: string;
}

// ============================================================
// Process ↔ Discovery messages
// ============================================================

export interface RegisterMessage {
  type: "register";
  processId: string;
  host: string;
  port: number;
  funcs: string[];
  maxConcurrency: number;
}

export interface RegisteredMessage {
  type: "registered";
  processId: string;
}

// Replaces StateChangeMessage: reports current capacity
export interface CapacityChangeMessage {
  type: "capacity_change";
  activeTasks: number;
  maxConcurrency: number;
}

export interface FindMessage {
  type: "find";
  func: string;
  exclude?: string[];
}

export interface PeerInfo {
  processId: string;
  host: string;
  port: number;
}

export interface FoundMessage {
  type: "found";
  peers: PeerInfo[];
}

export interface RequestSpawnMessage {
  type: "request_spawn";
  reason: "no_idle" | "overflow";
}

export interface SpawnResultMessage {
  type: "spawn_result";
  success: boolean;
  processId?: string;
  host?: string;
  port?: number;
}

export interface HeartbeatMessage {
  type: "heartbeat";
}

export interface HeartbeatAckMessage {
  type: "heartbeat_ack";
}

export interface ShutdownMessage {
  type: "shutdown";
}

export interface ByeMessage {
  type: "bye";
}

export type DiscoveryMessage =
  | RegisterMessage
  | RegisteredMessage
  | CapacityChangeMessage
  | FindMessage
  | FoundMessage
  | RequestSpawnMessage
  | SpawnResultMessage
  | HeartbeatMessage
  | HeartbeatAckMessage
  | ShutdownMessage
  | ByeMessage;

// ============================================================
// Process ↔ Process (P2P) messages
// ============================================================

export interface ExecMessage {
  type: "exec";
  taskId: string;
  func: string;
  args: unknown[];
  execTimeout?: number;
}

export interface AcceptMessage {
  type: "accept";
  taskId: string;
}

export interface RejectMessage {
  type: "reject";
  taskId: string;
  reason: "at_capacity" | "unknown_func";
}

export interface SendMessage {
  type: "send";
  taskId: string;
  value: unknown;
}

export interface ResultMessage {
  type: "result";
  taskId: string;
  value: unknown;
}

export interface ErrorMessage {
  type: "error";
  taskId: string;
  error: SerializedError;
}

export interface CancelMessage {
  type: "cancel";
  taskId: string;
}

export interface ObjectFetchMessage {
  type: "object_fetch";
  taskId: string;
  objectId: string;
}

export interface ObjectFetchResponseMessage {
  type: "object_fetch_response";
  taskId: string;
  objectId: string;
  found: boolean;
  data: unknown;
}

// ============================================================
// Actor P2P messages
// ============================================================

export interface ActorCreateMessage {
  type: "actor_create";
  taskId: string;
  actorName: string;
}

export interface ActorCreateResultMessage {
  type: "actor_create_result";
  taskId: string;
  actorId?: string;
  error?: SerializedError;
}

export interface ActorCallMessage {
  type: "actor_call";
  taskId: string;
  actorId: string;
  method: string;
  args: unknown[];
}

export interface ActorResultMessage {
  type: "actor_result";
  taskId: string;
  value: unknown;
}

export interface ActorErrorMessage {
  type: "actor_error";
  taskId: string;
  error: SerializedError;
}

export interface ActorDestroyMessage {
  type: "actor_destroy";
  taskId: string;
  actorId: string;
}

export type P2PMessage =
  | ExecMessage
  | AcceptMessage
  | RejectMessage
  | SendMessage
  | ResultMessage
  | ErrorMessage
  | CancelMessage
  | ObjectFetchMessage
  | ObjectFetchResponseMessage
  | ActorCreateMessage
  | ActorCreateResultMessage
  | ActorCallMessage
  | ActorResultMessage
  | ActorErrorMessage
  | ActorDestroyMessage;

// ============================================================
// Union of all messages
// ============================================================

export type Message = DiscoveryMessage | P2PMessage;

// ============================================================
// Config types
// ============================================================

export interface ScalingConfig {
  min: number;
  max: number;
  overflowMax: number;
  idleTimeout: number;
}

export interface DiscoveryConfig extends ScalingConfig {
  port: number;
  host?: string;
  registry: string;
  metricsPort?: number;
}

export interface NodeConfig {
  discoveryHost: string;
  discoveryPort: number;
  listenHost?: string;
  listenPort?: number;
  registry?: string;
  maxConcurrency?: number;
}

export interface SpawnOptions {
  timeout?: number;
  execTimeout?: number;
  highWaterMark?: number;
}

// ============================================================
// Peer state (Discovery internal)
// ============================================================

export interface PeerState {
  processId: string;
  conn: Deno.Conn;
  host: string;
  port: number;
  funcs: string[];
  activeTasks: number;
  maxConcurrency: number;
  managed: boolean;
  overflow: boolean;
  lastHeartbeat: number;
  idleSince: number;
}

// ============================================================
// Channel / Context interfaces
// ============================================================

export interface ReceiveResult {
  value: unknown;
  done: boolean;
}

export interface Channel {
  receive(): Promise<ReceiveResult>;
  send(value: unknown): Promise<void>;
  cancel(): void;
  join(): Promise<unknown>;
  [Symbol.asyncIterator](): AsyncIterableIterator<unknown>;
  returnValue: unknown;
}

export interface Context {
  send(value: unknown): Promise<void>;
  receive(): Promise<unknown>;
  spawn(func: string, args: unknown[]): Channel;
  signal: AbortSignal;
}

// ============================================================
// Actor types
// ============================================================

export interface ActorHandle {
  readonly actorId: string;
  call(method: string, ...args: unknown[]): Promise<unknown>;
  destroy(): Promise<void>;
}

// deno-lint-ignore no-explicit-any
export type ActorClass = new () => any;

// ============================================================
// Task function type
// ============================================================

// deno-lint-ignore no-explicit-any
export type TaskFunction = (ctx: Context, ...args: any[]) => any;

export type TaskRecord = Record<string, TaskFunction>;

// ============================================================
// Metrics types
// ============================================================

export interface MetricsSnapshot {
  tasksSpawned: number;
  tasksCompleted: number;
  tasksFailed: number;
  tasksRejectedAtCapacity: number;
  tasksRejectedUnknownFunc: number;
  activeNodeCount: number;
  activeTaskCount: number;
  queueDepth: number;
  processSpawnCount: number;
  latencyP50: number;
  latencyP95: number;
  latencyP99: number;
  latencyCount: number;
  latencySum: number;
  spawnWaitP50: number;
  spawnWaitP95: number;
  spawnWaitP99: number;
  spawnWaitCount: number;
  spawnWaitSum: number;
}
