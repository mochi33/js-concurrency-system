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

export type P2PMessage =
  | ExecMessage
  | AcceptMessage
  | RejectMessage
  | SendMessage
  | ResultMessage
  | ErrorMessage
  | CancelMessage;

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
  registry: string;
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
// Task function type
// ============================================================

// deno-lint-ignore no-explicit-any
export type TaskFunction = (ctx: Context, ...args: any[]) => any;
