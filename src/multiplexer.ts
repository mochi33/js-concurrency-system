import { FramedConnection } from "./protocol.ts";
import type { Message, P2PMessage } from "./types.ts";

/**
 * Callback for handling messages routed to a specific taskId.
 */
export type TaskMessageHandler = (msg: P2PMessage) => void;

/**
 * Callback for handling messages that don't have a taskId (e.g., exec requests).
 */
export type UnroutedMessageHandler = (msg: P2PMessage) => void;

/**
 * Multiplexes multiple tasks over a single TCP connection.
 *
 * Instead of one FramedConnection per task, a Multiplexer allows
 * multiple tasks to share a connection by routing messages based on taskId.
 */
export class Multiplexer {
  private handlers = new Map<string, TaskMessageHandler>();
  private unroutedHandler: UnroutedMessageHandler | null = null;
  private closed = false;
  private readLoopPromise: Promise<void> | null = null;
  private onCloseCallback: (() => void) | null = null;

  constructor(private conn: FramedConnection) {}

  /**
   * Register a callback to be invoked when this multiplexer is closed.
   */
  onClose(callback: () => void): void {
    this.onCloseCallback = callback;
  }

  /**
   * Start the read loop. Must be called after construction.
   * Reads messages and dispatches them to registered task handlers.
   */
  startReading(): void {
    if (this.readLoopPromise) return;
    this.readLoopPromise = this.readLoop();
  }

  /**
   * Register a handler for messages with a specific taskId.
   */
  registerTask(taskId: string, handler: TaskMessageHandler): void {
    this.handlers.set(taskId, handler);
  }

  /**
   * Unregister a handler when a task is complete.
   */
  unregisterTask(taskId: string): void {
    this.handlers.delete(taskId);
  }

  /**
   * Set a handler for incoming messages that aren't routed by taskId
   * (e.g., new exec requests on the executor side).
   */
  setUnroutedHandler(handler: UnroutedMessageHandler): void {
    this.unroutedHandler = handler;
  }

  /**
   * Write a message to the shared connection.
   */
  async writeMessage(msg: Message): Promise<void> {
    if (this.closed) throw new Error("Multiplexer is closed");
    await this.conn.writeMessage(msg);
  }

  /**
   * Check if any tasks are still registered.
   */
  get hasActiveTasks(): boolean {
    return this.handlers.size > 0;
  }

  /**
   * Number of active tasks on this connection.
   */
  get activeTaskCount(): number {
    return this.handlers.size;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.conn.close();
    // Notify all handlers of closure by sending a synthetic error
    for (const [taskId, handler] of this.handlers) {
      handler({
        type: "error",
        taskId,
        error: { message: "Connection closed", name: "ConnectionError" },
      });
    }
    this.handlers.clear();
    this.onCloseCallback?.();
  }

  private async readLoop(): Promise<void> {
    try {
      while (!this.closed && !this.conn.isClosed) {
        const msg = await this.conn.readMessage();
        if (msg === null) {
          this.close();
          return;
        }
        this.dispatch(msg as P2PMessage);
      }
    } catch {
      this.close();
    }
  }

  private dispatch(msg: P2PMessage): void {
    if ("taskId" in msg && msg.taskId) {
      const handler = this.handlers.get(msg.taskId);
      if (handler) {
        handler(msg);
        return;
      }
    }
    // No registered handler for this taskId — use unrouted handler
    if (this.unroutedHandler) {
      this.unroutedHandler(msg);
    }
  }
}
