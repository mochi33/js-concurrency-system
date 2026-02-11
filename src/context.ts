import { Multiplexer } from "./multiplexer.ts";
import type {
  Channel,
  Context,
  P2PMessage,
  SendMessage,
} from "./types.ts";
import { CancelledError } from "./types.ts";

class ContextImpl implements Context {
  private abortController = new AbortController();
  private receiveQueue: unknown[] = [];
  private pendingResolve:
    | { resolve: (value: unknown) => void; reject: (err: Error) => void }
    | null = null;
  private cancelled = false;

  readonly signal: AbortSignal;

  constructor(
    private mux: Multiplexer,
    private taskId: string,
    private spawnFn: (func: string, args: unknown[]) => Channel,
  ) {
    this.signal = this.abortController.signal;

    // Register with the multiplexer to receive messages for this taskId
    this.mux.registerTask(this.taskId, (msg: P2PMessage) => {
      this.handleMessage(msg);
    });
  }

  private handleMessage(msg: P2PMessage): void {
    switch (msg.type) {
      case "send": {
        const sendMsg = msg as SendMessage;
        this.enqueueReceive(sendMsg.value);
        break;
      }
      case "cancel": {
        this.handleCancel();
        break;
      }
      case "error": {
        // Connection-level error from multiplexer close
        this.handleCancel();
        break;
      }
      default:
        // Ignore other message types
        break;
    }
  }

  private handleCancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.abortController.abort();

    // Reject any pending receive
    if (this.pendingResolve) {
      const { reject } = this.pendingResolve;
      this.pendingResolve = null;
      reject(new CancelledError());
    }
  }

  private enqueueReceive(value: unknown): void {
    if (this.pendingResolve) {
      const { resolve } = this.pendingResolve;
      this.pendingResolve = null;
      resolve(value);
    } else {
      this.receiveQueue.push(value);
    }
  }

  async send(value: unknown): Promise<void> {
    const msg: SendMessage = {
      type: "send",
      taskId: this.taskId,
      value,
    };
    await this.mux.writeMessage(msg);
  }

  receive(): Promise<unknown> {
    if (this.cancelled && this.receiveQueue.length === 0) {
      return Promise.reject(new CancelledError());
    }

    if (this.receiveQueue.length > 0) {
      return Promise.resolve(this.receiveQueue.shift());
    }

    return new Promise<unknown>((resolve, reject) => {
      if (this.cancelled) {
        reject(new CancelledError());
        return;
      }
      this.pendingResolve = { resolve, reject };
    });
  }

  spawn(func: string, args: unknown[]): Channel {
    return this.spawnFn(func, args);
  }

  dispose(): void {
    this.mux.unregisterTask(this.taskId);
  }
}

export function createContext(
  mux: Multiplexer,
  taskId: string,
  spawnFn: (func: string, args: unknown[]) => Channel,
): Context & { dispose(): void } {
  return new ContextImpl(mux, taskId, spawnFn);
}
