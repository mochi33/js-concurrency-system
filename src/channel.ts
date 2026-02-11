import { Multiplexer } from "./multiplexer.ts";
import type {
  Channel,
  P2PMessage,
  ReceiveResult,
  SendMessage,
  ResultMessage,
  ErrorMessage,
  CancelMessage,
} from "./types.ts";
import { ChannelClosedError } from "./types.ts";

const DEFAULT_HIGH_WATER_MARK = 64;

// Internal queue item: either a received result or an error to throw
type QueueItem =
  | { kind: "value"; result: ReceiveResult }
  | { kind: "error"; error: Error };

class ChannelImpl implements Channel {
  private queue: QueueItem[] = [];
  private pendingResolve:
    | { resolve: (item: QueueItem) => void }
    | null = null;
  private done = false;
  private highWaterMark: number;
  returnValue: unknown = undefined;

  constructor(
    private mux: Multiplexer,
    private taskId: string,
    highWaterMark?: number,
  ) {
    this.highWaterMark = highWaterMark ?? DEFAULT_HIGH_WATER_MARK;

    // Register with the multiplexer to receive messages for this taskId
    this.mux.registerTask(this.taskId, (msg: P2PMessage) => {
      this.handleMessage(msg);
    });
  }

  private handleMessage(msg: P2PMessage): void {
    switch (msg.type) {
      case "send": {
        const sendMsg = msg as SendMessage;
        this.enqueue({
          kind: "value",
          result: { value: sendMsg.value, done: false },
        });
        break;
      }
      case "result": {
        const resultMsg = msg as ResultMessage;
        this.enqueue({
          kind: "value",
          result: { value: resultMsg.value, done: true },
        });
        this.done = true;
        this.mux.unregisterTask(this.taskId);
        break;
      }
      case "error": {
        const errorMsg = msg as ErrorMessage;
        const err = new Error(errorMsg.error.message);
        err.name = errorMsg.error.name;
        if (errorMsg.error.stack) {
          err.stack = errorMsg.error.stack;
        }
        this.enqueue({ kind: "error", error: err });
        this.done = true;
        this.mux.unregisterTask(this.taskId);
        break;
      }
      default:
        // Ignore other message types (accept, reject, cancel, exec)
        break;
    }
  }

  private enqueue(item: QueueItem): void {
    if (this.pendingResolve) {
      const { resolve } = this.pendingResolve;
      this.pendingResolve = null;
      resolve(item);
    } else {
      this.queue.push(item);
    }
  }

  private dequeue(): Promise<QueueItem> {
    if (this.queue.length > 0) {
      const item = this.queue.shift()!;
      return Promise.resolve(item);
    }

    // Nothing in queue - wait for next enqueue
    return new Promise<QueueItem>((resolve) => {
      this.pendingResolve = { resolve };
    });
  }

  async receive(): Promise<ReceiveResult> {
    if (this.done && this.queue.length === 0) {
      throw new ChannelClosedError();
    }

    const item = await this.dequeue();

    if (item.kind === "error") {
      throw item.error;
    }

    if (item.result.done) {
      this.returnValue = item.result.value;
    }

    return item.result;
  }

  async send(value: unknown): Promise<void> {
    if (this.done) {
      throw new ChannelClosedError();
    }
    const msg: SendMessage = {
      type: "send",
      taskId: this.taskId,
      value,
    };
    await this.mux.writeMessage(msg);
  }

  cancel(): void {
    if (this.done) return;
    const msg: CancelMessage = {
      type: "cancel",
      taskId: this.taskId,
    };
    // Fire-and-forget: write cancel message
    this.mux.writeMessage(msg).catch(() => {
      // Ignore write errors on cancel
    });
  }

  async join(): Promise<unknown> {
    while (true) {
      const { value, done } = await this.receive();
      if (done) {
        return value;
      }
      // Discard intermediate values
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
    while (true) {
      const { value, done } = await this.receive();
      if (done) {
        this.returnValue = value;
        return;
      }
      yield value;
    }
  }
}

export function createChannel(
  mux: Multiplexer,
  taskId: string,
  highWaterMark?: number,
): Channel {
  return new ChannelImpl(mux, taskId, highWaterMark);
}
