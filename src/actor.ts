import { Multiplexer } from "./multiplexer.ts";
import type {
  ActorHandle,
  ActorCallMessage,
  ActorCreateResultMessage,
  ActorDestroyMessage,
  ActorErrorMessage,
  ActorResultMessage,
  P2PMessage,
} from "./types.ts";

// ============================================================
// ActorInstance: server-side wrapper around an actor class instance
// ============================================================

interface MailboxItem {
  method: string;
  args: unknown[];
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
}

export class ActorInstance {
  readonly actorId: string;
  // deno-lint-ignore no-explicit-any
  private instance: any;
  private mailbox: MailboxItem[] = [];
  private processing = false;
  private destroyed = false;

  // deno-lint-ignore no-explicit-any
  constructor(actorId: string, cls: new () => any) {
    this.actorId = actorId;
    this.instance = new cls();
  }

  async call(method: string, args: unknown[]): Promise<unknown> {
    if (this.destroyed) {
      throw new Error("Actor has been destroyed");
    }
    if (typeof this.instance[method] !== "function") {
      throw new Error(`Actor has no method "${method}"`);
    }
    return new Promise<unknown>((resolve, reject) => {
      this.mailbox.push({ method, args, resolve, reject });
      this.processMailbox();
    });
  }

  private async processMailbox(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    while (this.mailbox.length > 0) {
      const item = this.mailbox.shift()!;
      try {
        const result = await this.instance[item.method](...item.args);
        item.resolve(result);
      } catch (e: unknown) {
        item.reject(e instanceof Error ? e : new Error(String(e)));
      }
    }
    this.processing = false;
  }

  destroy(): void {
    this.destroyed = true;
    // Reject any remaining mailbox items
    for (const item of this.mailbox) {
      item.reject(new Error("Actor has been destroyed"));
    }
    this.mailbox = [];
  }
}

// ============================================================
// ActorHandleImpl: client-side handle that communicates via Multiplexer
// ============================================================

class ActorHandleImpl implements ActorHandle {
  readonly actorId: string;
  private mux: Multiplexer;
  private destroyed = false;

  constructor(actorId: string, mux: Multiplexer) {
    this.actorId = actorId;
    this.mux = mux;
  }

  async call(method: string, ...args: unknown[]): Promise<unknown> {
    if (this.destroyed) {
      throw new Error("Actor handle has been destroyed");
    }

    const taskId = crypto.randomUUID();

    const callMsg: ActorCallMessage = {
      type: "actor_call",
      taskId,
      actorId: this.actorId,
      method,
      args,
    };

    const resultPromise = new Promise<unknown>((resolve, reject) => {
      this.mux.registerTask(taskId, (msg: P2PMessage) => {
        this.mux.unregisterTask(taskId);
        if (msg.type === "actor_result") {
          resolve((msg as ActorResultMessage).value);
        } else if (msg.type === "actor_error") {
          const errMsg = msg as ActorErrorMessage;
          const err = new Error(errMsg.error.message);
          err.name = errMsg.error.name;
          if (errMsg.error.stack) err.stack = errMsg.error.stack;
          reject(err);
        } else if (msg.type === "error") {
          reject(new Error("Connection closed"));
        } else {
          reject(new Error(`Unexpected message type: ${msg.type}`));
        }
      });
    });

    try {
      await this.mux.writeMessage(callMsg);
    } catch (e) {
      this.mux.unregisterTask(taskId);
      throw e;
    }
    return resultPromise;
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;

    const taskId = crypto.randomUUID();

    const destroyMsg: ActorDestroyMessage = {
      type: "actor_destroy",
      taskId,
      actorId: this.actorId,
    };

    const resultPromise = new Promise<void>((resolve, reject) => {
      this.mux.registerTask(taskId, (msg: P2PMessage) => {
        this.mux.unregisterTask(taskId);
        if (msg.type === "actor_result") {
          resolve();
        } else if (msg.type === "actor_error") {
          const errMsg = msg as ActorErrorMessage;
          reject(new Error(errMsg.error.message));
        } else {
          resolve(); // Connection closed is acceptable on destroy
        }
      });
    });

    try {
      await this.mux.writeMessage(destroyMsg);
    } catch (e) {
      this.mux.unregisterTask(taskId);
      throw e;
    }
    return resultPromise;
  }
}

export function createActorHandle(
  actorId: string,
  mux: Multiplexer,
): ActorHandle {
  return new ActorHandleImpl(actorId, mux);
}

/**
 * Request actor creation from a remote peer.
 * Sends actor_create and waits for actor_create_result.
 */
export async function requestActorCreation(
  mux: Multiplexer,
  actorName: string,
): Promise<ActorHandle> {
  const taskId = crypto.randomUUID();

  const resultPromise = new Promise<ActorHandle>((resolve, reject) => {
    mux.registerTask(taskId, (msg: P2PMessage) => {
      mux.unregisterTask(taskId);
      if (msg.type === "actor_create_result") {
        const createResult = msg as ActorCreateResultMessage;
        if (createResult.error) {
          const err = new Error(createResult.error.message);
          err.name = createResult.error.name;
          reject(err);
        } else if (createResult.actorId) {
          resolve(createActorHandle(createResult.actorId, mux));
        } else {
          reject(new Error("Actor creation returned no actorId"));
        }
      } else if (msg.type === "error") {
        reject(new Error("Connection closed during actor creation"));
      } else {
        reject(new Error(`Unexpected message type: ${msg.type}`));
      }
    });
  });

  try {
    await mux.writeMessage({
      type: "actor_create",
      taskId,
      actorName,
    });
  } catch (e) {
    mux.unregisterTask(taskId);
    throw e;
  }

  return resultPromise;
}
