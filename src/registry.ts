import type { TaskFunction } from "./types.ts";

export type TaskRecord = Record<string, TaskFunction>;

export class Registry {
  private fns = new Map<string, TaskFunction>();

  /**
   * Create a Registry from either an existing Registry instance or a
   * plain Record<string, TaskFunction> object.
   *
   * Usage:
   *   Registry.from({ multiply: async (ctx, a, b) => a * b })
   *   Registry.from(existingRegistry)  // returns as-is
   */
  static from(source: Registry | TaskRecord): Registry {
    if (source instanceof Registry) {
      return source;
    }
    const registry = new Registry();
    for (const [name, value] of Object.entries(source)) {
      if (typeof value !== "function") {
        throw new Error(
          `Registry.from: "${name}" is not a function (got ${typeof value})`,
        );
      }
      registry.register(name, value as TaskFunction);
    }
    return registry;
  }

  register(name: string, fn: TaskFunction): void {
    if (this.fns.has(name)) {
      throw new Error(`Function "${name}" is already registered`);
    }
    this.fns.set(name, fn);
  }

  get(name: string): TaskFunction | undefined {
    return this.fns.get(name);
  }

  list(): string[] {
    return [...this.fns.keys()];
  }
}
