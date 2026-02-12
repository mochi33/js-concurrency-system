import type { TaskFunction } from "./types.ts";

export type TaskRecord = Record<string, TaskFunction>;

// deno-lint-ignore no-explicit-any
export type TaskLoader = () => Promise<{ default: TaskFunction } | { [key: string]: any }>;

export class Registry {
  private fns = new Map<string, TaskFunction>();
  private loaders = new Map<string, TaskLoader>();

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
    if (source == null || typeof source !== "object") {
      throw new Error(
        `Registry.from: expected a Registry or Record<string, TaskFunction>, got ${typeof source}`,
      );
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

  /**
   * Register a task function eagerly (loaded immediately).
   */
  register(name: string, fn: TaskFunction): void {
    if (this.fns.has(name) || this.loaders.has(name)) {
      throw new Error(`Function "${name}" is already registered`);
    }
    this.fns.set(name, fn);
  }

  /**
   * Register a task function lazily. The loader is called only on first
   * execution and the result is cached.
   *
   * The loader should return a module with a default export (TaskFunction),
   * or return a TaskFunction directly.
   *
   * Usage:
   *   registry.lazy("heavyCalc", () => import("./tasks/heavy_calc.ts"));
   */
  lazy(name: string, loader: TaskLoader): void {
    if (this.fns.has(name) || this.loaders.has(name)) {
      throw new Error(`Function "${name}" is already registered`);
    }
    this.loaders.set(name, loader);
  }

  /**
   * Check if a task name is registered (eager or lazy). Synchronous.
   */
  has(name: string): boolean {
    return this.fns.has(name) || this.loaders.has(name);
  }

  /**
   * Get an eagerly-registered task function. Returns undefined for
   * lazy entries that haven't been resolved yet.
   */
  get(name: string): TaskFunction | undefined {
    return this.fns.get(name);
  }

  /**
   * Resolve a task function by name. For lazy entries, this triggers
   * the import and caches the result. Subsequent calls return the
   * cached function immediately.
   */
  async resolve(name: string): Promise<TaskFunction | undefined> {
    // Check eager cache first
    const fn = this.fns.get(name);
    if (fn) return fn;

    // Check for lazy loader
    const loader = this.loaders.get(name);
    if (!loader) return undefined;

    // Load, extract, and cache
    const loaded = await loader();
    let taskFn: TaskFunction;

    if (typeof loaded === "function") {
      taskFn = loaded as TaskFunction;
    } else if (
      loaded && typeof loaded === "object" && "default" in loaded &&
      typeof loaded.default === "function"
    ) {
      taskFn = loaded.default as TaskFunction;
    } else {
      throw new Error(
        `Lazy loader for "${name}" did not return a valid TaskFunction`,
      );
    }

    // Cache and remove loader
    this.fns.set(name, taskFn);
    this.loaders.delete(name);
    return taskFn;
  }

  /**
   * List all registered function names (eager + lazy).
   */
  list(): string[] {
    return [...new Set([...this.fns.keys(), ...this.loaders.keys()])];
  }
}
