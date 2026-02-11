import type { TaskFunction } from "./types.ts";

export class Registry {
  private fns = new Map<string, TaskFunction>();

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
