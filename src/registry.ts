import type { ActorClass, TaskFunction, TaskRecord } from "./types.ts";

export class Registry {
  private fns = new Map<string, TaskFunction>();
  private actors = new Map<string, ActorClass>();

  static from(record: TaskRecord): Registry {
    const registry = new Registry();
    for (const [name, fn] of Object.entries(record)) {
      registry.register(name, fn);
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

  registerActor(name: string, cls: ActorClass): void {
    if (this.actors.has(name)) {
      throw new Error(`Actor "${name}" is already registered`);
    }
    this.actors.set(name, cls);
  }

  getActor(name: string): ActorClass | undefined {
    return this.actors.get(name);
  }

  listActors(): string[] {
    return [...this.actors.keys()];
  }
}
