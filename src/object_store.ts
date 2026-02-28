// ============================================================
// ObjectRef: a reference to data stored in an ObjectStore
// ============================================================

export class ObjectRef {
  readonly id: string;
  readonly size: number;
  readonly ownerHost: string;
  readonly ownerPort: number;

  constructor(id: string, size: number, ownerHost: string, ownerPort: number) {
    this.id = id;
    this.size = size;
    this.ownerHost = ownerHost;
    this.ownerPort = ownerPort;
  }
}

// ============================================================
// ObjectStore: local object store per node
// ============================================================

interface StoredEntry {
  data: unknown;
  refCount: number;
}

export class ObjectStore {
  private store = new Map<string, StoredEntry>();
  private host: string;
  private port: number;

  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
  }

  /** Update the address after the listener port is assigned. */
  setAddress(host: string, port: number): void {
    this.host = host;
    this.port = port;
  }

  put(data: unknown): ObjectRef {
    const id = crypto.randomUUID();
    const size = estimateSize(data);
    this.store.set(id, { data, refCount: 1 });
    return new ObjectRef(id, size, this.host, this.port);
  }

  get(ref: ObjectRef): unknown {
    const entry = this.store.get(ref.id);
    if (entry === undefined) {
      throw new Error(`Object not found: ${ref.id}`);
    }
    return entry.data;
  }

  getById(id: string): unknown {
    const entry = this.store.get(id);
    if (entry === undefined) {
      throw new Error(`Object not found: ${id}`);
    }
    return entry.data;
  }

  has(id: string): boolean {
    return this.store.has(id);
  }

  delete(ref: ObjectRef): void {
    const entry = this.store.get(ref.id);
    if (entry) {
      entry.refCount--;
      if (entry.refCount <= 0) {
        this.store.delete(ref.id);
      }
    }
  }

  /** Cache a remotely fetched object. Cached entries have refCount 0 and can be evicted. */
  cache(id: string, data: unknown): void {
    if (!this.store.has(id)) {
      this.store.set(id, { data, refCount: 0 });
    }
  }

  pin(ref: ObjectRef): void {
    const entry = this.store.get(ref.id);
    if (entry) {
      entry.refCount++;
    }
  }

  unpin(ref: ObjectRef): void {
    const entry = this.store.get(ref.id);
    if (!entry || entry.refCount <= 0) return;
    entry.refCount--;
    if (entry.refCount <= 0) {
      this.store.delete(ref.id);
    }
  }

  get size(): number {
    return this.store.size;
  }
}

// ============================================================
// Utility: rough size estimation
// ============================================================

function estimateSize(data: unknown): number {
  if (data === null || data === undefined) return 0;
  if (typeof data === "string") return data.length * 2;
  if (typeof data === "number" || typeof data === "boolean") return 8;
  if (typeof data === "bigint") return 8;
  if (data instanceof Uint8Array) return data.byteLength;
  if (data instanceof ArrayBuffer) return data.byteLength;
  // Fallback: estimate via JSON serialization length
  try {
    return JSON.stringify(data).length * 2;
  } catch {
    return 0;
  }
}
