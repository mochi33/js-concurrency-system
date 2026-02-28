import type { Message } from "./types.ts";
import {
  parseDiscoveryMessage,
  parseP2PMessage,
  type ParsedDiscoveryMessage,
  type ParsedP2PMessage,
} from "./schemas.ts";
import { ObjectRef } from "./object_store.ts";

const MAX_MESSAGE_SIZE = 16 * 1024 * 1024; // 16 MiB
const LENGTH_PREFIX_SIZE = 4;

// ============================================================
// Extended JSON: replacer / reviver
// ============================================================

function replacer(_key: string, value: unknown): unknown {
  if (value === undefined) {
    return { $type: "undefined" };
  }
  if (typeof value === "bigint") {
    return { $type: "BigInt", value: value.toString() };
  }
  if (value instanceof Date) {
    return { $type: "Date", value: value.toISOString() };
  }
  if (value instanceof Map) {
    return { $type: "Map", entries: [...value.entries()] };
  }
  if (value instanceof Set) {
    return { $type: "Set", values: [...value.values()] };
  }
  if (value instanceof RegExp) {
    return { $type: "RegExp", source: value.source, flags: value.flags };
  }
  if (value instanceof Uint8Array) {
    return { $type: "Uint8Array", base64: encodeBase64(value) };
  }
  if (value instanceof ArrayBuffer) {
    return { $type: "ArrayBuffer", base64: encodeBase64(new Uint8Array(value)) };
  }
  if (value instanceof ObjectRef) {
    return { $type: "ObjectRef", id: value.id, size: value.size, ownerHost: value.ownerHost, ownerPort: value.ownerPort };
  }
  // Escape user data that has $type key
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "$type" in (value as Record<string, unknown>)
  ) {
    const obj = value as Record<string, unknown>;
    const escaped: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      escaped[k === "$type" ? "$$type" : k] = v;
    }
    return escaped;
  }
  return value;
}

function reviver(_key: string, value: unknown): unknown {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    if ("$type" in obj) {
      switch (obj.$type) {
        case "undefined":
          return undefined;
        case "BigInt":
          return BigInt(obj.value as string);
        case "Date":
          return new Date(obj.value as string);
        case "Map":
          return new Map(obj.entries as [unknown, unknown][]);
        case "Set":
          return new Set(obj.values as unknown[]);
        case "RegExp":
          return new RegExp(obj.source as string, obj.flags as string);
        case "Uint8Array":
          return decodeBase64(obj.base64 as string);
        case "ArrayBuffer":
          return decodeBase64(obj.base64 as string).buffer;
        case "ObjectRef": {
          if (typeof obj.id !== "string" || typeof obj.size !== "number" ||
              typeof obj.ownerHost !== "string" || typeof obj.ownerPort !== "number") {
            throw new Error("Invalid ObjectRef in deserialized message");
          }
          return new ObjectRef(obj.id, obj.size, obj.ownerHost, obj.ownerPort);
        }
      }
    }
    // Unescape $$type back to $type
    if ("$$type" in obj) {
      const unescaped: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(obj)) {
        unescaped[k === "$$type" ? "$type" : k] = v;
      }
      return unescaped;
    }
  }
  return value;
}

// ============================================================
// Base64 encode/decode
// ============================================================

function encodeBase64(data: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < data.length; i++) {
    binary += String.fromCharCode(data[i]!);
  }
  return btoa(binary);
}

function decodeBase64(str: string): Uint8Array {
  const binary = atob(str);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

// ============================================================
// Serialize / Deserialize
// ============================================================

export function serialize(msg: Message): Uint8Array {
  const json = JSON.stringify(msg, replacer);
  return new TextEncoder().encode(json);
}

export function deserialize(data: Uint8Array): Message {
  const json = new TextDecoder().decode(data);
  return JSON.parse(json, reviver) as Message;
}

export function deserializeDiscovery(data: Uint8Array): ParsedDiscoveryMessage {
  const json = new TextDecoder().decode(data);
  const raw = JSON.parse(json, reviver);
  return parseDiscoveryMessage(raw);
}

export function deserializeP2P(data: Uint8Array): ParsedP2PMessage {
  const json = new TextDecoder().decode(data);
  const raw = JSON.parse(json, reviver);
  return parseP2PMessage(raw);
}

// ============================================================
// Frame encoding: 4-byte length prefix + payload
// ============================================================

export function encodeFrame(msg: Message): Uint8Array {
  const payload = serialize(msg);
  if (payload.length > MAX_MESSAGE_SIZE) {
    throw new Error(
      `Message size ${payload.length} exceeds maximum ${MAX_MESSAGE_SIZE}`
    );
  }
  const frame = new Uint8Array(LENGTH_PREFIX_SIZE + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint32(0, payload.length, false); // BigEndian
  frame.set(payload, LENGTH_PREFIX_SIZE);
  return frame;
}

// ============================================================
// Connection: framed read/write over Deno.Conn
// ============================================================

export class FramedConnection {
  private buffer: Uint8Array<ArrayBuffer> = new Uint8Array(0);
  private closed = false;

  constructor(private conn: Deno.Conn) {}

  private async readPayload(): Promise<Uint8Array | null> {
    // Read until we have enough data for length prefix
    while (this.buffer.length < LENGTH_PREFIX_SIZE) {
      if (this.closed) return null;
      const chunk = await this.readChunk();
      if (chunk === null) {
        this.closed = true;
        return null;
      }
      this.buffer = concat(this.buffer, chunk);
    }

    // Parse length
    const view = new DataView(
      this.buffer.buffer,
      this.buffer.byteOffset,
      this.buffer.byteLength
    );
    const payloadLength = view.getUint32(0, false);

    if (payloadLength > MAX_MESSAGE_SIZE) {
      throw new Error(
        `Incoming message size ${payloadLength} exceeds maximum ${MAX_MESSAGE_SIZE}`
      );
    }

    // Read until we have the full payload
    const totalNeeded = LENGTH_PREFIX_SIZE + payloadLength;
    while (this.buffer.length < totalNeeded) {
      if (this.closed) return null;
      const chunk = await this.readChunk();
      if (chunk === null) {
        this.closed = true;
        return null;
      }
      this.buffer = concat(this.buffer, chunk);
    }

    // Extract payload
    const payload = this.buffer.slice(LENGTH_PREFIX_SIZE, totalNeeded);
    this.buffer = this.buffer.slice(totalNeeded);
    return payload;
  }

  async readDiscoveryMessage(): Promise<ParsedDiscoveryMessage | null> {
    const payload = await this.readPayload();
    if (payload === null) return null;
    return deserializeDiscovery(payload);
  }

  async readP2PMessage(): Promise<ParsedP2PMessage | null> {
    const payload = await this.readPayload();
    if (payload === null) return null;
    return deserializeP2P(payload);
  }

  async writeMessage(msg: Message): Promise<void> {
    const frame = encodeFrame(msg);
    let written = 0;
    while (written < frame.length) {
      const n = await this.conn.write(frame.subarray(written));
      written += n;
    }
  }

  private async readChunk(): Promise<Uint8Array | null> {
    const buf = new Uint8Array(65536);
    try {
      const n = await this.conn.read(buf);
      if (n === null) return null;
      return buf.subarray(0, n);
    } catch {
      return null;
    }
  }

  close(): void {
    this.closed = true;
    try {
      this.conn.close();
    } catch {
      // already closed
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get raw(): Deno.Conn {
    return this.conn;
  }
}

// ============================================================
// Utility
// ============================================================

function concat(a: Uint8Array, b: Uint8Array): Uint8Array<ArrayBuffer> {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}
