import { z } from "zod";

// ============================================================
// Helper
// ============================================================

export function assertNever(x: never): never {
  throw new Error(`Unexpected message type: ${(x as { type: string }).type}`);
}

// ============================================================
// Serialized error schema
// ============================================================

const SerializedErrorSchema = z.object({
  message: z.string(),
  name: z.string(),
  stack: z.string().optional(),
});

// ============================================================
// Discovery message schemas
// ============================================================

const RegisterSchema = z.object({
  type: z.literal("register"),
  processId: z.string(),
  host: z.string(),
  port: z.number().int(),
  funcs: z.array(z.string()),
  maxConcurrency: z.number().int(),
});

const RegisteredSchema = z.object({
  type: z.literal("registered"),
  processId: z.string(),
});

const CapacityChangeSchema = z.object({
  type: z.literal("capacity_change"),
  activeTasks: z.number().int(),
  maxConcurrency: z.number().int(),
});

const FindSchema = z.object({
  type: z.literal("find"),
  func: z.string(),
  exclude: z.array(z.string()).optional(),
});

const PeerInfoSchema = z.object({
  processId: z.string(),
  host: z.string(),
  port: z.number().int(),
});

const FoundSchema = z.object({
  type: z.literal("found"),
  peers: z.array(PeerInfoSchema),
});

const RequestSpawnSchema = z.object({
  type: z.literal("request_spawn"),
  reason: z.enum(["no_idle", "overflow"]),
});

const SpawnResultSchema = z.object({
  type: z.literal("spawn_result"),
  success: z.boolean(),
  processId: z.string().optional(),
  host: z.string().optional(),
  port: z.number().int().optional(),
});

const HeartbeatSchema = z.object({
  type: z.literal("heartbeat"),
});

const HeartbeatAckSchema = z.object({
  type: z.literal("heartbeat_ack"),
});

const ShutdownSchema = z.object({
  type: z.literal("shutdown"),
});

const ByeSchema = z.object({
  type: z.literal("bye"),
});

const DiscoveryMessageSchema = z.discriminatedUnion("type", [
  RegisterSchema,
  RegisteredSchema,
  CapacityChangeSchema,
  FindSchema,
  FoundSchema,
  RequestSpawnSchema,
  SpawnResultSchema,
  HeartbeatSchema,
  HeartbeatAckSchema,
  ShutdownSchema,
  ByeSchema,
]);

// ============================================================
// P2P message schemas
// ============================================================

const ExecSchema = z.object({
  type: z.literal("exec"),
  taskId: z.string(),
  func: z.string(),
  args: z.array(z.unknown()),
  execTimeout: z.number().int().optional(),
});

const AcceptSchema = z.object({
  type: z.literal("accept"),
  taskId: z.string(),
});

const RejectSchema = z.object({
  type: z.literal("reject"),
  taskId: z.string(),
  reason: z.enum(["at_capacity", "unknown_func"]),
});

const SendSchema = z.object({
  type: z.literal("send"),
  taskId: z.string(),
  value: z.unknown(),
});

const ResultSchema = z.object({
  type: z.literal("result"),
  taskId: z.string(),
  value: z.unknown(),
});

const ErrorSchema = z.object({
  type: z.literal("error"),
  taskId: z.string(),
  error: SerializedErrorSchema,
});

const CancelSchema = z.object({
  type: z.literal("cancel"),
  taskId: z.string(),
});

const ObjectFetchSchema = z.object({
  type: z.literal("object_fetch"),
  taskId: z.string(),
  objectId: z.string(),
});

const ObjectFetchResponseSchema = z.object({
  type: z.literal("object_fetch_response"),
  taskId: z.string(),
  objectId: z.string(),
  found: z.boolean(),
  data: z.unknown(),
});

const ActorCreateSchema = z.object({
  type: z.literal("actor_create"),
  taskId: z.string(),
  actorName: z.string(),
});

const ActorCreateResultSchema = z.object({
  type: z.literal("actor_create_result"),
  taskId: z.string(),
  actorId: z.string().optional(),
  error: SerializedErrorSchema.optional(),
});

const ActorCallSchema = z.object({
  type: z.literal("actor_call"),
  taskId: z.string(),
  actorId: z.string(),
  method: z.string(),
  args: z.array(z.unknown()),
});

const ActorResultSchema = z.object({
  type: z.literal("actor_result"),
  taskId: z.string(),
  value: z.unknown(),
});

const ActorErrorSchema = z.object({
  type: z.literal("actor_error"),
  taskId: z.string(),
  error: SerializedErrorSchema,
});

const ActorDestroySchema = z.object({
  type: z.literal("actor_destroy"),
  taskId: z.string(),
  actorId: z.string(),
});

const P2PMessageSchema = z.discriminatedUnion("type", [
  ExecSchema,
  AcceptSchema,
  RejectSchema,
  SendSchema,
  ResultSchema,
  ErrorSchema,
  CancelSchema,
  ObjectFetchSchema,
  ObjectFetchResponseSchema,
  ActorCreateSchema,
  ActorCreateResultSchema,
  ActorCallSchema,
  ActorResultSchema,
  ActorErrorSchema,
  ActorDestroySchema,
]);

// ============================================================
// Parse functions
// ============================================================

export type ParsedDiscoveryMessage = z.infer<typeof DiscoveryMessageSchema>;
export type ParsedP2PMessage = z.infer<typeof P2PMessageSchema>;

export function parseDiscoveryMessage(data: unknown): ParsedDiscoveryMessage {
  return DiscoveryMessageSchema.parse(data);
}

export function parseP2PMessage(data: unknown): ParsedP2PMessage {
  return P2PMessageSchema.parse(data);
}
