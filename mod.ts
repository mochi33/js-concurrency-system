export { connect, ProcessNode } from "./src/node.ts";
export { Registry } from "./src/registry.ts";
export { Discovery } from "./src/discovery.ts";
export { createChannel } from "./src/channel.ts";
export { createContext } from "./src/context.ts";
export type {
  Channel,
  Context,
  NodeConfig,
  DiscoveryConfig,
  SpawnOptions,
  ReceiveResult,
  TaskFunction,
} from "./src/types.ts";
export {
  ChannelClosedError,
  CancelledError,
  SpawnTimeoutError,
  ExecTimeoutError,
  SpawnError,
} from "./src/types.ts";
