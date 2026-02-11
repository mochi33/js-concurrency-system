import { Registry } from "../src/registry.ts";
import type { Context } from "../src/types.ts";

const registry = new Registry();

registry.register(
  "heavyCalc", heavyCalc
);

async function heavyCalc (ctx: Context, ...args: unknown[]): Promise<unknown> {
    const x = args[0] as number;
    const y = args[1] as number;

    // Send progress updates
    await ctx.send({ progress: 0.3 });
    await ctx.send({ progress: 0.7 });

    // Nested spawn: multiply x by 2
    const sub = ctx.spawn("multiply", [x, 2]);
    const doubled = await sub.join();

    await ctx.send({ progress: 1.0 });

    return (doubled as number) + y;
}

registry.register(
  "multiply",
  async (_ctx: Context, ...args: unknown[]): Promise<unknown> => {
    const a = args[0] as number;
    const b = args[1] as number;
    return a * b;
  },
);

registry.register(
  "echo",
  async (ctx: Context): Promise<unknown> => {
    // Receive a message from caller, echo it back with a prefix
    const data = await ctx.receive();
    await ctx.send({ echoed: data });
    return "echo_done";
  },
);

export default registry;
