import type { Context } from "../../src/types.ts";

export default async function echo(ctx: Context): Promise<unknown> {
  const data = await ctx.receive();
  await ctx.send({ echoed: data });
  return "echo_done";
}
