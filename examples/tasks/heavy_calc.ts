import type { Context } from "../../src/types.ts";

export default async function heavyCalc(
  ctx: Context,
  ...args: unknown[]
): Promise<unknown> {
  const x = args[0] as number;
  const y = args[1] as number;

  await ctx.send({ progress: 0.3 });
  await ctx.send({ progress: 0.7 });

  const sub = ctx.spawn("multiply", [x, 2]);
  const doubled = await sub.join();

  await ctx.send({ progress: 1.0 });

  return (doubled as number) + y;
}
