import type { Context } from "../../src/types.ts";

export default async function multiply(
  _ctx: Context,
  ...args: unknown[]
): Promise<unknown> {
  const a = args[0] as number;
  const b = args[1] as number;
  return a * b;
}
