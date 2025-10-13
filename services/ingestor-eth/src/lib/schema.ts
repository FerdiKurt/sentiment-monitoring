import { z } from "zod";

export const SwapMsg = z.object({
  ts: z.string(),
  chain: z.number(),
  pool: z.string(),
  protocol: z.literal("UNIV3"),
  base_token: z.object({
    address: z.string(),
    symbol: z.string(),
    decimals: z.number()
  }),
  quote_token: z.object({
    address: z.string(),
    symbol: z.string(),
    decimals: z.number()
  }),
  amount0: z.string(),
  amount1: z.string(),
  price: z.number(),     // quote per base
  usd: z.number().nullable()
});
export type SwapMsg = z.infer<typeof SwapMsg>;
