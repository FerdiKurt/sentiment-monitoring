import { createPublicClient, webSocket, getAddress, parseAbi } from "viem";

export const ERC20_ABI = parseAbi([
  "function decimals() view returns (uint8)",
  "function symbol() view returns (string)",
]);
export const UNIV3_POOL_ABI = parseAbi([
  "function token0() view returns (address)",
  "function token1() view returns (address)",
  "function fee() view returns (uint24)"
]);
export const UNIV3_SWAP_ABI = parseAbi([
  "event Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)"
]);

export const client = createPublicClient({
  chain: {
    id: Number(process.env.CHAIN_ID || 1),
    name: "eth",
    nativeCurrency: { name: "ETH", symbol: "ETH", decimals: 18 },
    rpcUrls: { default: { http: [], webSocket: [process.env.RPC_URL!] } },
  },
  transport: webSocket(process.env.RPC_URL!),
});

export function norm(addr: string) { return getAddress(addr); }
