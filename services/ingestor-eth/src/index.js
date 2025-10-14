import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import {
  createPublicClient, webSocket, getAddress,
  parseAbi, decodeEventLog, formatUnits
} from 'viem';

const TOPIC_SWAPS = 'dex.swaps';
const chain = Number(process.env.CHAIN_ID || 1);
const brokers = (process.env.KAFKA_BROKERS || 'redpanda:9092').split(',');
const kafka = new Kafka({ clientId: 'ingestor-eth', brokers, logLevel: logLevel.INFO });
const producer = kafka.producer({ allowAutoTopicCreation: true });

const ERC20_ABI = parseAbi([
  'function decimals() view returns (uint8)',
  'function symbol() view returns (string)'
]);
const UNIV3_POOL_ABI = parseAbi([
  'function token0() view returns (address)',
  'function token1() view returns (address)',
  'function fee() view returns (uint24)'
]);
const UNIV3_SWAP_ABI = parseAbi([
  'event Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)'
]);

const client = createPublicClient({
  chain: {
    id: chain,
    name: 'eth',
    nativeCurrency: { name: 'ETH', symbol: 'ETH', decimals: 18 },
    rpcUrls: { default: { http: [], webSocket: [process.env.RPC_URL] } }
  },
  transport: webSocket(process.env.RPC_URL)
});

function norm(a){ return getAddress(a); }

async function readToken(addr) {
  const [dec, sym] = await Promise.all([
    client.readContract({ address: addr, abi: ERC20_ABI, functionName: 'decimals' }),
    client.readContract({ address: addr, abi: ERC20_ABI, functionName: 'symbol' })
  ]);
  return { address: norm(addr), symbol: sym, decimals: Number(dec) };
}

async function readPoolMeta(addr) {
  const [t0, t1, fee] = await Promise.all([
    client.readContract({ address: addr, abi: UNIV3_POOL_ABI, functionName: 'token0' }),
    client.readContract({ address: addr, abi: UNIV3_POOL_ABI, functionName: 'token1' }),
    client.readContract({ address: addr, abi: UNIV3_POOL_ABI, functionName: 'fee' })
  ]);
  const tok0 = await readToken(t0);
  const tok1 = await readToken(t1);
  return { pool: norm(addr), fee: Number(fee), t0: tok0, t1: tok1 };
}
