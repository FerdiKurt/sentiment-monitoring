import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import {
  createPublicClient, webSocket, getAddress, parseAbi,
  decodeEventLog
} from 'viem';

const chain = Number(process.env.CHAIN_ID || 1);
const brokers = (process.env.KAFKA_BROKERS || 'redpanda:9092').split(',');
const kafka = new Kafka({ clientId: 'ingestor-eth', brokers, logLevel: logLevel.WARN });
const producer = kafka.producer({ allowAutoTopicCreation: true });
const consumer = kafka.consumer({ groupId: 'pools-feed' });

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
    nativeCurrency: { name:'ETH', symbol:'ETH', decimals:18 },
    rpcUrls: { default: { http: [], webSocket: [process.env.RPC_URL] } }
  },
  transport: webSocket(process.env.RPC_URL)
});

const STABLES = new Set((process.env.STABLE_TOKEN_ADDRESSES||'').split(',').map(s=>s.trim().toLowerCase()).filter(Boolean));
const WETH = (process.env.WETH_ADDRESS||'').toLowerCase();
const TOPIC_SWAPS = 'dex.swaps';
const TOPIC_POOLS = 'dex.pools';

const watchers = new Map(); // pool => unwatch()
const meta = new Map();     // pool => {t0,t1,fee}
const priceBook = new Map(); // tokenAddrLower => {pxUsd, tsMs}
const TTL = Number(process.env.PRICE_TTL_SEC || 600) * 1000;

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

function priceFromSqrt(sqrtPriceX96, d0, d1) {
  // price token1/token0
  // p = (sqrtPriceX96^2 / 2^192) * 10^(d0 - d1)
  const SQ = Number(sqrtPriceX96) / (2 ** 96);
  const p = (SQ * SQ) * (10 ** (d0 - d1));
  return p;
}

function setUsd(addr, pxUsd) {
  const key = addr.toLowerCase();
  priceBook.set(key, { pxUsd, tsMs: Date.now() });
}
function getUsd(addr) {
  const key = addr.toLowerCase();
  const v = priceBook.get(key);
  if (!v) return null;
  if (Date.now() - v.tsMs > TTL) { priceBook.delete(key); return null; }
  return v.pxUsd;
}

    abi: UNIV3_SWAP_ABI,
    onLogs: async (logs) => {
      for (const log of logs) {
        try {
          const pm = metas.find(m => m.pool.toLowerCase() === log.address.toLowerCase());
          if (!pm) continue;

          const ev = decodeEventLog({ abi: UNIV3_SWAP_ABI, data: log.data, topics: log.topics }).args;
          const amount0 = ev.amount0;
          const amount1 = ev.amount1;

          const price = priceFromAmounts(amount0, amount1, pm.t0.decimals, pm.t1.decimals);
          if (price == null) continue;
          const usd = usdFromQuote(pm.t1.symbol, amount1, pm.t1.decimals);

          const ts = log.blockTimestamp ? new Date(Number(log.blockTimestamp)*1000) : new Date();

          const msg = {
            ts: ts.toISOString(),
            chain,
            pool: pm.pool,
            protocol: 'UNIV3',
            base_token: pm.t0,
            quote_token: pm.t1,
            amount0: formatUnits(amount0, pm.t0.decimals),
            amount1: formatUnits(amount1, pm.t1.decimals),
            price,
            usd
          };
          await producer.send({ topic: TOPIC_SWAPS, messages: [{ key: pm.pool, value: JSON.stringify(msg) }] });
        } catch (e) {
          console.error('log err', e?.message || e);
        }
      }
    }
  });

  console.log('ingestor-eth (JS) listening…');
})().catch(e => { console.error(e); process.exit(1); });
