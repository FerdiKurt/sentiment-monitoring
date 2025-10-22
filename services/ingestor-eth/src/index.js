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

async function addPoolWatcher(pool) {
  if (watchers.has(pool)) return;
  let m = meta.get(pool);
  if (!m) { m = await readPoolMeta(pool); meta.set(pool, m); }
  const unwatch = client.watchEvent({
    address: pool,
    abi: UNIV3_SWAP_ABI,
    onLogs: async (logs) => {
      for (const log of logs) {
        try {
          const ev = decodeEventLog({ abi: UNIV3_SWAP_ABI, data: log.data, topics: log.topics }).args;
          const amount0 = ev.amount0; // bigint
          const amount1 = ev.amount1;
          const sqrt = ev.sqrtPriceX96;

          const p_t1_per_t0 = priceFromSqrt(Number(sqrt), m.t0.decimals, m.t1.decimals);

          // USD hints
          let t0_usd = null, t1_usd = null;

          // If this is a stable pair, we can compute USD for the other side directly
          const t0_is_stable = STABLES.has(m.t0.address.toLowerCase());
          const t1_is_stable = STABLES.has(m.t1.address.toLowerCase());

          if (t0_is_stable && !t1_is_stable) {
            // token0 is USD, price = token1 per USD => USD per token1 = 1 / price
            if (p_t1_per_t0 > 0) { t1_usd = 1 / p_t1_per_t0; setUsd(m.t1.address, t1_usd); }
          } else if (!t0_is_stable && t1_is_stable) {
            // token1 is USD, price = USD per token0
            t0_usd = p_t1_per_t0; setUsd(m.t0.address, t0_usd);
          } else {
            // If one side is WETH and we know WETH/USD, propagate
            const wethUsd = getUsd(WETH);
            if (wethUsd) {
              if (m.t0.address.toLowerCase() === WETH) {
                // price = token1 per WETH => USD per token1 = wethUsd / price
                if (p_t1_per_t0 > 0) { t1_usd = wethUsd / p_t1_per_t0; setUsd(m.t1.address, t1_usd); }
                t0_usd = wethUsd;
              } else if (m.t1.address.toLowerCase() === WETH) {
                // price = WETH per token0 => USD per token0 = wethUsd * price
                t0_usd = wethUsd * p_t1_per_t0; setUsd(m.t0.address, t0_usd);
                t1_usd = wethUsd;
              }
            }
          }

          // Notional in USD if we know one side
          let usd_notional = null;
          const a0 = Math.abs(Number(amount0)) / (10 ** m.t0.decimals);
          const a1 = Math.abs(Number(amount1)) / (10 ** m.t1.decimals);
          if (t0_usd != null) usd_notional = (usd_notional == null) ? a0 * t0_usd : Math.min(usd_notional, a0 * t0_usd);
          if (t1_usd != null) usd_notional = (usd_notional == null) ? a1 * t1_usd : Math.min(usd_notional, a1 * t1_usd);

          const msg = {
            ts: (log.blockTimestamp ? new Date(Number(log.blockTimestamp)*1000) : new Date()).toISOString(),
            chain,
            pool: m.pool,
            protocol: 'UNIV3',
            token0: m.t0.address, token1: m.t1.address,
            symbol0: m.t0.symbol, symbol1: m.t1.symbol,
            decimals0: m.t0.decimals, decimals1: m.t1.decimals,
            sqrtPriceX96: String(sqrt),
            price: p_t1_per_t0,
            usd_t0: t0_usd, usd_t1: t1_usd,
            amount0: a0, amount1: a1,
            usd: usd_notional
          };

          await producer.send({ topic: TOPIC_SWAPS, messages: [{ key: m.pool, value: JSON.stringify(msg) }] });
        } catch (e) { console.error('swap err', e?.message || e); }
      }
    }
  });
  watchers.set(pool, unwatch);
  console.log(`watching pool ${pool} (${m.t0.symbol}/${m.t1.symbol}) fee=${m.fee}`);
}

(async () => {
  await producer.connect();
  await consumer.connect();

  // Seed from env (optional)
  const envPools = (process.env.UNIV3_POOLS || '').split(',').map(s=>s.trim()).filter(Boolean).map(s => s.split('@')[0]);
  for (const p of envPools) { await addPoolWatcher(p); }

  // Dynamic via pool-indexer
  await consumer.subscribe({ topic: TOPIC_POOLS, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const v = JSON.parse(message.value.toString());
        if (v && v.pool) await addPoolWatcher(v.pool);
      } catch (e) { /* ignore */ }
    }
  });

  console.log('ingestor-eth (W2) listening…');
})();
