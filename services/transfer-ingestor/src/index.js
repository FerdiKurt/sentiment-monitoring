import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import {
  createPublicClient, http, parseAbi, getAddress, decodeEventLog
} from 'viem';

const TOKENS = (process.env.TRACK_TOKEN_ADDRESSES || '')
  .split(',').map(s => s.trim()).filter(Boolean)
  .map(a => { try { return getAddress(a); } catch { return a; } });

const brokers = (process.env.KAFKA_BROKERS || 'redpanda:9092').split(',');
const kafka = new Kafka({ clientId: 'transfer-ingestor', brokers, logLevel: logLevel.WARN });
const producer = kafka.producer({
  allowAutoTopicCreation: false, // topic should exist: erc20.transfers
  retry: { retries: 5 },
});
const TOPIC = 'erc20.transfers';

const HTTP_URL =
  process.env.RPC_URL_HTTP ||
  (process.env.RPC_URL ? process.env.RPC_URL
    .replace(/^wss:\/\//, 'https://')
    .replace('/ws/v3/', '/v3/')
    .replace(/\/ws$/, '') : '');

const chain = {
  id: Number(process.env.CHAIN_ID || 1),
  name: 'eth',
  nativeCurrency: { name: 'ETH', symbol: 'ETH', decimals: 18 },
  rpcUrls: { default: { http: HTTP_URL ? [HTTP_URL] : [] } },
};

const client = createPublicClient({ chain, transport: http(HTTP_URL || '') });

const abi = parseAbi([
  'event Transfer(address indexed from, address indexed to, uint256 value)',
  'function symbol() view returns (string)',
  'function decimals() view returns (uint8)',
]);

const FALLBACKS = {
  '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': { symbol: 'WETH', decimals: 18 },
  '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48': { symbol: 'USDC', decimals: 6 },
  '0xdAC17F958D2ee523a2206206994597C13D831ec7': { symbol: 'USDT', decimals: 6 },
  '0x6B175474E89094C44Da98b954EedeAC495271d0F': { symbol: 'DAI', decimals: 18 },
};

const POLL_MS = Number(process.env.TRANSFER_POLL_MS || 6000);

async function readMeta(addr) {
  try {
    const [sym, dec] = await Promise.all([
      client.readContract({ address: addr, abi, functionName: 'symbol' }),
      client.readContract({ address: addr, abi, functionName: 'decimals' }),
    ]);
    return { symbol: String(sym), decimals: Number(dec) };
  } catch (e) {
    console.warn('readMeta fallback for', addr, e?.shortMessage || e?.message || e);
    return FALLBACKS[addr] || { symbol: addr.slice(0, 6), decimals: 18 };
  }
}

function toBigIntSafe(v) {
  if (typeof v === 'bigint') return v;
  if (typeof v === 'number') return BigInt(v);
  // strings: decimal or hex (0x…)
  return BigInt(v);
}

function mkMessages(logs, addr, meta) {
  const messages = [];
  let removed = 0, undecoded = 0, errors = 0;

  for (const l of logs) {
    try {
      if (l.removed) { removed++; continue; }

      let args = l.args;
      if (!args) {
        // Fallback decode for providers that don’t populate args over HTTP polling
        try {
          const decoded = decodeEventLog({
            abi,
            data: l.data,
            topics: l.topics,
            strict: false,
          });
          args = decoded.args;
        } catch {
          undecoded++;
          continue;
        }
      }

      const { from, to, value } = args;
      if (!from || !to || value === undefined || value === null) {
        undecoded++;
        continue;
      }

      const v = toBigIntSafe(value);
      const dec = BigInt(10) ** BigInt(meta.decimals);
      const amt = Number((v * 1000000n) / dec) / 1_000_000; // 6dp, avoids FP overflow

      const ts = l.blockTimestamp ? new Date(Number(l.blockTimestamp) * 1000) : new Date();

      messages.push({
        key: addr,
        value: JSON.stringify({
          ts: ts.toISOString(),
          chain: Number(process.env.CHAIN_ID || 1),
          token: addr,
          symbol: meta.symbol,
          decimals: meta.decimals,
          from, to,
          amount: amt,
          usd: null,
        }),
      });
    } catch (err) {
      errors++;
      console.error('transfer handler err:', err?.message || err);
    }
  }

  return { messages, removed, undecoded, errors };
}

async function sendBatched(topic, messages, chunkSize = 50) {
  let sent = 0;
  for (let i = 0; i < messages.length; i += chunkSize) {
    const slice = messages.slice(i, i + chunkSize);
    try {
      await producer.send({ topic, messages: slice, acks: -1 });
      sent += slice.length;
    } catch (e) {
      console.error('KAFKA SEND ERROR:', e?.message || e);
    }
  }
  return sent;
}

async function watchSingleToken(addr) {
  const meta = await readMeta(addr);
  console.log(`transfer-ingestor watching ${meta.symbol} ${addr}`);

  let fromBlock = (await client.getBlockNumber()) - 1n;

  client.watchEvent({
    address: addr,
    abi,
    eventName: 'Transfer',
    strict: true,           // only Transfer logs pass through
    poll: true,             // HTTP polling, no WS/rate limits
    pollingInterval: POLL_MS,
    fromBlock,
    onLogs: async (logs) => {
      const n = logs.length;
      console.log(`transfer-ingestor received ${n} ${meta.symbol} Transfer logs`);
      if (!n) return;

      const { messages, removed, undecoded, errors } = mkMessages(logs, addr, meta);

      if (!messages.length) {
        console.log(`transfer-ingestor built 0 messages for ${meta.symbol} (removed=${removed}, undecoded=${undecoded}, errors=${errors})`);
        return;
      }

      const sent = await sendBatched(TOPIC, messages, 50);
      console.log(`transfer-ingestor SENT ${sent}/${messages.length} messages to ${TOPIC} (${meta.symbol}); removed=${removed}, undecoded=${undecoded}, errors=${errors}`);
    },
    onError: (err) => {
      console.error(`watchEvent (poll) error for ${meta.symbol}:`, err?.message || err);
    },
  });
}

(async () => {
  if (!HTTP_URL) {
    console.error('RPC_URL_HTTP missing (or could not derive from RPC_URL). Set RPC_URL_HTTP in .env');
    process.exit(1);
  }
  try { await producer.connect(); }
  catch (e) {
    console.error('Kafka producer connect failed:', e?.message || e);
    process.exit(1);
  }

  if (!TOKENS.length) {
    console.log('transfer-ingestor idle (no tokens configured)');
    return;
  }

  for (const t of TOKENS) {
    try { await watchSingleToken(t); }
    catch (e) { console.error('failed to start watcher for', t, e?.message || e); }
  }
})();
