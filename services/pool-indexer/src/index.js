import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import { createPublicClient, webSocket, parseAbi } from 'viem';

const FACTORY = (process.env.UNIV3_FACTORY || '').trim();
if (!FACTORY) {
  console.error('UNIV3_FACTORY missing');
  process.exit(1);
}

const kafka = new Kafka({
  clientId: 'pool-indexer',
  brokers: (process.env.KAFKA_BROKERS || 'redpanda:9092').split(','),
  logLevel: logLevel.WARN,
});
const producer = kafka.producer({ allowAutoTopicCreation: true });
const TOPIC = 'dex.pools';

const abi = parseAbi([
  // Only the event we care about
  'event PoolCreated(address indexed token0,address indexed token1,uint24 fee,int24 tickSpacing,address pool)',
]);

const client = createPublicClient({
  chain: {
    id: Number(process.env.CHAIN_ID || 1),
    name: 'eth',
    nativeCurrency: { name: 'ETH', symbol: 'ETH', decimals: 18 },
    rpcUrls: { default: { http: [], webSocket: [process.env.RPC_URL] } },
  },
  transport: webSocket(process.env.RPC_URL),
});

(async () => {
  await producer.connect();

  // Filter strictly to PoolCreated + guard against weird logs/reorgs
  client.watchEvent({
    address: FACTORY,
    abi,
    eventName: 'PoolCreated',
    onLogs: async (logs) => {
      for (const l of logs) {
        try {
          if (l.removed) continue;            // skip reorged-out logs
          if (!l.args) continue;              // extra safety
          const { token0, token1, fee, tickSpacing, pool } = l.args;
          if (!pool) continue;

          const msg = {
            ts: (l.blockTimestamp ? new Date(Number(l.blockTimestamp) * 1000) : new Date()).toISOString(),
            chain: Number(process.env.CHAIN_ID || 1),
            pool,
            token0,
            token1,
            fee: Number(fee),
            tickSpacing: Number(tickSpacing),
          };
          await producer.send({ topic: TOPIC, messages: [{ key: pool, value: JSON.stringify(msg) }] });
        } catch (e) {
          console.error('PoolCreated handler error:', e?.message || e);
        }
      }
    },
  });

  console.log('pool-indexer watching PoolCreated…');
})();
