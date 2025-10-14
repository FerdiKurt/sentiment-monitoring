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

