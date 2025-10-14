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

