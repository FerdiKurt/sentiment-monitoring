import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import {
  createPublicClient, webSocket, getAddress,
  parseAbi, decodeEventLog, formatUnits
} from 'viem';

