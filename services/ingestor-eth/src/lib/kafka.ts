import { Kafka, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS || "redpanda:9092").split(",");
export const kafka = new Kafka({
  clientId: "ingestor-eth",
  brokers,
  logLevel: logLevel.INFO,
});
export const producer = kafka.producer({ allowAutoTopicCreation: true });
