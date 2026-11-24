/**
 * Apache Kafka (KRaft mode) implementation of `IControlPlaneBroker`.
 *
 * Uses `kafkajs` with ordered keyed partitions (`key = roomId` or `nodeId`)
 * and strict Zero-Trust secret assertion before serialization.
 */

import {
  Kafka,
  Partitioners,
  logLevel,
  type Admin,
  type Consumer,
  type Producer,
} from "kafkajs";
import { assertNoForbiddenSecrets } from "./sanitizer";
import type { BrokerMessageHandler, IControlPlaneBroker } from "./types";

export interface KafkaBrokerOptions {
  brokers: string[];
  clientId: string;
}

export class KafkaKraftBroker implements IControlPlaneBroker {
  public readonly kind = "kafka" as const;

  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly admin: Admin;
  private readonly consumers: Consumer[] = [];
  private connected = false;

  constructor(options: KafkaBrokerOptions) {
    this.kafka = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers,
      logLevel: logLevel.WARN,
      connectionTimeout: 3000,
      requestTimeout: 5000,
      retry: {
        initialRetryTime: 200,
        retries: 3,
      },
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
      allowAutoTopicCreation: true,
    });
    this.admin = this.kafka.admin();
  }

  public async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    await this.producer.connect();
    await this.admin.connect();
    this.connected = true;
  }

  public async publish<T extends object>(
    topic: string,
    message: T,
    key?: string,
  ): Promise<void> {
    assertNoForbiddenSecrets(message);
    if (!this.connected) {
      throw new Error("KafkaKraftBroker is not connected");
    }

    const serialized = JSON.stringify(message);
    await this.producer.send({
      topic,
      messages: [
        {
          key: key ?? null,
          value: serialized,
        },
      ],
    });
  }

  public async subscribe<T>(
    topic: string,
    groupId: string,
    handler: BrokerMessageHandler<T>,
  ): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId,
      allowAutoTopicCreation: true,
      sessionTimeout: 10_000,
      heartbeatInterval: 3000,
    });

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    this.consumers.push(consumer);

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) {
          return;
        }
        try {
          const parsed = JSON.parse(message.value.toString("utf8")) as T;
          assertNoForbiddenSecrets(parsed);
          await handler(parsed, message.key?.toString("utf8"));
        } catch (error) {
          console.error(
            `[KafkaKraftBroker] Failed to process message on topic "${topic}":`,
            error,
          );
        }
      },
    });
  }

  public async healthCheck(): Promise<boolean> {
    if (!this.connected) {
      return false;
    }
    try {
      const cluster = await this.admin.describeCluster();
      return cluster.brokers.length > 0;
    } catch {
      return false;
    }
  }

  public async disconnect(): Promise<void> {
    for (const consumer of this.consumers) {
      await consumer.disconnect().catch((err: unknown) => {
        console.warn("[KafkaKraftBroker] Consumer disconnect warning:", err);
      });
    }
    this.consumers.length = 0;

    if (this.connected) {
      await this.producer.disconnect().catch((err: unknown) => {
        console.warn("[KafkaKraftBroker] Producer disconnect warning:", err);
      });
      await this.admin.disconnect().catch((err: unknown) => {
        console.warn("[KafkaKraftBroker] Admin disconnect warning:", err);
      });
      this.connected = false;
    }
  }
}
