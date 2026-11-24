import {
  Kafka,
  Partitioners,
  logLevel,
  type Admin,
  type Consumer,
  type Producer,
} from "kafkajs";
import {
  assertNoForbiddenSecrets,
  type BrokerMessageHandler,
  type IControlPlaneBroker,
} from "./types";

export class KafkaKraftBroker implements IControlPlaneBroker {
  public readonly kind = "kafka" as const;

  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly admin: Admin;
  private readonly consumers: Consumer[] = [];
  private connected = false;

  constructor(brokers: string[], clientId: string) {
    this.kafka = new Kafka({
      clientId,
      brokers,
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
    await this.producer.send({
      topic,
      messages: [
        {
          key: key ?? null,
          value: JSON.stringify(message),
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
    });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    this.consumers.push(consumer);

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) {
          return;
        }
        const parsed = JSON.parse(message.value.toString("utf8")) as T;
        assertNoForbiddenSecrets(parsed);
        await handler(parsed, message.key?.toString("utf8"));
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
      await consumer.disconnect().catch(() => {});
    }
    this.consumers.length = 0;

    if (this.connected) {
      await this.producer.disconnect().catch(() => {});
      await this.admin.disconnect().catch(() => {});
      this.connected = false;
    }
  }
}
