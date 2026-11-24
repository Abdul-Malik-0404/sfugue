/**
 * Factory creating the configured `IControlPlaneBroker` adapter.
 *
 * Swapping between Apache Kafka (KRaft) and Redis requires only changing
 * `CONTROL_PLANE_BROKER=kafka|redis|noop`.
 */

import type { SfuConfig } from "../config";
import { KafkaKraftBroker } from "./kafkaBroker";
import { NoopControlPlaneBroker, RedisBroker } from "./redisFallbackBroker";
import type { IControlPlaneBroker } from "./types";

export function createControlPlaneBroker(config: SfuConfig): IControlPlaneBroker {
  switch (config.controlPlaneBroker) {
    case "kafka":
      return new KafkaKraftBroker({
        brokers: config.kafkaBrokers,
        clientId: config.kafkaClientId,
      });
    case "redis":
      return new RedisBroker(config.redisUrl);
    case "noop":
      return new NoopControlPlaneBroker();
  }
}
