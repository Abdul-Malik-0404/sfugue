import type { WatcherConfig } from "../config";
import { KafkaKraftBroker } from "./kafkaBroker";
import { NoopControlPlaneBroker, RedisBroker } from "./redisFallbackBroker";
import type { IControlPlaneBroker } from "./types";

export function createControlPlaneBroker(
  config: WatcherConfig,
): IControlPlaneBroker {
  switch (config.controlPlaneBroker) {
    case "kafka":
      return new KafkaKraftBroker(config.kafkaBrokers, config.kafkaClientId);
    case "redis":
      return new RedisBroker(config.redisUrl);
    case "noop":
      return new NoopControlPlaneBroker();
  }
}
