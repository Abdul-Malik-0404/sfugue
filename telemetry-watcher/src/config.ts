/**
 * Typed configuration for `telemetry-watcher`.
 *
 * Externalizes Prometheus endpoint, death-signature thresholds,
 * ingress-discrimination DTLS ratio guards, cooldown timer, and modular
 * control-plane broker settings.
 */

export type BrokerKind = "kafka" | "redis" | "noop";

export interface WatcherConfig {
  prometheusUrl: string;
  targetNodeId: string;
  standbyNodeId: string;
  standbySignalingUrl: string;
  pollIntervalMs: number;
  failoverCooldownMs: number;
  consecutiveHitsRequired: number;
  jitterThresholdMs: number;
  pliRateThresholdPerSec: number;
  nackRateThresholdPerSec: number;
  cpuRatioThreshold: number;
  udpDropRateThresholdPerSec: number;
  minValidDtlsRatio: number;
  minActiveTransportsForOrganic: number;
  controlPlaneBroker: BrokerKind;
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaTopicFailoverTrigger: string;
  redisUrl: string;
}

function parseNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid numeric environment variable ${name}="${raw}"`);
  }
  return parsed;
}

function parseIntStrict(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid integer environment variable ${name}="${raw}"`);
  }
  return parsed;
}

function parseBrokerKind(raw: string | undefined): BrokerKind {
  if (raw === undefined || raw.trim() === "") {
    return "kafka";
  }
  const normalized = raw.trim().toLowerCase();
  if (normalized === "kafka" || normalized === "redis" || normalized === "noop") {
    return normalized;
  }
  throw new Error(
    `Invalid CONTROL_PLANE_BROKER="${raw}". Expected "kafka", "redis", or "noop".`,
  );
}

export function loadConfig(): WatcherConfig {
  const rawBrokers = process.env.KAFKA_BROKERS?.trim() || "127.0.0.1:30094";
  const kafkaBrokers = rawBrokers
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  return {
    prometheusUrl: (process.env.PROMETHEUS_URL?.trim() || "http://127.0.0.1:30090").replace(
      /\/+$/,
      "",
    ),
    targetNodeId: process.env.TARGET_NODE_ID?.trim() || "sfu-primary",
    standbyNodeId: process.env.STANDBY_NODE_ID?.trim() || "sfu-standby",
    standbySignalingUrl:
      process.env.STANDBY_SIGNALING_URL?.trim() || "http://127.0.0.1:30001",
    pollIntervalMs: parseIntStrict("POLL_INTERVAL_MS", 5000),
    failoverCooldownMs: parseIntStrict("FAILOVER_COOLDOWN_MS", 30_000),
    consecutiveHitsRequired: Math.max(
      1,
      parseIntStrict("CONSECUTIVE_HITS_REQUIRED", 2),
    ),
    jitterThresholdMs: parseNumber("THRESHOLD_JITTER_MS", 40),
    pliRateThresholdPerSec: parseNumber("THRESHOLD_PLI_RATE", 5),
    nackRateThresholdPerSec: parseNumber("THRESHOLD_NACK_RATE", 20),
    cpuRatioThreshold: parseNumber("THRESHOLD_CPU_RATIO", 0.85),
    udpDropRateThresholdPerSec: parseNumber("THRESHOLD_UDP_DROP_RATE", 10),
    minValidDtlsRatio: parseNumber("MIN_VALID_DTLS_RATIO", 0.7),
    minActiveTransportsForOrganic: parseIntStrict("MIN_ORGANIC_TRANSPORTS", 1),
    controlPlaneBroker: parseBrokerKind(process.env.CONTROL_PLANE_BROKER),
    kafkaBrokers: kafkaBrokers.length > 0 ? kafkaBrokers : ["127.0.0.1:30094"],
    kafkaClientId: process.env.KAFKA_CLIENT_ID?.trim() || "telemetry-watcher",
    kafkaTopicFailoverTrigger:
      process.env.KAFKA_TOPIC_FAILOVER_TRIGGER?.trim() || "sfugue.failover.trigger",
    redisUrl: process.env.REDIS_URL?.trim() || "redis://127.0.0.1:6379",
  };
}
