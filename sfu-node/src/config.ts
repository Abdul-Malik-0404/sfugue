/**
 * Typed runtime configuration for `sfu-node`.
 *
 * All network bindings, Mediasoup RTC port ranges (constrained to match
 * KinD `extraPortMappings` and Dockerfile `EXPOSE 40000-40010/udp`), and
 * modular control-plane broker settings are externalized to environment
 * variables per CLAUDE.md.
 */

export type SfuRole = "primary" | "standby";
export type BrokerKind = "kafka" | "redis" | "noop";

export interface SfuConfig {
  port: number;
  nodeId: string;
  role: SfuRole;
  announcedIp: string;
  rtcMinPort: number;
  rtcMaxPort: number;
  maxIncomingBitrate: number;
  initialOutgoingBitrate: number;
  controlPlaneBroker: BrokerKind;
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  kafkaTopicSessionState: string;
  kafkaTopicFailoverTrigger: string;
  standbySignalingUrl: string;
  metricsPollIntervalMs: number;
  prewarmTtlMs: number;
  redisUrl: string;
}

function parseInteger(name: string, fallback: number): number {
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

function parseSfuRole(raw: string | undefined): SfuRole {
  if (raw === undefined || raw.trim() === "") {
    return "primary";
  }
  const normalized = raw.trim().toLowerCase();
  if (normalized === "primary" || normalized === "standby") {
    return normalized;
  }
  throw new Error(`Invalid SFU_ROLE="${raw}". Expected "primary" or "standby".`);
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

export function loadConfig(): SfuConfig {
  const role = parseSfuRole(process.env.SFU_ROLE);
  const nodeId = process.env.NODE_ID?.trim() || (role === "standby" ? "sfu-standby" : "sfu-primary");
  const rtcMinPort = parseInteger("RTC_MIN_PORT", 40000);
  const rtcMaxPort = parseInteger("RTC_MAX_PORT", 40010);

  if (rtcMinPort > rtcMaxPort) {
    throw new Error(`RTC_MIN_PORT (${rtcMinPort}) cannot exceed RTC_MAX_PORT (${rtcMaxPort})`);
  }

  const rawBrokers = process.env.KAFKA_BROKERS?.trim() || "localhost:9092";
  const kafkaBrokers = rawBrokers
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);

  return {
    port: parseInteger("PORT", 3000),
    nodeId,
    role,
    announcedIp: process.env.ANNOUNCED_IP?.trim() || "127.0.0.1",
    rtcMinPort,
    rtcMaxPort,
    maxIncomingBitrate: parseInteger("MAX_INCOMING_BITRATE", 1_500_000),
    initialOutgoingBitrate: parseInteger("INITIAL_OUTGOING_BITRATE", 1_000_000),
    controlPlaneBroker: parseBrokerKind(process.env.CONTROL_PLANE_BROKER),
    kafkaBrokers: kafkaBrokers.length > 0 ? kafkaBrokers : ["localhost:9092"],
    kafkaClientId: process.env.KAFKA_CLIENT_ID?.trim() || nodeId,
    kafkaGroupId: process.env.KAFKA_GROUP_ID?.trim() || `${nodeId}-group`,
    kafkaTopicSessionState:
      process.env.KAFKA_TOPIC_SESSION_STATE?.trim() || "sfugue.session.state",
    kafkaTopicFailoverTrigger:
      process.env.KAFKA_TOPIC_FAILOVER_TRIGGER?.trim() || "sfugue.failover.trigger",
    standbySignalingUrl:
      process.env.STANDBY_SIGNALING_URL?.trim() || "http://127.0.0.1:30001",
    metricsPollIntervalMs: parseInteger("METRICS_POLL_INTERVAL_MS", 2000),
    prewarmTtlMs: parseInteger("PREWARM_TTL_MS", 120_000),
    redisUrl: process.env.REDIS_URL?.trim() || "redis://127.0.0.1:6379",
  };
}
