/**
 * Zero-Trust control-plane broker contract and failover trigger DTO for
 * `telemetry-watcher`. Mirrors the `sfu-node` wire schema strictly.
 */

export type BrokerMessageHandler<T> = (payload: T, key?: string) => Promise<void> | void;

export interface IControlPlaneBroker {
  readonly kind: "kafka" | "redis" | "noop";
  connect(): Promise<void>;
  publish<T extends object>(topic: string, message: T, key?: string): Promise<void>;
  subscribe<T>(
    topic: string,
    groupId: string,
    handler: BrokerMessageHandler<T>,
  ): Promise<void>;
  healthCheck(): Promise<boolean>;
  disconnect(): Promise<void>;
}

export type FailoverReason =
  | "DEATH_SIGNATURE_CONFIRMED"
  | "MANUAL_EVACUATION";

export interface FailoverTriggerEvent {
  schemaVersion: "1.0";
  eventType: "PROACTIVE_EVACUATION";
  correlationId: string;
  sourceNodeId: string;
  targetNodeId: string;
  targetSignalingUrl: string;
  reason: FailoverReason;
  triggeredAtMs: number;
  metricsSnapshot: {
    jitterMs: number;
    pliRatePerSec: number;
    nackRatePerSec: number;
    workerCpuRatio: number;
    udpDropRatePerSec: number;
    validDtlsSessionRatio: number;
  };
}

const FORBIDDEN_KEY_PATTERN =
  /sframe|secret|privatekey|keymaterial|ratchet|salt|aes|plaintext|decryption/i;

export function assertNoForbiddenSecrets(payload: unknown): void {
  const seen = new WeakSet<object>();

  const scan = (value: unknown, path: string): void => {
    if (value === null || value === undefined || typeof value !== "object") {
      return;
    }
    if (seen.has(value)) {
      return;
    }
    seen.add(value);

    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i += 1) {
        scan(value[i], `${path}[${i}]`);
      }
      return;
    }

    const record = value as Record<string, unknown>;
    for (const [key, nested] of Object.entries(record)) {
      const fieldPath = path ? `${path}.${key}` : key;
      if (FORBIDDEN_KEY_PATTERN.test(key)) {
        throw new Error(
          `Zero-Trust violation: forbidden key "${fieldPath}" cannot leave telemetry-watcher`,
        );
      }
      scan(nested, fieldPath);
    }
  };

  scan(payload, "");
}
