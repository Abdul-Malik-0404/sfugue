/**
 * Zero-Trust outbound payload sanitizer and allow-list DTO builder.
 *
 * Enforces the hard security boundary defined in CLAUDE.md:
 * - Recursively rejects any field whose name suggests SFrame cryptographic keys,
 *   ratchet secrets, salts, private keys, or plaintext media.
 * - Reconstructs outbound `SessionStateEvent` objects field-by-field from an
 *   explicit allow-list so extra properties cannot leak onto Kafka or Redis.
 */

import type {
  SanitizedRtpMetadata,
  SanitizedTransportDescriptor,
  SessionStateEvent,
} from "./types";

const FORBIDDEN_KEY_PATTERN =
  /sframe|secret|privatekey|keymaterial|ratchet|salt|aes|plaintext|decryption/i;

export class ZeroTrustViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ZeroTrustViolationError";
  }
}

function scanObjectForSecrets(value: unknown, path: string, seen: WeakSet<object>): void {
  if (value === null || value === undefined || typeof value !== "object") {
    return;
  }

  if (seen.has(value)) {
    return;
  }
  seen.add(value);

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i += 1) {
      scanObjectForSecrets(value[i], `${path}[${i}]`, seen);
    }
    return;
  }

  const record = value as Record<string, unknown>;
  for (const [key, nested] of Object.entries(record)) {
    const fieldPath = path ? `${path}.${key}` : key;
    if (FORBIDDEN_KEY_PATTERN.test(key)) {
      throw new ZeroTrustViolationError(
        `Zero-Trust control-plane violation: forbidden key "${fieldPath}" cannot be transmitted on broker`,
      );
    }
    scanObjectForSecrets(nested, fieldPath, seen);
  }
}

export function assertNoForbiddenSecrets(payload: unknown): void {
  scanObjectForSecrets(payload, "", new WeakSet<object>());
}

function copyTransportDescriptor(
  input: SanitizedTransportDescriptor,
): SanitizedTransportDescriptor {
  return {
    transportId: String(input.transportId),
    direction: input.direction === "recv" ? "recv" : "send",
    iceParameters: {
      usernameFragment: String(input.iceParameters.usernameFragment),
      password: String(input.iceParameters.password),
      ...(input.iceParameters.iceLite !== undefined
        ? { iceLite: Boolean(input.iceParameters.iceLite) }
        : {}),
    },
    iceCandidates: input.iceCandidates.map((candidate) => ({
      foundation: String(candidate.foundation),
      priority: Number(candidate.priority),
      ip: String(candidate.ip),
      address: String(candidate.address),
      protocol: candidate.protocol === "tcp" ? "tcp" : "udp",
      port: Number(candidate.port),
      type: "host",
      ...(candidate.tcpType ? { tcpType: candidate.tcpType } : {}),
    })),
    dtlsParameters: {
      ...(input.dtlsParameters.role ? { role: input.dtlsParameters.role } : {}),
      fingerprints: input.dtlsParameters.fingerprints.map((fp) => ({
        algorithm: String(fp.algorithm),
        value: String(fp.value),
      })),
    },
  };
}

function copyRtpMetadata(input: SanitizedRtpMetadata): SanitizedRtpMetadata {
  return {
    producerId: String(input.producerId),
    kind: input.kind === "audio" ? "audio" : "video",
    ...(input.mid !== undefined ? { mid: String(input.mid) } : {}),
    ssrcs: input.ssrcs.map((ssrc) => Number(ssrc)),
    rtxSsrcs: input.rtxSsrcs.map((ssrc) => Number(ssrc)),
    mimeTypes: input.mimeTypes.map((mime) => String(mime)),
    payloadTypes: input.payloadTypes.map((pt) => Number(pt)),
  };
}

export function sanitizeSessionStateEvent(input: SessionStateEvent): SessionStateEvent {
  assertNoForbiddenSecrets(input);

  const sanitized: SessionStateEvent = {
    schemaVersion: "1.0",
    eventType: input.eventType,
    sourceNodeId: String(input.sourceNodeId),
    roomId: String(input.roomId),
    peerId: String(input.peerId),
    correlationId: String(input.correlationId),
    timestampMs: Number(input.timestampMs),
  };

  if (input.transport) {
    sanitized.transport = copyTransportDescriptor(input.transport);
  }
  if (input.rtp) {
    sanitized.rtp = copyRtpMetadata(input.rtp);
  }

  return sanitized;
}
