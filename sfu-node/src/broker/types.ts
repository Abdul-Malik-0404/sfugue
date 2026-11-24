/**
 * Modular control-plane broker contract and allow-listed Zero-Trust DTOs.
 *
 * Architectural rationale (CLAUDE.md):
 * - Domain logic depends solely on `IControlPlaneBroker` so Apache Kafka (KRaft)
 *   can be swapped for Redis Pub/Sub or Streams via `CONTROL_PLANE_BROKER`
 *   without editing SFU signaling or telemetry-watcher logic.
 * - All messages adhere strictly to the Zero-Trust allow-list: only WebRTC
 *   routing and orchestration metadata are allowed; SFrame cryptographic keys
 *   and plaintext media must never cross the broker boundary.
 */

import type * as mediasoup from "mediasoup";

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

/**
 * Public transport handshake descriptor permitted on the control plane.
 * Fingerprints and ICE candidates contain zero private key material.
 */
export interface SanitizedTransportDescriptor {
  transportId: string;
  direction: "send" | "recv";
  iceParameters: {
    usernameFragment: string;
    password: string;
    iceLite?: boolean;
  };
  iceCandidates: Array<{
    foundation: string;
    priority: number;
    ip: string;
    address: string;
    protocol: "udp" | "tcp";
    port: number;
    type: "host";
    tcpType?: "passive";
  }>;
  dtlsParameters: {
    role?: "auto" | "client" | "server";
    fingerprints: Array<{
      algorithm: string;
      value: string;
    }>;
  };
}

/**
 * Non-secret RTP routing metadata (SSRCs, MID, MIME type, payload type) used
 * to pre-allocate standby router/consumer structures before handover.
 */
export interface SanitizedRtpMetadata {
  producerId: string;
  kind: mediasoup.types.MediaKind;
  mid?: string;
  ssrcs: number[];
  rtxSsrcs: number[];
  mimeTypes: string[];
  payloadTypes: number[];
}

export type SessionStateEventType =
  | "ROOM_CREATED"
  | "TRANSPORT_SYNC"
  | "PRODUCER_SYNC"
  | "PEER_DISCONNECTED";

export interface SessionStateEvent {
  schemaVersion: "1.0";
  eventType: SessionStateEventType;
  sourceNodeId: string;
  roomId: string;
  peerId: string;
  correlationId: string;
  timestampMs: number;
  transport?: SanitizedTransportDescriptor;
  rtp?: SanitizedRtpMetadata;
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
