/**
 * Strongly typed Socket.IO signaling wire contracts between WebRTC clients
 * (including `index.html` and Playwright `load-gen` bots) and `sfu-node`.
 *
 * Preserves the baseline 8-step acknowledgment protocol while adding optional
 * room registration (`joinRoom`) and server-pushed proactive failover directives
 * (`proactiveFailover`) containing Zero-Trust routing metadata only.
 */

import type * as mediasoup from "mediasoup";

export type TransportDirection = "send" | "recv";

export interface ErrorAck {
  error: string;
}

export interface TransportCreatedAck {
  id: string;
  iceParameters: mediasoup.types.IceParameters;
  iceCandidates: mediasoup.types.IceCandidate[];
  dtlsParameters: mediasoup.types.DtlsParameters;
}

export interface ConnectTransportPayload {
  dtlsParameters: mediasoup.types.DtlsParameters;
}

export interface ProducePayload {
  kind: mediasoup.types.MediaKind;
  rtpParameters: mediasoup.types.RtpParameters;
}

export interface ProduceAck {
  id: string;
}

export interface ConsumePayload {
  rtpCapabilities: mediasoup.types.RtpCapabilities;
  producerId?: string;
}

export interface ConsumeAck {
  id: string;
  producerId: string;
  kind: mediasoup.types.MediaKind;
  rtpParameters: mediasoup.types.RtpParameters;
}

export interface ResumePayload {
  consumerId?: string;
}

export interface JoinRoomPayload {
  roomId: string;
  peerId?: string;
}

export interface JoinRoomAck {
  roomId: string;
  peerId: string;
  existingProducerIds: string[];
}

export interface ProactiveFailoverDirective {
  correlationId: string;
  sourceNodeId: string;
  targetNodeId: string;
  targetSignalingUrl: string;
  reason: string;
  triggeredAtMs: number;
}

export interface ClientToServerEvents {
  joinRoom: (
    payload: JoinRoomPayload,
    callback: (response: JoinRoomAck | ErrorAck) => void,
  ) => void;
  getRouterRtpCapabilities: (
    callback: (capabilities: mediasoup.types.RtpCapabilities) => void,
  ) => void;
  createProducerTransport: (
    callback: (response: TransportCreatedAck | ErrorAck) => void,
  ) => void;
  connectProducerTransport: (
    payload: ConnectTransportPayload,
    callback: (response?: ErrorAck) => void,
  ) => void;
  produce: (
    payload: ProducePayload,
    callback: (response: ProduceAck | ErrorAck) => void,
  ) => void;
  createConsumerTransport: (
    callback: (response: TransportCreatedAck | ErrorAck) => void,
  ) => void;
  connectConsumerTransport: (
    payload: ConnectTransportPayload,
    callback: (response?: ErrorAck) => void,
  ) => void;
  consume: (
    payload: ConsumePayload,
    callback: (response: ConsumeAck | ErrorAck) => void,
  ) => void;
  resume: (
    payloadOrCallback: ResumePayload | (() => void),
    maybeCallback?: (response?: ErrorAck) => void,
  ) => void;
}

export interface ServerToClientEvents {
  proactiveFailover: (directive: ProactiveFailoverDirective) => void;
}
