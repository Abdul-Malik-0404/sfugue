/**
 * Mediasoup Worker, Router, Transport, Producer/Consumer, and Standby
 * Pre-Warming manager (`sfu-node`).
 *
 * Maps directly to the 3-tier WebRTC state model in CLAUDE.md:
 * 1. Transport State: Manages constrained UDP/TCP `WebRtcTransport` ports
 *    (`RTC_MIN_PORT..RTC_MAX_PORT`) and tracks live `dtlsState` for ingress
 *    discrimination (valid DTLS sessions vs. unauthenticated UDP floods).
 * 2. Media Pipeline State: Hosts 1 C++ worker per pod and multi-room/multi-peer
 *    VP8/Opus `Router` instances; handles `PROACTIVE_EVACUATION` make-before-break
 *    standby pre-warming from Kafka `sfugue.session.state` events.
 * 3. Cryptographic State: Remains strictly Zero-Trust — Mediasoup never stores,
 *    decrypts, or inspects SFrame keys.
 */

import * as mediasoup from "mediasoup";
import type { SfuConfig } from "../config";
import type {
  SanitizedRtpMetadata,
  SanitizedTransportDescriptor,
  SessionStateEvent,
} from "../broker/types";
import type { TransportDirection } from "../types/signaling";

export const MEDIA_CODECS: mediasoup.types.RouterRtpCodecCapability[] = [
  {
    kind: "audio",
    mimeType: "audio/opus",
    clockRate: 48000,
    channels: 2,
  },
  {
    kind: "video",
    mimeType: "video/VP8",
    clockRate: 90000,
    parameters: {
      "x-google-start-bitrate": 1000,
    },
  },
];

export interface ManagedTransport {
  transport: mediasoup.types.WebRtcTransport;
  direction: TransportDirection;
  dtlsState: mediasoup.types.DtlsState;
}

export interface PeerSession {
  peerId: string;
  roomId: string;
  producerTransport?: ManagedTransport;
  consumerTransport?: ManagedTransport;
  producers: Map<string, mediasoup.types.Producer>;
  consumers: Map<string, mediasoup.types.Consumer>;
}

export interface PrewarmedSession {
  roomId: string;
  peerId: string;
  direction: TransportDirection;
  transport: mediasoup.types.WebRtcTransport;
  createdAtMs: number;
  promoted: boolean;
}

export interface TelemetrySnapshot {
  workerResourceUsage: mediasoup.types.WorkerResourceUsage | null;
  sendTransports: number;
  recvTransports: number;
  dtlsConnectedTransports: number;
  producers: mediasoup.types.Producer[];
  consumers: mediasoup.types.Consumer[];
  prewarmedCount: number;
}

export class MediaManager {
  private readonly config: SfuConfig;
  private worker: mediasoup.types.Worker | null = null;
  private readonly routers = new Map<string, mediasoup.types.Router>();
  private readonly peers = new Map<string, PeerSession>();
  private readonly prewarmedSessions = new Map<string, PrewarmedSession>();
  private sweepTimer: NodeJS.Timeout | null = null;
  private isClosing = false;

  constructor(config: SfuConfig) {
    this.config = config;
  }

  public async init(): Promise<void> {
    if (this.worker) {
      return;
    }

    // Enforce deterministic footprint: 1 Mediasoup C++ worker per pod constrained
    // to the KinD extraPortMappings UDP port window.
    const worker = await mediasoup.createWorker({
      logLevel: "warn",
      rtcMinPort: this.config.rtcMinPort,
      rtcMaxPort: this.config.rtcMaxPort,
    });

    worker.on("died", () => {
      if (this.isClosing) {
        return;
      }
      console.error(
        "Mediasoup worker died, exiting in 2 seconds... [pid:%d]",
        worker.pid,
      );
      setTimeout(() => process.exit(1), 2000);
    });

    this.worker = worker;
    await this.getOrCreateRouter("default");

    // Periodically evict expired standby pre-warmed transports so stale states
    // never exhaust the 8 GB RAM host budget.
    this.sweepTimer = setInterval(() => {
      this.sweepExpiredPrewarmed();
    }, 15_000);
    this.sweepTimer.unref();

    console.log(
      `[MediaManager] Worker [pid:${worker.pid}] ready (${this.config.nodeId}, role=${this.config.role}, ports=${this.config.rtcMinPort}-${this.config.rtcMaxPort})`,
    );
  }

  public isReady(): boolean {
    return this.worker !== null && !this.worker.closed && !this.worker.died;
  }

  public async getOrCreateRouter(roomId: string): Promise<mediasoup.types.Router> {
    const existing = this.routers.get(roomId);
    if (existing && !existing.closed) {
      return existing;
    }

    if (!this.worker || this.worker.closed) {
      throw new Error("Mediasoup worker is not initialized");
    }

    const router = await this.worker.createRouter({ mediaCodecs: MEDIA_CODECS });
    this.routers.set(roomId, router);
    return router;
  }

  public getRouterRtpCapabilities(
    roomId = "default",
  ): mediasoup.types.RtpCapabilities {
    const router = this.routers.get(roomId);
    if (!router || router.closed) {
      throw new Error(`Router for room "${roomId}" is not ready`);
    }
    return router.rtpCapabilities;
  }

  public getOrCreatePeer(peerId: string, roomId = "default"): PeerSession {
    const existing = this.peers.get(peerId);
    if (existing) {
      if (existing.roomId !== roomId) {
        existing.roomId = roomId;
      }
      return existing;
    }

    const created: PeerSession = {
      peerId,
      roomId,
      producers: new Map(),
      consumers: new Map(),
    };
    this.peers.set(peerId, created);
    return created;
  }

  public listRoomProducerIds(roomId: string): string[] {
    const ids: string[] = [];
    for (const peer of this.peers.values()) {
      if (peer.roomId === roomId) {
        for (const producerId of peer.producers.keys()) {
          ids.push(producerId);
        }
      }
    }
    return ids;
  }

  public async createWebRtcTransport(
    roomId: string,
    peerId: string,
    direction: TransportDirection,
  ): Promise<ManagedTransport> {
    const peer = this.getOrCreatePeer(peerId, roomId);
    const router = await this.getOrCreateRouter(roomId);

    // Claim a standby pre-warmed transport if one was already provisioned via Kafka.
    const prewarmKey = this.prewarmKey(roomId, peerId, direction);
    const prewarmed = this.prewarmedSessions.get(prewarmKey);
    let transport: mediasoup.types.WebRtcTransport;

    if (prewarmed && !prewarmed.transport.closed) {
      this.prewarmedSessions.delete(prewarmKey);
      transport = prewarmed.transport;
      console.log(
        `[MediaManager] Claimed pre-warmed ${direction} transport ${transport.id} for peer=${peerId} room=${roomId}`,
      );
    } else {
      transport = await this.allocateWebRtcTransport(router);
    }

    const managed: ManagedTransport = {
      transport,
      direction,
      dtlsState: transport.dtlsState,
    };

    transport.on("dtlsstatechange", (dtlsState) => {
      managed.dtlsState = dtlsState;
    });

    if (direction === "send") {
      peer.producerTransport?.transport.close();
      peer.producerTransport = managed;
    } else {
      peer.consumerTransport?.transport.close();
      peer.consumerTransport = managed;
    }

    return managed;
  }

  public async connectTransport(
    peerId: string,
    direction: TransportDirection,
    dtlsParameters: mediasoup.types.DtlsParameters,
  ): Promise<void> {
    const peer = this.peers.get(peerId);
    const managed =
      direction === "send" ? peer?.producerTransport : peer?.consumerTransport;
    if (!managed) {
      throw new Error(`No ${direction} transport registered for peer "${peerId}"`);
    }
    await managed.transport.connect({ dtlsParameters });
  }

  public async produce(
    peerId: string,
    kind: mediasoup.types.MediaKind,
    rtpParameters: mediasoup.types.RtpParameters,
  ): Promise<mediasoup.types.Producer> {
    const peer = this.peers.get(peerId);
    if (!peer?.producerTransport) {
      throw new Error(`No send transport registered for peer "${peerId}"`);
    }

    const producer = await peer.producerTransport.transport.produce({
      kind,
      rtpParameters,
    });

    peer.producers.set(producer.id, producer);
    producer.on("transportclose", () => {
      peer.producers.delete(producer.id);
    });

    return producer;
  }

  public async consume(
    peerId: string,
    rtpCapabilities: mediasoup.types.RtpCapabilities,
    requestedProducerId?: string,
  ): Promise<mediasoup.types.Consumer> {
    const peer = this.peers.get(peerId);
    if (!peer?.consumerTransport) {
      throw new Error(`No recv transport registered for peer "${peerId}"`);
    }

    const router = await this.getOrCreateRouter(peer.roomId);
    const targetProducerId =
      requestedProducerId ?? this.resolveLatestRoomProducerId(peer.roomId, peerId);

    if (!targetProducerId) {
      throw new Error(`No active producer available in room "${peer.roomId}"`);
    }
    if (!router.canConsume({ producerId: targetProducerId, rtpCapabilities })) {
      throw new Error("Cannot consume");
    }

    // Start paused so the client decoder/SFrame worker attaches before keyframes arrive.
    const consumer = await peer.consumerTransport.transport.consume({
      producerId: targetProducerId,
      rtpCapabilities,
      paused: true,
    });

    peer.consumers.set(consumer.id, consumer);
    consumer.on("transportclose", () => {
      peer.consumers.delete(consumer.id);
    });
    consumer.on("producerclose", () => {
      peer.consumers.delete(consumer.id);
    });

    return consumer;
  }

  public async resumeConsumer(peerId: string, consumerId?: string): Promise<void> {
    const peer = this.peers.get(peerId);
    if (!peer) {
      return;
    }

    if (consumerId) {
      const target = peer.consumers.get(consumerId);
      if (target && !target.closed) {
        await target.resume();
      }
      return;
    }

    for (const consumer of peer.consumers.values()) {
      if (!consumer.closed && consumer.paused) {
        await consumer.resume();
      }
    }
  }

  public cleanupPeer(peerId: string): string | null {
    const peer = this.peers.get(peerId);
    if (!peer) {
      return null;
    }

    for (const consumer of peer.consumers.values()) {
      if (!consumer.closed) {
        consumer.close();
      }
    }
    peer.consumers.clear();

    for (const producer of peer.producers.values()) {
      if (!producer.closed) {
        producer.close();
      }
    }
    peer.producers.clear();

    if (peer.producerTransport && !peer.producerTransport.transport.closed) {
      peer.producerTransport.transport.close();
    }
    if (peer.consumerTransport && !peer.consumerTransport.transport.closed) {
      peer.consumerTransport.transport.close();
    }

    this.peers.delete(peerId);
    return peer.roomId;
  }

  /**
   * Standby make-before-break preparation triggered by Kafka `sfugue.session.state`.
   * Pre-allocates router + WebRTC transport UDP sockets ahead of handover without
   * ever touching client-side SFrame keys.
   */
  public async handleSessionStateSync(event: SessionStateEvent): Promise<void> {
    if (this.config.role !== "standby") {
      return;
    }

    if (event.eventType === "PEER_DISCONNECTED") {
      this.deletePrewarmedForPeer(event.roomId, event.peerId);
      return;
    }

    const router = await this.getOrCreateRouter(event.roomId);

    if (
      (event.eventType === "TRANSPORT_SYNC" || event.eventType === "ROOM_CREATED") &&
      event.transport
    ) {
      const key = this.prewarmKey(
        event.roomId,
        event.peerId,
        event.transport.direction,
      );
      if (!this.prewarmedSessions.has(key)) {
        const transport = await this.allocateWebRtcTransport(router);
        this.prewarmedSessions.set(key, {
          roomId: event.roomId,
          peerId: event.peerId,
          direction: event.transport.direction,
          transport,
          createdAtMs: Date.now(),
          promoted: false,
        });
        console.log(
          `[Standby] Pre-warmed ${event.transport.direction} transport ${transport.id} for room=${event.roomId} peer=${event.peerId}`,
        );
      }
    } else if (event.eventType === "PRODUCER_SYNC" && event.rtp) {
      console.log(
        `[Standby] Synced RTP metadata producer=${event.rtp.producerId} kind=${event.rtp.kind} ssrcs=[${event.rtp.ssrcs.join(",")}]`,
      );
    }
  }

  public promotePrewarmedSessions(correlationId: string): number {
    let promoted = 0;
    for (const session of this.prewarmedSessions.values()) {
      if (!session.transport.closed && !session.promoted) {
        session.promoted = true;
        promoted += 1;
      }
    }
    console.log(
      `[Standby] Promoted ${promoted} pre-warmed transport(s) for proactive evacuation correlationId=${correlationId}`,
    );
    return promoted;
  }

  public buildTransportDescriptor(
    transport: mediasoup.types.WebRtcTransport,
    direction: TransportDirection,
  ): SanitizedTransportDescriptor {
    return {
      transportId: transport.id,
      direction,
      iceParameters: {
        usernameFragment: transport.iceParameters.usernameFragment,
        password: transport.iceParameters.password,
        ...(transport.iceParameters.iceLite !== undefined
          ? { iceLite: transport.iceParameters.iceLite }
          : {}),
      },
      iceCandidates: transport.iceCandidates.map((candidate) => ({
        foundation: candidate.foundation,
        priority: candidate.priority,
        ip: candidate.ip,
        address: candidate.address,
        protocol: candidate.protocol,
        port: candidate.port,
        type: candidate.type,
        ...(candidate.tcpType ? { tcpType: candidate.tcpType } : {}),
      })),
      dtlsParameters: {
        ...(transport.dtlsParameters.role
          ? { role: transport.dtlsParameters.role }
          : {}),
        fingerprints: transport.dtlsParameters.fingerprints.map((fp) => ({
          algorithm: fp.algorithm,
          value: fp.value,
        })),
      },
    };
  }

  public buildRtpMetadata(producer: mediasoup.types.Producer): SanitizedRtpMetadata {
    const encodings = producer.rtpParameters.encodings ?? [];
    const codecs = producer.rtpParameters.codecs ?? [];

    return {
      producerId: producer.id,
      kind: producer.kind,
      ...(producer.rtpParameters.mid !== undefined
        ? { mid: producer.rtpParameters.mid }
        : {}),
      ssrcs: encodings
        .map((enc) => enc.ssrc)
        .filter((ssrc): ssrc is number => typeof ssrc === "number"),
      rtxSsrcs: encodings
        .map((enc) => enc.rtx?.ssrc)
        .filter((ssrc): ssrc is number => typeof ssrc === "number"),
      mimeTypes: codecs.map((codec) => codec.mimeType),
      payloadTypes: codecs.map((codec) => codec.payloadType),
    };
  }

  public async getTelemetrySnapshot(): Promise<TelemetrySnapshot> {
    let sendTransports = 0;
    let recvTransports = 0;
    let dtlsConnectedTransports = 0;
    const producers: mediasoup.types.Producer[] = [];
    const consumers: mediasoup.types.Consumer[] = [];

    for (const peer of this.peers.values()) {
      if (peer.producerTransport && !peer.producerTransport.transport.closed) {
        sendTransports += 1;
        if (peer.producerTransport.dtlsState === "connected") {
          dtlsConnectedTransports += 1;
        }
      }
      if (peer.consumerTransport && !peer.consumerTransport.transport.closed) {
        recvTransports += 1;
        if (peer.consumerTransport.dtlsState === "connected") {
          dtlsConnectedTransports += 1;
        }
      }
      for (const producer of peer.producers.values()) {
        if (!producer.closed) {
          producers.push(producer);
        }
      }
      for (const consumer of peer.consumers.values()) {
        if (!consumer.closed) {
          consumers.push(consumer);
        }
      }
    }

    let workerResourceUsage: mediasoup.types.WorkerResourceUsage | null = null;
    if (this.worker && !this.worker.closed && !this.worker.died) {
      try {
        workerResourceUsage = await this.worker.getResourceUsage();
      } catch {
        workerResourceUsage = null;
      }
    }

    return {
      workerResourceUsage,
      sendTransports,
      recvTransports,
      dtlsConnectedTransports,
      producers,
      consumers,
      prewarmedCount: this.prewarmedSessions.size,
    };
  }

  public async close(): Promise<void> {
    this.isClosing = true;
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }

    for (const peerId of Array.from(this.peers.keys())) {
      this.cleanupPeer(peerId);
    }

    for (const prewarmed of this.prewarmedSessions.values()) {
      if (!prewarmed.transport.closed) {
        prewarmed.transport.close();
      }
    }
    this.prewarmedSessions.clear();

    for (const router of this.routers.values()) {
      if (!router.closed) {
        router.close();
      }
    }
    this.routers.clear();

    if (this.worker && !this.worker.closed) {
      this.worker.close();
      this.worker = null;
    }
  }

  private async allocateWebRtcTransport(
    router: mediasoup.types.Router,
  ): Promise<mediasoup.types.WebRtcTransport> {
    const listenInfos: mediasoup.types.TransportListenInfo[] = [
      {
        protocol: "udp",
        ip: "0.0.0.0",
        announcedAddress: this.config.announcedIp,
        portRange: {
          min: this.config.rtcMinPort,
          max: this.config.rtcMaxPort,
        },
      },
      {
        protocol: "tcp",
        ip: "0.0.0.0",
        announcedAddress: this.config.announcedIp,
        portRange: {
          min: this.config.rtcMinPort,
          max: this.config.rtcMaxPort,
        },
      },
    ];

    const transport = await router.createWebRtcTransport({
      listenInfos,
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: this.config.initialOutgoingBitrate,
    });

    if (this.config.maxIncomingBitrate > 0) {
      try {
        await transport.setMaxIncomingBitrate(this.config.maxIncomingBitrate);
      } catch {
        // Non-fatal if BWE cap cannot be applied on an uninitialized transport
      }
    }

    return transport;
  }

  private resolveLatestRoomProducerId(
    roomId: string,
    preferPeerId: string,
  ): string | undefined {
    const preferredPeer = this.peers.get(preferPeerId);
    if (preferredPeer && preferredPeer.roomId === roomId) {
      let latestOwn: string | undefined;
      for (const id of preferredPeer.producers.keys()) {
        latestOwn = id;
      }
      if (latestOwn) {
        return latestOwn;
      }
    }

    let latestRoomProducer: string | undefined;
    for (const peer of this.peers.values()) {
      if (peer.roomId === roomId) {
        for (const id of peer.producers.keys()) {
          latestRoomProducer = id;
        }
      }
    }
    return latestRoomProducer;
  }

  private prewarmKey(
    roomId: string,
    peerId: string,
    direction: TransportDirection,
  ): string {
    return `${roomId}:${peerId}:${direction}`;
  }

  private deletePrewarmedForPeer(roomId: string, peerId: string): void {
    for (const direction of ["send", "recv"] as const) {
      const key = this.prewarmKey(roomId, peerId, direction);
      const entry = this.prewarmedSessions.get(key);
      if (entry) {
        if (!entry.transport.closed) {
          entry.transport.close();
        }
        this.prewarmedSessions.delete(key);
      }
    }
  }

  private sweepExpiredPrewarmed(): void {
    const now = Date.now();
    for (const [key, entry] of this.prewarmedSessions.entries()) {
      if (now - entry.createdAtMs > this.config.prewarmTtlMs) {
        if (!entry.transport.closed) {
          entry.transport.close();
        }
        this.prewarmedSessions.delete(key);
      }
    }
  }
}
