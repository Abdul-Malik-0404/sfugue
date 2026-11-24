/**
 * Socket.IO WebRTC signaling handler for `sfu-node`.
 *
 * Implements the 8-step Mediasoup signaling protocol while supporting
 * multi-peer/multi-track (VP8 + Opus) rooms, clean disconnect teardown,
 * and Zero-Trust `SessionStateEvent` publication on `sfugue.session.state`.
 */

import { randomUUID } from "crypto";
import type { Server, Socket } from "socket.io";
import type { SfuConfig } from "../config";
import { sanitizeSessionStateEvent } from "../broker/sanitizer";
import type {
  FailoverTriggerEvent,
  IControlPlaneBroker,
  SessionStateEvent,
} from "../broker/types";
import type { MediaManager } from "../mediasoup/mediaManager";
import type {
  ClientToServerEvents,
  ServerToClientEvents,
} from "../types/signaling";

export type TypedSocketIoServer = Server<
  ClientToServerEvents,
  ServerToClientEvents
>;

export type TypedSocket = Socket<
  ClientToServerEvents,
  ServerToClientEvents
>;

export class SignalingHandler {
  private readonly io: TypedSocketIoServer;
  private readonly config: SfuConfig;
  private readonly mediaManager: MediaManager;
  private readonly broker: IControlPlaneBroker;

  constructor(
    io: TypedSocketIoServer,
    config: SfuConfig,
    mediaManager: MediaManager,
    broker: IControlPlaneBroker,
  ) {
    this.io = io;
    this.config = config;
    this.mediaManager = mediaManager;
    this.broker = broker;
  }

  public attach(): void {
    this.io.on("connection", (socket) => {
      this.handleConnection(socket);
    });
  }

  public broadcastFailoverDirective(event: FailoverTriggerEvent): void {
    const directive = {
      correlationId: event.correlationId,
      sourceNodeId: event.sourceNodeId,
      targetNodeId: event.targetNodeId,
      targetSignalingUrl: event.targetSignalingUrl,
      reason: event.reason,
      triggeredAtMs: event.triggeredAtMs,
    };

    this.io.emit("proactiveFailover", directive);
    console.log(
      `[Signaling] Broadcasted proactiveFailover to connected peers -> ${directive.targetSignalingUrl} (correlationId=${directive.correlationId})`,
    );
  }

  private handleConnection(socket: TypedSocket): void {
    let activeRoomId = "default";
    this.mediaManager.getOrCreatePeer(socket.id, activeRoomId);
    console.log(`Client connected: ${socket.id}`);

    socket.on("joinRoom", async ({ roomId }, callback) => {
      try {
        activeRoomId = roomId.trim() || "default";
        await this.mediaManager.getOrCreateRouter(activeRoomId);
        this.mediaManager.getOrCreatePeer(socket.id, activeRoomId);

        void this.publishSessionEvent({
          schemaVersion: "1.0",
          eventType: "ROOM_CREATED",
          sourceNodeId: this.config.nodeId,
          roomId: activeRoomId,
          peerId: socket.id,
          correlationId: randomUUID(),
          timestampMs: Date.now(),
        });

        callback({
          roomId: activeRoomId,
          peerId: socket.id,
          existingProducerIds: this.mediaManager.listRoomProducerIds(activeRoomId),
        });
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 1: Client requests RTP capabilities to initialize its Mediasoup Device
    socket.on("getRouterRtpCapabilities", (callback) => {
      callback(this.mediaManager.getRouterRtpCapabilities(activeRoomId));
    });

    // Step 2: Client requests a transport to send media
    socket.on("createProducerTransport", async (callback) => {
      try {
        const managed = await this.mediaManager.createWebRtcTransport(
          activeRoomId,
          socket.id,
          "send",
        );
        const descriptor = this.mediaManager.buildTransportDescriptor(
          managed.transport,
          "send",
        );

        void this.publishSessionEvent({
          schemaVersion: "1.0",
          eventType: "TRANSPORT_SYNC",
          sourceNodeId: this.config.nodeId,
          roomId: activeRoomId,
          peerId: socket.id,
          correlationId: randomUUID(),
          timestampMs: Date.now(),
          transport: descriptor,
        });

        callback({
          id: managed.transport.id,
          iceParameters: managed.transport.iceParameters,
          iceCandidates: managed.transport.iceCandidates,
          dtlsParameters: managed.transport.dtlsParameters,
        });
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 3: Client connects the producer transport using DTLS parameters
    socket.on("connectProducerTransport", async ({ dtlsParameters }, callback) => {
      try {
        await this.mediaManager.connectTransport(socket.id, "send", dtlsParameters);
        callback();
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 4: Client starts sending media (Zero-Trust: RTP headers/SSRCs only)
    socket.on("produce", async ({ kind, rtpParameters }, callback) => {
      try {
        const producer = await this.mediaManager.produce(
          socket.id,
          kind,
          rtpParameters,
        );
        console.log(`Producer created with ID: ${producer.id} (kind=${kind})`);

        void this.publishSessionEvent({
          schemaVersion: "1.0",
          eventType: "PRODUCER_SYNC",
          sourceNodeId: this.config.nodeId,
          roomId: activeRoomId,
          peerId: socket.id,
          correlationId: randomUUID(),
          timestampMs: Date.now(),
          rtp: this.mediaManager.buildRtpMetadata(producer),
        });

        callback({ id: producer.id });
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 5: Client requests a transport to receive media
    socket.on("createConsumerTransport", async (callback) => {
      try {
        const managed = await this.mediaManager.createWebRtcTransport(
          activeRoomId,
          socket.id,
          "recv",
        );
        const descriptor = this.mediaManager.buildTransportDescriptor(
          managed.transport,
          "recv",
        );

        void this.publishSessionEvent({
          schemaVersion: "1.0",
          eventType: "TRANSPORT_SYNC",
          sourceNodeId: this.config.nodeId,
          roomId: activeRoomId,
          peerId: socket.id,
          correlationId: randomUUID(),
          timestampMs: Date.now(),
          transport: descriptor,
        });

        callback({
          id: managed.transport.id,
          iceParameters: managed.transport.iceParameters,
          iceCandidates: managed.transport.iceCandidates,
          dtlsParameters: managed.transport.dtlsParameters,
        });
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 6: Client connects the consumer transport
    socket.on("connectConsumerTransport", async ({ dtlsParameters }, callback) => {
      try {
        await this.mediaManager.connectTransport(socket.id, "recv", dtlsParameters);
        callback();
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 7: Client consumes the media stream
    socket.on("consume", async ({ rtpCapabilities, producerId }, callback) => {
      try {
        const consumer = await this.mediaManager.consume(
          socket.id,
          rtpCapabilities,
          producerId,
        );
        callback({
          id: consumer.id,
          producerId: consumer.producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        });
      } catch (err) {
        callback({ error: this.errorMessage(err) });
      }
    });

    // Step 8: Client resumes the media stream
    socket.on("resume", async (payloadOrCallback, maybeCallback) => {
      const callback =
        typeof payloadOrCallback === "function"
          ? payloadOrCallback
          : maybeCallback;
      const consumerId =
        typeof payloadOrCallback === "function"
          ? undefined
          : payloadOrCallback?.consumerId;

      try {
        await this.mediaManager.resumeConsumer(socket.id, consumerId);
        callback?.();
      } catch (err) {
        callback?.({ error: this.errorMessage(err) });
      }
    });

    socket.on("disconnect", () => {
      const roomId = this.mediaManager.cleanupPeer(socket.id) ?? activeRoomId;
      console.log(`Client disconnected: ${socket.id}`);

      void this.publishSessionEvent({
        schemaVersion: "1.0",
        eventType: "PEER_DISCONNECTED",
        sourceNodeId: this.config.nodeId,
        roomId,
        peerId: socket.id,
        correlationId: randomUUID(),
        timestampMs: Date.now(),
      });
    });
  }

  private async publishSessionEvent(event: SessionStateEvent): Promise<void> {
    if (this.config.role !== "primary") {
      return;
    }

    try {
      const sanitized = sanitizeSessionStateEvent(event);
      await this.broker.publish(
        this.config.kafkaTopicSessionState,
        sanitized,
        sanitized.roomId,
      );
    } catch (err) {
      // Broker degradation must never interrupt active WebRTC signaling
      console.warn("[Signaling] Control-plane publish warning:", this.errorMessage(err));
    }
  }

  private errorMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
