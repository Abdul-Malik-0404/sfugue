import express from "express";
import * as http from "http";
import { Server } from "socket.io";
import * as mediasoup from "mediasoup";
import { Router } from "mediasoup/node/lib/Router";
import { Worker } from "mediasoup/node/lib/Worker";
import { WebRtcTransport } from "mediasoup/node/lib/WebRtcTransport";

const app = express();
app.use(express.static("public"));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

// Mediasoup state
let worker: Worker;
let router: Router;
let producerTransport: WebRtcTransport | undefined;
let consumerTransport: WebRtcTransport | undefined;
let producer: mediasoup.types.Producer | undefined;
let consumer: mediasoup.types.Consumer | undefined;

// Basic VP8 codec configuration for the router
const mediaCodecs: mediasoup.types.RtpCodecCapability[] = [
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

async function startMediasoup() {
  // 1. Create a Worker (runs as a C++ subprocess)
  worker = await mediasoup.createWorker({
    logLevel: "warn",
  });

  worker.on("died", () => {
    console.error(
      "Mediasoup worker died, exiting in 2 seconds... [pid:%d]",
      worker.pid,
    );
    setTimeout(() => process.exit(1), 2000);
  });

  // 2. Create a Router (handles media streams between producers and consumers)
  router = await worker.createRouter({ mediaCodecs });
  console.log("Mediasoup Router created successfully.");
}

async function createWebRtcTransport(): Promise<WebRtcTransport> {
  const maxIncomingBitrate = 1500000;
  const initialAvailableOutgoingBitrate = 1000000;

  // 3. Create WebRTC Transport within the router context
  const transport = await router.createWebRtcTransport({
    listenIps: [{ ip: "0.0.0.0", announcedIp: "127.0.0.1" }], // Update announcedIp for production
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
    initialAvailableOutgoingBitrate,
  });

  if (maxIncomingBitrate) {
    try {
      await transport.setMaxIncomingBitrate(maxIncomingBitrate);
    } catch (error) { }
  }

  return transport;
}

// Signaling API
io.on("connection", (socket) => {
  console.log(`Client connected: ${socket.id}`);

  // Step 1: Client requests RTP capabilities to initialize its device
  socket.on("getRouterRtpCapabilities", (callback) => {
    callback(router.rtpCapabilities);
  });

  // Step 2: Client requests a transport to send media
  socket.on("createProducerTransport", async (callback) => {
    try {
      producerTransport = await createWebRtcTransport();
      callback({
        id: producerTransport.id,
        iceParameters: producerTransport.iceParameters,
        iceCandidates: producerTransport.iceCandidates,
        dtlsParameters: producerTransport.dtlsParameters,
      });
    } catch (err: any) {
      callback({ error: err.message });
    }
  });

  // Step 3: Client connects the producer transport using DTLS parameters
  socket.on(
    "connectProducerTransport",
    async ({ dtlsParameters }, callback) => {
      await producerTransport?.connect({ dtlsParameters });
      callback();
    },
  );

  // Step 4: Client starts sending media
  socket.on("produce", async ({ kind, rtpParameters }, callback) => {
    producer = await producerTransport?.produce({ kind, rtpParameters });
    console.log(`Producer created with ID: ${producer?.id}`);
    callback({ id: producer?.id });
  });

  // Step 5: Client requests a transport to receive media
  socket.on("createConsumerTransport", async (callback) => {
    try {
      consumerTransport = await createWebRtcTransport();
      callback({
        id: consumerTransport.id,
        iceParameters: consumerTransport.iceParameters,
        iceCandidates: consumerTransport.iceCandidates,
        dtlsParameters: consumerTransport.dtlsParameters,
      });
    } catch (err: any) {
      callback({ error: err.message });
    }
  });

  // Step 6: Client connects the consumer transport
  socket.on(
    "connectConsumerTransport",
    async ({ dtlsParameters }, callback) => {
      await consumerTransport?.connect({ dtlsParameters });
      callback();
    },
  );

  // Step 7: Client consumes the media stream
  socket.on("consume", async ({ rtpCapabilities }, callback) => {
    try {
      if (!router.canConsume({ producerId: producer!.id, rtpCapabilities })) {
        return callback({ error: "Cannot consume" });
      }

      consumer = await consumerTransport?.consume({
        producerId: producer!.id,
        rtpCapabilities,
        paused: true, // Start paused, let the client resume it
      });

      callback({
        id: consumer!.id,
        producerId: producer!.id,
        kind: consumer!.kind,
        rtpParameters: consumer!.rtpParameters,
      });
    } catch (err: any) {
      callback({ error: err.message });
    }
  });

  // Step 8: Client resumes the media stream
  socket.on("resume", async (callback) => {
    await consumer?.resume();
    callback();
  });

  socket.on("disconnect", () => {
    console.log(`Client disconnected: ${socket.id}`);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  await startMediasoup();
  console.log(`SFU Signaling server listening on port ${PORT}`);
});
