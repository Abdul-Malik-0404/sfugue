/**
 * Composition root for `sfu-node`.
 *
 * Lifecycle guarantees:
 * 1. Initialize Mediasoup Worker + default Router BEFORE opening HTTP/Socket.IO
 *    so clients never hit an uninitialized router race on startup.
 * 2. Expose `/metrics` (Prometheus death-signature telemetry) and `/healthz`.
 * 3. Connect `IControlPlaneBroker` non-fatally so standalone local media runs
 *    still work if Kafka is offline, while KinD deployments stream session state
 *    from `sfu-primary` to `sfu-standby` and react to `PROACTIVE_EVACUATION`.
 * 4. Cleanly close HTTP, Socket.IO, telemetry timers, broker, and Mediasoup
 *    worker on `SIGINT`/`SIGTERM`.
 */

import * as http from "http";
import * as path from "path";
import cors from "cors";
import express from "express";
import { Server } from "socket.io";
import { createControlPlaneBroker } from "./broker/factory";
import type { FailoverTriggerEvent, SessionStateEvent } from "./broker/types";
import { loadConfig } from "./config";
import { MediaManager } from "./mediasoup/mediaManager";
import {
  SignalingHandler,
  type TypedSocketIoServer,
} from "./signaling/socketHandler";
import { SfuTelemetry } from "./telemetry/metrics";
import type {
  ClientToServerEvents,
  ServerToClientEvents,
} from "./types/signaling";

async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const app = express();
  app.use(cors({ origin: "*" }));

  const indexHtmlPath = path.resolve(__dirname, "..", "index.html");
  const publicDirPath = path.resolve(__dirname, "..", "public");

  app.get(["/", "/index.html"], (_req, res) => {
    res.sendFile(indexHtmlPath);
  });
  app.use(express.static(publicDirPath));

  const server = http.createServer(app);
  const io: TypedSocketIoServer = new Server<
    ClientToServerEvents,
    ServerToClientEvents
  >(server, {
    cors: { origin: "*" },
  });

  const mediaManager = new MediaManager(config);
  await mediaManager.init();

  const telemetry = new SfuTelemetry(config, mediaManager);
  telemetry.start();

  const broker = createControlPlaneBroker(config);
  const signaling = new SignalingHandler(io, config, mediaManager, broker);
  signaling.attach();

  app.get("/metrics", async (_req, res) => {
    try {
      res.set("Content-Type", telemetry.registry.contentType);
      res.send(await telemetry.renderMetrics());
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      res.status(500).send(msg);
    }
  });

  app.get("/healthz", async (_req, res) => {
    const mediasoupReady = mediaManager.isReady();
    const brokerHealthy = await broker.healthCheck().catch(() => false);
    const statusCode = mediasoupReady ? 200 : 503;

    res.status(statusCode).json({
      status: mediasoupReady ? "ok" : "degraded",
      nodeId: config.nodeId,
      role: config.role,
      mediasoupReady,
      brokerKind: broker.kind,
      brokerHealthy,
    });
  });

  let shuttingDown = false;
  let retryTimer: NodeJS.Timeout | null = null;
  let stateSubscribed = false;
  let failoverSubscribed = false;

  const connectControlPlane = async (attempt = 1): Promise<void> => {
    const maxAttempts = 24;
    if (shuttingDown) {
      return;
    }

    try {
      await broker.connect();

      if (config.role === "standby" && !stateSubscribed) {
        await broker.subscribe<SessionStateEvent>(
          config.kafkaTopicSessionState,
          `${config.kafkaGroupId}-state`,
          async (event) => {
            await mediaManager.handleSessionStateSync(event);
          },
        );
        stateSubscribed = true;
      }

      if (!failoverSubscribed) {
        await broker.subscribe<FailoverTriggerEvent>(
          config.kafkaTopicFailoverTrigger,
          `${config.kafkaGroupId}-failover`,
          async (event) => {
            if (event.eventType !== "PROACTIVE_EVACUATION") {
              return;
            }
            if (config.role === "primary" && event.sourceNodeId === config.nodeId) {
              signaling.broadcastFailoverDirective(event);
            } else if (config.role === "standby") {
              mediaManager.promotePrewarmedSessions(event.correlationId);
            }
          },
        );
        failoverSubscribed = true;
      }

      console.log(
        `[ControlPlane] Connected ${broker.kind} broker for node=${config.nodeId} (${config.kafkaBrokers.join(",")})`,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt >= maxAttempts || shuttingDown) {
        console.warn(
          `[ControlPlane] Broker connection exhausted after ${attempt} attempts (${msg}); continuing in standalone mode`,
        );
        return;
      }

      console.warn(
        `[ControlPlane] Broker attempt ${attempt}/${maxAttempts} failed (${msg}); retrying in 5s...`,
      );
      await broker.disconnect().catch(() => {});
      stateSubscribed = false;
      failoverSubscribed = false;

      retryTimer = setTimeout(() => {
        retryTimer = null;
        void connectControlPlane(attempt + 1);
      }, 5000);
      retryTimer.unref();
    }
  };

  await new Promise<void>((resolve) => {
    server.listen(config.port, () => {
      console.log(
        `SFU Signaling server (${config.nodeId}, role=${config.role}) listening on port ${config.port}`,
      );
      resolve();
    });
  });

  // Connect asynchronously after HTTP listener is up so /healthz responds immediately
  // even while KRaft Kafka finishes booting inside KinD.
  void connectControlPlane();

  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    console.log(`[sfu-node] Received ${signal}, shutting down gracefully...`);

    telemetry.stop();
    await new Promise<void>((resolve) => {
      void io.close(() => resolve());
    });
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    await broker.disconnect().catch((err: unknown) => {
      console.warn("[sfu-node] Broker disconnect warning:", err);
    });
    await mediaManager.close();
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

bootstrap().catch((err) => {
  console.error("[sfu-node] Fatal startup error:", err);
  process.exit(1);
});
