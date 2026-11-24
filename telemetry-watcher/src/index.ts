/**
 * Entrypoint for `telemetry-watcher`: polls Prometheus on a fixed cadence,
 * filters unauthenticated UDP floods from organic WebRTC death signatures,
 * and emits Zero-Trust `PROACTIVE_EVACUATION` triggers to Kafka/Redis.
 */

import { randomUUID } from "crypto";
import { createControlPlaneBroker } from "./broker/factory";
import type { FailoverTriggerEvent } from "./broker/types";
import { loadConfig } from "./config";
import { DeathSignatureDetector } from "./detector";
import { PrometheusClient } from "./prometheusClient";

async function main(): Promise<void> {
  const config = loadConfig();
  const prometheus = new PrometheusClient(
    config.prometheusUrl,
    config.targetNodeId,
  );
  const detector = new DeathSignatureDetector(config);
  const broker = createControlPlaneBroker(config);

  try {
    await broker.connect();
    console.log(
      `[telemetry-watcher] Connected ${broker.kind} broker (${config.kafkaBrokers.join(",")})`,
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(
      `[telemetry-watcher] Initial broker connection failed (${msg}); will retry before publishing`,
    );
  }

  let pollInFlight = false;
  let stopping = false;

  const runPoll = async (): Promise<void> => {
    if (pollInFlight || stopping) {
      return;
    }
    pollInFlight = true;

    try {
      const snapshot = await prometheus.fetchNodeSnapshot();
      const outcome = detector.evaluate(snapshot, snapshot.sampledAtMs);

      if (outcome.verdict === "TRIGGER_FAILOVER") {
        const triggerEvent: FailoverTriggerEvent = {
          schemaVersion: "1.0",
          eventType: "PROACTIVE_EVACUATION",
          correlationId: randomUUID(),
          sourceNodeId: config.targetNodeId,
          targetNodeId: config.standbyNodeId,
          targetSignalingUrl: config.standbySignalingUrl,
          reason: "DEATH_SIGNATURE_CONFIRMED",
          triggeredAtMs: snapshot.sampledAtMs,
          metricsSnapshot: {
            jitterMs: snapshot.jitterMs,
            pliRatePerSec: snapshot.pliRatePerSec,
            nackRatePerSec: snapshot.nackRatePerSec,
            workerCpuRatio: snapshot.workerCpuRatio,
            udpDropRatePerSec: snapshot.udpDropRatePerSec,
            validDtlsSessionRatio: outcome.validDtlsSessionRatio,
          },
        };

        if (!(await broker.healthCheck())) {
          await broker.connect();
        }

        await broker.publish(
          config.kafkaTopicFailoverTrigger,
          triggerEvent,
          config.targetNodeId,
        );

        console.warn(
          `[telemetry-watcher] TRIGGER_FAILOVER published correlationId=${triggerEvent.correlationId}: ${outcome.reason}`,
        );
      } else if (outcome.verdict !== "HEALTHY") {
        console.log(`[telemetry-watcher] ${outcome.verdict}: ${outcome.reason}`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`[telemetry-watcher] Poll cycle error: ${msg}`);
    } finally {
      pollInFlight = false;
    }
  };

  console.log(
    `[telemetry-watcher] Watching node="${config.targetNodeId}" via ${config.prometheusUrl} every ${config.pollIntervalMs}ms (cooldown=${config.failoverCooldownMs}ms)`,
  );

  void runPoll();
  const timer = setInterval(() => {
    void runPoll();
  }, config.pollIntervalMs);

  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    clearInterval(timer);
    console.log(
      `[telemetry-watcher] Received ${signal}. Summary counters:`,
      JSON.stringify(detector.getCounters()),
    );
    await broker.disconnect().catch(() => {});
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

main().catch((err) => {
  console.error("[telemetry-watcher] Fatal startup error:", err);
  process.exit(1);
});
