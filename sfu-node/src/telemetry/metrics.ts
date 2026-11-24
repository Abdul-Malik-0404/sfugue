/**
 * Prometheus observability module (`GET /metrics`) for `sfu-node`.
 *
 * Exposes all algorithmic "death signature" telemetry signals required by
 * `telemetry-watcher` (CLAUDE.md Section 1 & Scope 2):
 *   Jitter > 40ms -> Kernel UDP Buffer Drops -> PLI/NACK Spike -> CPU Saturation
 * along with DTLS-connected transport counts for ingress discrimination
 * (organic flash crowd vs. unauthenticated UDP flood).
 */

import * as fs from "fs/promises";
import { monitorEventLoopDelay } from "perf_hooks";
import * as promClient from "prom-client";
import type * as mediasoup from "mediasoup";
import type { SfuConfig } from "../config";
import type { MediaManager } from "../mediasoup/mediaManager";

interface PrevStreamStats {
  packetCount: number;
  packetsLost: number;
  pliCount: number;
  nackCount: number;
}

interface PrevWorkerCpu {
  ruUtimeMs: number;
  ruStimeMs: number;
  sampledAtMs: number;
}

export class SfuTelemetry {
  public readonly registry: promClient.Registry;

  private readonly config: SfuConfig;
  private readonly mediaManager: MediaManager;
  private readonly loopDelay: ReturnType<typeof monitorEventLoopDelay>;
  private pollTimer: NodeJS.Timeout | null = null;
  private pollingInFlight = false;

  private readonly prevProducerStats = new Map<string, PrevStreamStats>();
  private readonly prevConsumerStats = new Map<string, PrevStreamStats>();
  private prevWorkerCpu: PrevWorkerCpu | null = null;
  private prevUdpErrorsTotal: number | null = null;

  private readonly activeTransportsGauge: promClient.Gauge<
    "node_id" | "role" | "direction"
  >;
  private readonly dtlsConnectedGauge: promClient.Gauge<"node_id" | "role">;
  private readonly activeProducersGauge: promClient.Gauge<
    "node_id" | "role" | "kind"
  >;
  private readonly activeConsumersGauge: promClient.Gauge<
    "node_id" | "role" | "kind"
  >;
  private readonly workerCpuRatioGauge: promClient.Gauge<"node_id" | "role">;
  private readonly workerCpuUserGauge: promClient.Gauge<"node_id" | "role">;
  private readonly workerCpuSystemGauge: promClient.Gauge<"node_id" | "role">;
  private readonly workerMemoryRssGauge: promClient.Gauge<"node_id" | "role">;
  private readonly eventLoopLagGauge: promClient.Gauge<"node_id" | "role">;
  private readonly rtpPacketsReceivedCounter: promClient.Counter<
    "node_id" | "role" | "kind"
  >;
  private readonly rtpPacketsSentCounter: promClient.Counter<
    "node_id" | "role" | "kind"
  >;
  private readonly rtpPacketsLostCounter: promClient.Counter<
    "node_id" | "role" | "kind"
  >;
  private readonly rtpJitterGauge: promClient.Gauge<"node_id" | "role" | "kind">;
  private readonly rtcpPliCounter: promClient.Counter<"node_id" | "role">;
  private readonly rtcpNackCounter: promClient.Counter<"node_id" | "role">;
  private readonly udpReceiveErrorsCounter: promClient.Counter<
    "node_id" | "role"
  >;
  private readonly prewarmedSessionsGauge: promClient.Gauge<"node_id" | "role">;

  constructor(config: SfuConfig, mediaManager: MediaManager) {
    this.config = config;
    this.mediaManager = mediaManager;
    this.registry = new promClient.Registry();

    promClient.collectDefaultMetrics({ register: this.registry });

    this.loopDelay = monitorEventLoopDelay({ resolution: 20 });
    this.loopDelay.enable();

    this.activeTransportsGauge = new promClient.Gauge({
      name: "sfugue_active_transports",
      help: "Number of active Mediasoup WebRTC transports by direction",
      labelNames: ["node_id", "role", "direction"],
      registers: [this.registry],
    });

    this.dtlsConnectedGauge = new promClient.Gauge({
      name: "sfugue_dtls_connected_transports",
      help: "Number of active WebRTC transports with completed DTLS handshake",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.activeProducersGauge = new promClient.Gauge({
      name: "sfugue_active_producers",
      help: "Number of active Mediasoup producers by media kind",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.activeConsumersGauge = new promClient.Gauge({
      name: "sfugue_active_consumers",
      help: "Number of active Mediasoup consumers by media kind",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.workerCpuRatioGauge = new promClient.Gauge({
      name: "sfugue_worker_cpu_utilization_ratio",
      help: "Mediasoup C++ worker CPU utilization ratio (user + sys time / wall time)",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.workerCpuUserGauge = new promClient.Gauge({
      name: "sfugue_worker_cpu_user_seconds_total",
      help: "Cumulative Mediasoup C++ worker user CPU seconds",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.workerCpuSystemGauge = new promClient.Gauge({
      name: "sfugue_worker_cpu_system_seconds_total",
      help: "Cumulative Mediasoup C++ worker system CPU seconds",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.workerMemoryRssGauge = new promClient.Gauge({
      name: "sfugue_worker_memory_rss_bytes",
      help: "Mediasoup C++ worker max resident set size in bytes",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.eventLoopLagGauge = new promClient.Gauge({
      name: "sfugue_event_loop_lag_ms",
      help: "Node.js signaling event loop p99 delay in milliseconds",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.rtpPacketsReceivedCounter = new promClient.Counter({
      name: "sfugue_rtp_packets_received_total",
      help: "Cumulative RTP packets ingested by Mediasoup producers",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.rtpPacketsSentCounter = new promClient.Counter({
      name: "sfugue_rtp_packets_sent_total",
      help: "Cumulative RTP packets forwarded by Mediasoup consumers",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.rtpPacketsLostCounter = new promClient.Counter({
      name: "sfugue_rtp_packets_lost_total",
      help: "Cumulative RTP packets lost across active streams",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.rtpJitterGauge = new promClient.Gauge({
      name: "sfugue_rtp_jitter_ms",
      help: "Maximum observed RFC 3550 RTP jitter in milliseconds by media kind",
      labelNames: ["node_id", "role", "kind"],
      registers: [this.registry],
    });

    this.rtcpPliCounter = new promClient.Counter({
      name: "sfugue_rtcp_pli_total",
      help: "Cumulative RTCP Picture Loss Indication (PLI) requests observed",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.rtcpNackCounter = new promClient.Counter({
      name: "sfugue_rtcp_nack_total",
      help: "Cumulative RTCP NACK retransmission requests observed",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.udpReceiveErrorsCounter = new promClient.Counter({
      name: "sfugue_udp_socket_receive_errors_total",
      help: "Kernel UDP receive and socket buffer errors parsed from /proc/net/snmp",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.prewarmedSessionsGauge = new promClient.Gauge({
      name: "sfugue_prewarmed_sessions",
      help: "Number of standby pre-warmed transports staged for make-before-break handover",
      labelNames: ["node_id", "role"],
      registers: [this.registry],
    });

    this.initializeZeroSeries();
  }

  public start(): void {
    if (this.pollTimer) {
      return;
    }

    void this.collectOnce();
    this.pollTimer = setInterval(() => {
      void this.collectOnce();
    }, this.config.metricsPollIntervalMs);
    this.pollTimer.unref();
  }

  public async renderMetrics(): Promise<string> {
    await this.collectOnce();
    return this.registry.metrics();
  }

  public stop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    this.loopDelay.disable();
  }

  private initializeZeroSeries(): void {
    const base = { node_id: this.config.nodeId, role: this.config.role };
    this.activeTransportsGauge.set({ ...base, direction: "send" }, 0);
    this.activeTransportsGauge.set({ ...base, direction: "recv" }, 0);
    this.dtlsConnectedGauge.set(base, 0);
    this.activeProducersGauge.set({ ...base, kind: "audio" }, 0);
    this.activeProducersGauge.set({ ...base, kind: "video" }, 0);
    this.activeConsumersGauge.set({ ...base, kind: "audio" }, 0);
    this.activeConsumersGauge.set({ ...base, kind: "video" }, 0);
    this.workerCpuRatioGauge.set(base, 0);
    this.workerCpuUserGauge.set(base, 0);
    this.workerCpuSystemGauge.set(base, 0);
    this.workerMemoryRssGauge.set(base, 0);
    this.eventLoopLagGauge.set(base, 0);
    this.rtpJitterGauge.set({ ...base, kind: "audio" }, 0);
    this.rtpJitterGauge.set({ ...base, kind: "video" }, 0);
    this.prewarmedSessionsGauge.set(base, 0);

    this.rtpPacketsReceivedCounter.inc({ ...base, kind: "audio" }, 0);
    this.rtpPacketsReceivedCounter.inc({ ...base, kind: "video" }, 0);
    this.rtpPacketsSentCounter.inc({ ...base, kind: "audio" }, 0);
    this.rtpPacketsSentCounter.inc({ ...base, kind: "video" }, 0);
    this.rtpPacketsLostCounter.inc({ ...base, kind: "audio" }, 0);
    this.rtpPacketsLostCounter.inc({ ...base, kind: "video" }, 0);
    this.rtcpPliCounter.inc(base, 0);
    this.rtcpNackCounter.inc(base, 0);
    this.udpReceiveErrorsCounter.inc(base, 0);
  }

  private async collectOnce(): Promise<void> {
    if (this.pollingInFlight) {
      return;
    }
    this.pollingInFlight = true;

    try {
      const base = { node_id: this.config.nodeId, role: this.config.role };
      const snapshot = await this.mediaManager.getTelemetrySnapshot();

      this.activeTransportsGauge.set(
        { ...base, direction: "send" },
        snapshot.sendTransports,
      );
      this.activeTransportsGauge.set(
        { ...base, direction: "recv" },
        snapshot.recvTransports,
      );
      this.dtlsConnectedGauge.set(base, snapshot.dtlsConnectedTransports);
      this.prewarmedSessionsGauge.set(base, snapshot.prewarmedCount);

      const producerCounts = { audio: 0, video: 0 };
      for (const producer of snapshot.producers) {
        producerCounts[producer.kind] += 1;
      }
      this.activeProducersGauge.set(
        { ...base, kind: "audio" },
        producerCounts.audio,
      );
      this.activeProducersGauge.set(
        { ...base, kind: "video" },
        producerCounts.video,
      );

      const consumerCounts = { audio: 0, video: 0 };
      for (const consumer of snapshot.consumers) {
        consumerCounts[consumer.kind] += 1;
      }
      this.activeConsumersGauge.set(
        { ...base, kind: "audio" },
        consumerCounts.audio,
      );
      this.activeConsumersGauge.set(
        { ...base, kind: "video" },
        consumerCounts.video,
      );

      const p99Ns = this.loopDelay.percentile(99);
      this.eventLoopLagGauge.set(
        base,
        Number.isFinite(p99Ns) ? Math.max(0, p99Ns / 1_000_000) : 0,
      );
      this.loopDelay.reset();

      if (snapshot.workerResourceUsage) {
        const nowMs = Date.now();
        const { ru_utime, ru_stime, ru_maxrss } = snapshot.workerResourceUsage;
        this.workerCpuUserGauge.set(base, ru_utime / 1000);
        this.workerCpuSystemGauge.set(base, ru_stime / 1000);
        this.workerMemoryRssGauge.set(base, ru_maxrss * 1024);

        if (this.prevWorkerCpu) {
          const wallDeltaMs = nowMs - this.prevWorkerCpu.sampledAtMs;
          const cpuDeltaMs =
            ru_utime -
            this.prevWorkerCpu.ruUtimeMs +
            (ru_stime - this.prevWorkerCpu.ruStimeMs);
          const ratio =
            wallDeltaMs > 0 ? Math.max(0, Math.min(2, cpuDeltaMs / wallDeltaMs)) : 0;
          this.workerCpuRatioGauge.set(base, ratio);
        }

        this.prevWorkerCpu = {
          ruUtimeMs: ru_utime,
          ruStimeMs: ru_stime,
          sampledAtMs: nowMs,
        };
      }

      await this.collectRtpStats(snapshot.producers, snapshot.consumers);
      await this.collectKernelUdpErrors();
    } finally {
      this.pollingInFlight = false;
    }
  }

  private async collectRtpStats(
    producers: mediasoup.types.Producer[],
    consumers: mediasoup.types.Consumer[],
  ): Promise<void> {
    const base = { node_id: this.config.nodeId, role: this.config.role };
    const maxJitterMs: Record<mediasoup.types.MediaKind, number> = {
      audio: 0,
      video: 0,
    };

    const liveProducerKeys = new Set<string>();
    for (const producer of producers) {
      if (producer.closed) {
        continue;
      }
      const clockRate = producer.kind === "video" ? 90_000 : 48_000;
      const stats = await producer.getStats().catch(() => []);
      for (const stat of stats) {
        const key = `${producer.id}:${stat.ssrc}`;
        liveProducerKeys.add(key);

        const jitterMs = (stat.jitter / clockRate) * 1000;
        if (jitterMs > maxJitterMs[producer.kind]) {
          maxJitterMs[producer.kind] = jitterMs;
        }

        const prev = this.prevProducerStats.get(key);
        if (prev) {
          const rxDelta = Math.max(0, stat.packetCount - prev.packetCount);
          const lostDelta = Math.max(0, stat.packetsLost - prev.packetsLost);
          const pliDelta = Math.max(0, stat.pliCount - prev.pliCount);
          const nackDelta = Math.max(0, stat.nackCount - prev.nackCount);

          if (rxDelta > 0) {
            this.rtpPacketsReceivedCounter.inc(
              { ...base, kind: producer.kind },
              rxDelta,
            );
          }
          if (lostDelta > 0) {
            this.rtpPacketsLostCounter.inc(
              { ...base, kind: producer.kind },
              lostDelta,
            );
          }
          if (pliDelta > 0) {
            this.rtcpPliCounter.inc(base, pliDelta);
          }
          if (nackDelta > 0) {
            this.rtcpNackCounter.inc(base, nackDelta);
          }
        }

        this.prevProducerStats.set(key, {
          packetCount: stat.packetCount,
          packetsLost: stat.packetsLost,
          pliCount: stat.pliCount,
          nackCount: stat.nackCount,
        });
      }
    }
    this.pruneMap(this.prevProducerStats, liveProducerKeys);

    const liveConsumerKeys = new Set<string>();
    for (const consumer of consumers) {
      if (consumer.closed) {
        continue;
      }
      const clockRate = consumer.kind === "video" ? 90_000 : 48_000;
      const stats = await consumer.getStats().catch(() => []);
      for (const stat of stats) {
        if (stat.type !== "outbound-rtp") {
          continue;
        }
        const key = `${consumer.id}:${stat.ssrc}`;
        liveConsumerKeys.add(key);

        const jitterMs = (stat.jitter / clockRate) * 1000;
        if (jitterMs > maxJitterMs[consumer.kind]) {
          maxJitterMs[consumer.kind] = jitterMs;
        }

        const prev = this.prevConsumerStats.get(key);
        if (prev) {
          const txDelta = Math.max(0, stat.packetCount - prev.packetCount);
          const lostDelta = Math.max(0, stat.packetsLost - prev.packetsLost);
          const pliDelta = Math.max(0, stat.pliCount - prev.pliCount);
          const nackDelta = Math.max(0, stat.nackCount - prev.nackCount);

          if (txDelta > 0) {
            this.rtpPacketsSentCounter.inc(
              { ...base, kind: consumer.kind },
              txDelta,
            );
          }
          if (lostDelta > 0) {
            this.rtpPacketsLostCounter.inc(
              { ...base, kind: consumer.kind },
              lostDelta,
            );
          }
          if (pliDelta > 0) {
            this.rtcpPliCounter.inc(base, pliDelta);
          }
          if (nackDelta > 0) {
            this.rtcpNackCounter.inc(base, nackDelta);
          }
        }

        this.prevConsumerStats.set(key, {
          packetCount: stat.packetCount,
          packetsLost: stat.packetsLost,
          pliCount: stat.pliCount,
          nackCount: stat.nackCount,
        });
      }
    }
    this.pruneMap(this.prevConsumerStats, liveConsumerKeys);

    this.rtpJitterGauge.set({ ...base, kind: "audio" }, maxJitterMs.audio);
    this.rtpJitterGauge.set({ ...base, kind: "video" }, maxJitterMs.video);
  }

  private async collectKernelUdpErrors(): Promise<void> {
    try {
      const content = await fs.readFile("/proc/net/snmp", "utf8");
      const lines = content.split("\n");
      let headerCols: string[] | null = null;
      let valueCols: string[] | null = null;

      for (const line of lines) {
        if (!line.startsWith("Udp:")) {
          continue;
        }
        const cols = line.trim().split(/\s+/);
        if (!headerCols) {
          headerCols = cols;
        } else {
          valueCols = cols;
          break;
        }
      }

      if (!headerCols || !valueCols) {
        return;
      }

      const inErrorsIdx = headerCols.indexOf("InErrors");
      const rcvbufErrorsIdx = headerCols.indexOf("RcvbufErrors");

      const inErrors =
        inErrorsIdx >= 0 ? Number.parseInt(valueCols[inErrorsIdx] ?? "0", 10) : 0;
      const rcvbufErrors =
        rcvbufErrorsIdx >= 0
          ? Number.parseInt(valueCols[rcvbufErrorsIdx] ?? "0", 10)
          : 0;

      const total =
        (Number.isFinite(inErrors) ? inErrors : 0) +
        (Number.isFinite(rcvbufErrors) ? rcvbufErrors : 0);

      if (this.prevUdpErrorsTotal !== null && total > this.prevUdpErrorsTotal) {
        this.udpReceiveErrorsCounter.inc(
          { node_id: this.config.nodeId, role: this.config.role },
          total - this.prevUdpErrorsTotal,
        );
      }
      this.prevUdpErrorsTotal = total;
    } catch {
      // Non-Linux or restricted procfs: gracefully skip kernel UDP error polling
    }
  }

  private pruneMap<T>(map: Map<string, T>, liveKeys: Set<string>): void {
    for (const key of map.keys()) {
      if (!liveKeys.has(key)) {
        map.delete(key);
      }
    }
  }
}
