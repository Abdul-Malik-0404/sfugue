/**
 * Ingress Discrimination & Algorithmic "Death Signature" Detector.
 *
 * Implements the two core heuristics from CLAUDE.md:
 * 1. Ingress Discrimination (RQ2): Distinguishes organic SRTP/RTCP flash crowds
 *    from unauthenticated volumetric UDP floods by verifying valid DTLS
 *    session ratios (`dtlsConnectedTransports / activeTransports`) alongside
 *    RTCP PLI/NACK pressure before permitting migration.
 * 2. Pre-Crash Death Signature (RQ1): Detects correlated progression across
 *    Jitter > 40ms, kernel UDP drops, PLI/NACK spike, and CPU saturation across
 *    consecutive polls, gated by a failover cooldown timer to avoid thrashing.
 */

import type { WatcherConfig } from "./config";
import type { NodeTelemetrySnapshot } from "./prometheusClient";

export type ClassificationVerdict =
  | "HEALTHY"
  | "UNAUTHENTICATED_UDP_FLOOD"
  | "ORGANIC_DEGRADATION_PENDING"
  | "TRIGGER_FAILOVER"
  | "COOLDOWN_SUPPRESSED";

export interface DetectionOutcome {
  verdict: ClassificationVerdict;
  validDtlsSessionRatio: number;
  activeSignals: string[];
  consecutiveOrganicHits: number;
  reason: string;
}

export interface DetectorCounters {
  healthy: number;
  organicFailoversTriggered: number;
  udpFloodsSuppressed: number;
  cooldownSuppressed: number;
}

export class DeathSignatureDetector {
  private readonly config: WatcherConfig;
  private consecutiveOrganicHits = 0;
  private lastTriggerMs = 0;

  private readonly counters: DetectorCounters = {
    healthy: 0,
    organicFailoversTriggered: 0,
    udpFloodsSuppressed: 0,
    cooldownSuppressed: 0,
  };

  constructor(config: WatcherConfig) {
    this.config = config;
  }

  public evaluate(
    snapshot: NodeTelemetrySnapshot,
    nowMs = Date.now(),
  ): DetectionOutcome {
    const validDtlsSessionRatio =
      snapshot.activeTransports > 0
        ? Math.min(1, snapshot.dtlsConnectedTransports / snapshot.activeTransports)
        : 0;

    const rtcpSpike =
      snapshot.pliRatePerSec >= this.config.pliRateThresholdPerSec ||
      snapshot.nackRatePerSec >= this.config.nackRateThresholdPerSec;
    const jitterSpike = snapshot.jitterMs > this.config.jitterThresholdMs;
    const cpuSaturated = snapshot.workerCpuRatio >= this.config.cpuRatioThreshold;
    const udpDropsSpiking =
      snapshot.udpDropRatePerSec >= this.config.udpDropRateThresholdPerSec;

    // 1. Ingress discrimination: high kernel UDP drops without authenticated
    //    DTLS sessions (or with zero active organic transports & no RTCP signal)
    //    indicates a volumetric UDP flood rather than an organic call surge.
    const lacksAuthenticatedSessions =
      validDtlsSessionRatio < this.config.minValidDtlsRatio;
    const belowOrganicTransportFloor =
      snapshot.activeTransports < this.config.minActiveTransportsForOrganic &&
      !rtcpSpike;

    if (udpDropsSpiking && (lacksAuthenticatedSessions || belowOrganicTransportFloor)) {
      this.consecutiveOrganicHits = 0;
      this.counters.udpFloodsSuppressed += 1;
      return {
        verdict: "UNAUTHENTICATED_UDP_FLOOD",
        validDtlsSessionRatio,
        activeSignals: ["udp_drops_without_valid_dtls"],
        consecutiveOrganicHits: 0,
        reason: `Suppressed UDP flood: udpDrops=${snapshot.udpDropRatePerSec.toFixed(1)}/s, dtlsRatio=${validDtlsSessionRatio.toFixed(2)}, activeTransports=${snapshot.activeTransports}`,
      };
    }

    // 2. Correlated organic death-signature scoring over authenticated sessions
    const activeSignals: string[] = [];
    if (jitterSpike) {
      activeSignals.push(`jitter=${snapshot.jitterMs.toFixed(1)}ms`);
    }
    if (udpDropsSpiking) {
      activeSignals.push(`udpDrops=${snapshot.udpDropRatePerSec.toFixed(1)}/s`);
    }
    if (rtcpSpike) {
      activeSignals.push(
        `rtcp(pli=${snapshot.pliRatePerSec.toFixed(1)}/s,nack=${snapshot.nackRatePerSec.toFixed(1)}/s)`,
      );
    }
    if (cpuSaturated) {
      activeSignals.push(`cpuRatio=${snapshot.workerCpuRatio.toFixed(2)}`);
    }

    const organicSessionPresent =
      snapshot.activeTransports >= this.config.minActiveTransportsForOrganic &&
      validDtlsSessionRatio >= this.config.minValidDtlsRatio;

    const deathSignatureMatched =
      organicSessionPresent &&
      (activeSignals.length >= 2 || (cpuSaturated && rtcpSpike));

    if (!deathSignatureMatched) {
      this.consecutiveOrganicHits = 0;
      this.counters.healthy += 1;
      return {
        verdict: "HEALTHY",
        validDtlsSessionRatio,
        activeSignals,
        consecutiveOrganicHits: 0,
        reason: "Telemetry within healthy bounds",
      };
    }

    this.consecutiveOrganicHits += 1;

    if (this.consecutiveOrganicHits < this.config.consecutiveHitsRequired) {
      return {
        verdict: "ORGANIC_DEGRADATION_PENDING",
        validDtlsSessionRatio,
        activeSignals,
        consecutiveOrganicHits: this.consecutiveOrganicHits,
        reason: `Organic degradation sample ${this.consecutiveOrganicHits}/${this.config.consecutiveHitsRequired}: [${activeSignals.join(", ")}]`,
      };
    }

    // 3. Cooldown guard prevents migration thrashing after an evacuation fires
    if (
      this.lastTriggerMs > 0 &&
      nowMs - this.lastTriggerMs < this.config.failoverCooldownMs
    ) {
      this.counters.cooldownSuppressed += 1;
      return {
        verdict: "COOLDOWN_SUPPRESSED",
        validDtlsSessionRatio,
        activeSignals,
        consecutiveOrganicHits: this.consecutiveOrganicHits,
        reason: `Cooldown active (${nowMs - this.lastTriggerMs}ms < ${this.config.failoverCooldownMs}ms)`,
      };
    }

    this.lastTriggerMs = nowMs;
    this.consecutiveOrganicHits = 0;
    this.counters.organicFailoversTriggered += 1;

    return {
      verdict: "TRIGGER_FAILOVER",
      validDtlsSessionRatio,
      activeSignals,
      consecutiveOrganicHits: 0,
      reason: `Confirmed organic death signature: [${activeSignals.join(", ")}]`,
    };
  }

  public getCounters(): DetectorCounters {
    return { ...this.counters };
  }
}
