/**
 * Strongly typed Prometheus HTTP API client polling `sfu-node` telemetry
 * series exported by `prom-client` at `/metrics`.
 */

export interface NodeTelemetrySnapshot {
  jitterMs: number;
  pliRatePerSec: number;
  nackRatePerSec: number;
  workerCpuRatio: number;
  udpDropRatePerSec: number;
  activeTransports: number;
  dtlsConnectedTransports: number;
  rtpPacketsRatePerSec: number;
  sampledAtMs: number;
}

interface PrometheusVectorSample {
  metric: Record<string, string>;
  value: [number, string];
}

interface PrometheusApiResponse {
  status: "success" | "error";
  data?: {
    resultType: "vector" | "matrix" | "scalar" | "string";
    result: PrometheusVectorSample[];
  };
  error?: string;
}

export class PrometheusClient {
  private readonly baseUrl: string;
  private readonly targetNodeId: string;

  constructor(baseUrl: string, targetNodeId: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.targetNodeId = targetNodeId;
  }

  public async fetchNodeSnapshot(): Promise<NodeTelemetrySnapshot> {
    const id = this.escapeLabelValue(this.targetNodeId);
    const [
      jitterMs,
      pliRatePerSec,
      nackRatePerSec,
      workerCpuRatio,
      udpDropRatePerSec,
      activeTransports,
      dtlsConnectedTransports,
      rtpPacketsRatePerSec,
    ] = await Promise.all([
      this.queryScalar(`max(sfugue_rtp_jitter_ms{node_id="${id}"})`),
      this.queryScalar(`sum(rate(sfugue_rtcp_pli_total{node_id="${id}"}[30s]))`),
      this.queryScalar(`sum(rate(sfugue_rtcp_nack_total{node_id="${id}"}[30s]))`),
      this.queryScalar(
        `max(sfugue_worker_cpu_utilization_ratio{node_id="${id}"})`,
      ),
      this.queryScalar(
        `sum(rate(sfugue_udp_socket_receive_errors_total{node_id="${id}"}[30s]))`,
      ),
      this.queryScalar(`sum(sfugue_active_transports{node_id="${id}"})`),
      this.queryScalar(
        `max(sfugue_dtls_connected_transports{node_id="${id}"})`,
      ),
      this.queryScalar(
        `sum(rate(sfugue_rtp_packets_received_total{node_id="${id}"}[30s]))`,
      ),
    ]);

    return {
      jitterMs,
      pliRatePerSec,
      nackRatePerSec,
      workerCpuRatio,
      udpDropRatePerSec,
      activeTransports,
      dtlsConnectedTransports,
      rtpPacketsRatePerSec,
      sampledAtMs: Date.now(),
    };
  }

  private async queryScalar(promql: string): Promise<number> {
    const url = `${this.baseUrl}/api/v1/query?query=${encodeURIComponent(promql)}`;
    const response = await fetch(url, {
      method: "GET",
      signal: AbortSignal.timeout(3000),
    });

    if (!response.ok) {
      throw new Error(
        `Prometheus query HTTP ${response.status} for "${promql}"`,
      );
    }

    const payload = (await response.json()) as PrometheusApiResponse;
    if (payload.status !== "success") {
      throw new Error(payload.error || `Prometheus error for "${promql}"`);
    }

    const first = payload.data?.result?.[0];
    if (!first) {
      return 0;
    }

    const parsed = Number(first.value[1]);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private escapeLabelValue(raw: string): string {
    return raw.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  }
}
