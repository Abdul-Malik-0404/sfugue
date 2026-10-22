# sfugue

**Proactive Stateful Session Evacuation & E2EE Failover Benchmarking for Distributed WebRTC SFUs**

`sfugue` is an empirical chaos-engineering testbed designed to measure and minimize End-to-End Encryption (SFrame) cryptographic blackout latency during Selective Forwarding Unit (SFU) node failures.

## Architecture

- `infra/`: KinD cluster configuration, Kubernetes manifests (Mediasoup, Apache Kafka, Prometheus), and Chaos Mesh fault-injection policies.
- `sfu-node/`: Containerized TypeScript Mediasoup SFU worker exposing sub-minute Prometheus telemetry (`/metrics`) and Kafka control-plane consumers.
- `telemetry-watcher/`: Anomaly detection engine differentiating organic flash crowds from UDP floods to trigger proactive ICE restarts prior to pod termination.
- `load-gen/`: Isolated Playwright headless Chromium bots executing W3C WebRTC Encoded Transforms (SFrame AES-GCM) and capturing Time-to-First-Frame (TTFF) recovery timestamps.
- `analysis/`: Statistical evaluation scripts comparing Reactive vs. Proactive failover distributions.
