# sfugue Development Context

## Project Overview
Empirical benchmarking of failover latency and E2EE (SFrame) overhead in distributed WebRTC/Mediasoup architectures.

## Tech Stack
- **Language:** TypeScript / Node.js (v20+)
- **Orchestration:** Kubernetes (KinD) / Docker
- **Media Server:** Mediasoup v3 (C++ worker processes)
- **Control Plane:** Apache Kafka, Prometheus (`/metrics`), Chaos Mesh

## Key Commands
- Build SFU node: `cd sfu-node && npm run build`
- Run SFU locally: `npx tsx src/server.ts`
- Create KinD cluster: `kind create cluster --name sfu-testbed --config infra/kind-config.yaml`
