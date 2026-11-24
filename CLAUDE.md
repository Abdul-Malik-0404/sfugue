# `sfugue` — Architectural Contract & Development Guide

> **Canonical Contract:** `CLAUDE.md` is the single source of truth for all coding agents and contributors (`GEMINI.md` symlinks directly to this file so Antigravity and Claude Code enforce identical rules). Every rule marked **NON-NEGOTIABLE** MUST be satisfied before code or manifests are accepted.

---

## 1. Project Overview & Research Scope

**`sfugue`** (SFU + musical *fugue* for uninterrupted transitions) is a Final Year Project (FYP) empirical chaos-engineering testbed for **Proactive Stateful Session Evacuation & E2EE (SFrame) Failover Benchmarking in Distributed WebRTC SFUs**.

### Three Coupled WebRTC State Layers Under Study
1. **Transport State (OS / Network Layer):** Persistent UDP sockets, NAT bindings, ICE candidate pairs, and DTLS sessions.
2. **Media Pipeline State (Application Layer):** Temporal video codec state (VP8 delta frames requiring an IDR keyframe via RTCP PLI/FIR upon server switch).
3. **Cryptographic Ratchet State (E2EE Security Layer):** Application-layer SFrame (W3C Insertable Streams / WebRTC Encoded Transforms) rolling cryptographic ratchets managed exclusively on clients under a zero-trust SFU model.

### Research Framing & Evaluation Metrics (Viva Alignment)
- Frame architectural choices as **Candidate Technologies** backed by empirical benchmarking and documented fallback paths (never "because it's easier").
- Frame the research gap accurately: **lightweight pre-crash heuristics for stateful WebRTC SFUs** lack empirical E2EE failover benchmarks compared to heavy ML/neural cloud outage predictors.
- **Algorithmic "Death Signature" Progression:**
  `Network Jitter > 40ms -> Kernel UDP Buffer Drops -> Exponential PLI/NACK Spike -> CPU Saturation (100%) -> Pod Termination`
- **Ingress Discrimination:** Differentiate organic flash crowds from unauthenticated UDP floods by tracking valid DTLS/RTCP session ratios against socket-level UDP drops.
- **Required Evaluation Metrics:**
  1. **TTFF (Time-to-First-Frame)** recovery latency delta: Reactive baseline vs. Proactive Kafka-triggered handover.
  2. **Isolated SFrame Cryptographic Tax:** Additional blackout latency introduced by E2EE ratchet resynchronization vs. unencrypted media.
  3. **MTTD (Mean Time to Detect)** pre-crash signature prior to pod collapse.
  4. **Confusion Matrix (TP/FP/TN/FN)** for flash-crowd vs. UDP-flood classification.
  5. **False-Positive Migration Overhead:** Wasted CPU/RAM when a false alarm triggers unnecessary evacuation.
  6. **E2EE Media Resumption Success Rate:** Percentage of evacuated streams resuming cleanly without frame/decryption corruption.

---

## 2. Existing Codebase Context (Audited Ground Truth)

### Repository Directory Layout
- `sfu-node/` — Active TypeScript Mediasoup SFU prototype (`src/server.ts`, `index.html`, `package.json`, `tsconfig.json`, `Dockerfile`).
- `infra/k8s/`, `infra/chaos/` — Reserved for KinD configs, Kubernetes manifests, and Chaos Mesh policies (currently empty).
- `telemetry-watcher/` — Reserved for Prometheus death-signature detection & ingress discrimination engine (currently empty).
- `load-gen/` — Reserved for headless Playwright Chromium + SFrame load generator & TTFF harness (currently empty).
- `analysis/` — Reserved for statistical evaluation scripts (currently empty).
- `FYP Notes/` — Obsidian research vault (`Problem.md`, `Tasks.md`, `SFUGUE Architecture Glossary.md`, `Supervisor Meeting Notes/`, `Tools/`).

### Ports & Network Configuration (`sfu-node/src/server.ts` & `sfu-node/Dockerfile`)
| Surface | Current Implementation | Notes / Future Alignment |
| :--- | :--- | :--- |
| HTTP + Socket.IO Signaling | `process.env.PORT \|\| 3000` (TCP `3000`), CORS `{ origin: "*" }` | Served by `express` + `http.createServer(app)` + `socket.io` v4. |
| Static Assets | `app.use(express.static("public"))` | `sfu-node/index.html` currently resides at `sfu-node/index.html`, **not** `sfu-node/public/index.html`. |
| Mediasoup RTC Media Ports | Unconstrained in `server.ts` (Mediasoup defaults to `10000–59999`) | `sfu-node/Dockerfile` declares `EXPOSE 3000 40000-40010/udp`. Parameterize `rtcMinPort` / `rtcMaxPort` via env vars to match K8s/KinD mappings. |
| Transport `listenIps` | `[{ ip: "0.0.0.0", announcedIp: "127.0.0.1" }]` | Hardcoded loopback; MUST be driven by `ANNOUNCED_IP` env var in containers/KinD. |
| Transport Bitrate & Protocol | `enableUdp: true`, `enableTcp: true`, `preferUdp: true`, `initialAvailableOutgoingBitrate: 1_000_000`, `maxIncomingBitrate: 1_500_000` | Applied in `createWebRtcTransport()`. |
| Prometheus Metrics | Not yet wired in `server.ts` (`prom-client` is installed) | Target endpoint: `GET /metrics` (either on signaling port or dedicated metrics port). |

### Mediasoup Worker & Router Setup (`sfu-node/src/server.ts`)
- **Worker:** Single C++ subprocess via `mediasoup.createWorker({ logLevel: "warn" })` started inside `startMediasoup()`, invoked from the `server.listen(PORT, async () => ...)` callback.
- **Crash Handler:** `worker.on("died")` logs `worker.pid` and terminates the Node process (`process.exit(1)`) after a `2000` ms timeout.
- **Router Codecs (`mediaCodecs`):**
  1. **Audio:** `kind: "audio"`, `mimeType: "audio/opus"`, `clockRate: 48000`, `channels: 2`
  2. **Video:** `kind: "video"`, `mimeType: "video/VP8"`, `clockRate: 90000`, `parameters: { "x-google-start-bitrate": 1000 }`
- **Current Prototype State Scope:** Module-level singletons (`worker`, `router`, `producerTransport`, `consumerTransport`, `producer`, `consumer`) supporting only a single local loopback session. No multi-room/multi-peer `Map` registry or resource teardown on `disconnect` exists yet.

### Socket.IO Signaling Protocol (`sfu-node/src/server.ts` <-> `sfu-node/index.html`)
All custom events currently use Socket.IO acknowledgment callbacks:

| Order | Event Name | Direction | Client Payload | Server Ack Response | Behavior |
| :---: | :--- | :--- | :--- | :--- | :--- |
| 0 | `connection` / `disconnect` | Lifecycle | — | — | Logs `socket.id` only (no transport/producer cleanup yet). |
| 1 | `getRouterRtpCapabilities` | Client -> SFU | *(none)* | `RtpCapabilities` | Returns `router.rtpCapabilities` for `device.load()`. |
| 2 | `createProducerTransport` | Client -> SFU | *(none)* | `{ id, iceParameters, iceCandidates, dtlsParameters }` \| `{ error }` | Creates send `WebRtcTransport` via `createWebRtcTransport()`. |
| 3 | `connectProducerTransport` | Client -> SFU | `{ dtlsParameters }` | `()` | Calls `producerTransport?.connect({ dtlsParameters })`. |
| 4 | `produce` | Client -> SFU | `{ kind, rtpParameters }` | `{ id }` | Calls `producerTransport?.produce({ kind, rtpParameters })`. |
| 5 | `createConsumerTransport` | Client -> SFU | *(none)* | `{ id, iceParameters, iceCandidates, dtlsParameters }` \| `{ error }` | Creates recv `WebRtcTransport` via `createWebRtcTransport()`. |
| 6 | `connectConsumerTransport` | Client -> SFU | `{ dtlsParameters }` | `()` | Calls `consumerTransport?.connect({ dtlsParameters })`. |
| 7 | `consume` | Client -> SFU | `{ rtpCapabilities }` | `{ id, producerId, kind, rtpParameters }` \| `{ error }` | Checks `router.canConsume(...)`; creates consumer with `paused: true`. |
| 8 | `resume` | Client -> SFU | *(none)* | `()` | Calls `consumer?.resume()` to start media flow. |

### Local Loopback Client (`sfu-node/index.html`)
- Loads `/socket.io/socket.io.js` and `mediasoup-client@3` UMD from jsDelivr (`https://cdn.jsdelivr.net/npm/mediasoup-client@3/umd/mediasoup-client.min.js`).
- UI controls: `#localVideo` (muted autoplay), `#remoteVideo` (autoplay), `#startBtn`.
- Clicking `#startBtn` requests video-only webcam media (`getUserMedia({ video: true })`), executes Steps 1–8 above using `mediasoupClient.Device`, attaches `consumer.track` to `#remoteVideo`, and emits `resume`. Does not yet use W3C Insertable Streams / SFrame.

### Toolchain & Audited Baseline Gaps in `sfu-node/`
- **Installed Dependencies (`sfu-node/package.json`, `type: "commonjs"`):** `express@^5.3.0`, `socket.io@^4.8.4`, `mediasoup@^3.30.1`, `prom-client@^15.1.3`, `kafkajs@^2.2.4`, `ws@^8.22.0`, `cors@^2.8.6`, plus `typescript@^7.0.2` and `tsx@^4.23.15`.
- **TypeScript Config (`sfu-node/tsconfig.json`):** `target: "ES2022"`, `module: "CommonJS"`, `rootDir: "./src"`, `outDir: "./dist"`, `strict: true`, `esModuleInterop: true`, `skipLibCheck: true`.
- **Known Baseline Issues (fix when refactoring `sfu-node/`):**
  1. `npx tsc --noEmit` currently fails with 5 errors in `src/server.ts`: deep imports (`mediasoup/node/lib/Router`, `Worker`, `WebRtcTransport`) trigger `TS2307` under `mediasoup@^3.30.1` (migrate to `mediasoup.types.*`), and `mediaCodecs` is typed as `RtpCodecCapability[]` instead of `RouterRtpCodecCapability[]` (`TS2741` missing `preferredPayloadType`).
  2. `sfu-node/package.json` lacks `"build"`, `"start"`, and `"dev"` npm scripts (only placeholder `"test"` exists).
  3. `sfu-node/Dockerfile` (`node:20-bullseye-slim`) has no `.dockerignore`, risking copying local `node_modules/` and `worker/` binaries during builds.

---

## 3. Candidate Tech Stack & Modular Broker Contract (NON-NEGOTIABLE)

| Layer | Candidate Technology | Architectural Requirement |
| :--- | :--- | :--- |
| **Language / Runtime** | TypeScript (`strict: true`) / Node.js v20+ | Shared typed contracts across `sfu-node`, `telemetry-watcher`, and `load-gen`. |
| **Media Plane (SFU)** | Mediasoup v3 (C++ worker subprocesses) | Blind RTP forwarding only; 1 worker per constrained pod. |
| **Observability** | Prometheus (`prom-client` `/metrics`) | Sub-minute scrape intervals tracking CPU, memory, kernel UDP drops, active transports, RTCP PLI/NACK, and jitter. |
| **Control Plane Broker** | **Apache Kafka (KRaft mode — NO ZooKeeper)** via `kafkajs` | **MUST** be encapsulated behind a modular broker interface so **Redis (Pub/Sub or Streams)** can act as a drop-in fallback without touching SFU or watcher domain logic. |
| **Orchestration & Chaos** | KinD (Kubernetes in Docker) + Chaos Mesh | Declarative YAML manifests under `infra/k8s/` and `infra/chaos/`. |
| **Load & E2EE Harness** | Playwright Chromium + W3C Insertable Streams (SFrame AES-GCM) | Synthetic encrypted WebRTC peers measuring TTFF, ratchet resync delta, and frame integrity. |

### Modular Broker Interface Mandate
All control-plane messaging MUST depend on an abstract interface (e.g., `IControlPlaneBroker` / `ControlPlaneBroker`) rather than importing `kafkajs` directly in business logic:
- Core methods: `connect()`, `publish<T>(topic, message, key?)`, `subscribe<T>(topic, groupId, handler)`, `healthCheck()`, `disconnect()`.
- Concrete adapters: `KafkaKraftBroker` (primary candidate) and `RedisBroker` (drop-in fallback), selected via configuration (`CONTROL_PLANE_BROKER=kafka|redis`).
- This guarantees empirical Kafka vs. Redis failover latency comparisons and provides a clean viva contingency if Kafka latency or memory overhead proves prohibitive.

---

## 4. Security Boundary — Zero-Trust Control Plane (NON-NEGOTIABLE)

The SFU media plane and Kafka/Redis control plane operate strictly under **Zero-Trust**.

### Permitted on Kafka / Control-Plane Broker (ALLOW-LIST ONLY)
Kafka (and any Redis fallback) may **ONLY** transfer serialized JSON WebRTC routing and orchestration metadata:
- Session/topology identifiers: `roomId`, `peerId`, `sourceNodeId`, `targetNodeId`, `correlationId`, `timestampMs`.
- RTP routing metadata: `producerId`, `consumerId`, `kind` (`"audio" | "video"`), `ssrc`, `rtxSsrc`, `mid`, non-secret `rtpParameters` / codec metadata.
- Transport handshake descriptors: `iceParameters`, `iceCandidates`, `dtlsParameters` (public fingerprints & DTLS role only — never private keys).
- Failover control signals: migration reason/trigger, target standby endpoint, schema version.

### Strictly Prohibited (HARD SECURITY VIOLATION)
- **NEVER** transmit, serialize, cache, or log **SFrame E2EE encryption keys**, base secrets, epoch/ratchet keys, IV salts, or decrypted media frames over Kafka, Redis, Prometheus `/metrics`, Socket.IO server logs, or SFU memory.
- All SFrame key derivation, key ratcheting, and AES-GCM payload encryption/decryption MUST execute **strictly client-side** inside browser/Playwright W3C Insertable Streams transform workers.
- Outbound broker publishers MUST validate/sanitize payloads against explicit allow-listed TypeScript DTOs so secret fields cannot leak accidentally.

---

## 5. Host Hardware Constraints (NON-NEGOTIABLE)

All local development, KinD cluster runs, and load benchmarks execute on:
- **OS:** Arch Linux
- **RAM:** **8 GB total system memory**
- **CPU:** **11th Gen Intel Core i3** (resource-constrained dual-core / 4-thread class laptop)

### Mandatory Kubernetes & Infrastructure Rules
1. **Strict Resource Limits Everywhere:** EVERY container in EVERY Kubernetes manifest (Mediasoup Primary, Mediasoup Standby, Kafka KRaft, Prometheus, Telemetry Watcher, Chaos Mesh workloads) **MUST** explicitly define both `resources.requests` and `resources.limits` for `cpu` and `memory`. Unbounded pods are forbidden.
2. **KRaft-Only Kafka:** Deploy Apache Kafka strictly in **single-node KRaft mode** (zero ZooKeeper containers) with explicitly bounded JVM heap (`KAFKA_HEAP_OPTS`, e.g., `-Xms256m -Xmx384m` to stay within container memory limits).
3. **Bounded Prometheus TSDB:** Configure short retention windows/size caps (e.g., `--storage.tsdb.retention.time=2h`, `--storage.tsdb.retention.size=256MB`) and tight memory limits so scraping never causes host OOM or swap thrashing.
4. **Cluster Memory Headroom:** Keep total KinD pod `resources.limits` conservatively budgeted (targeting <= ~4.5 GB aggregate) to preserve headroom for Arch Linux, Docker daemon, and Playwright Chromium processes.
5. **Deterministic Mediasoup Footprint:** Run **1 Mediasoup C++ worker per pod** and bind `rtcMinPort`/`rtcMaxPort` to a narrow UDP port window matching KinD `extraPortMappings`.

### Mandatory Playwright Load Generator Rules
1. **Headless by Default:** Playwright bots MUST default to `headless: true` (configurable via CLI flag / env var for viva visual demos) and pass resource-conscious Chromium flags (e.g., `--use-fake-ui-for-media-stream`, `--use-fake-device-for-media-stream`, `--disable-gpu`, `--disable-dev-shm-usage`).
2. **CPU Core Pinning:** Load test launchers MUST support explicit **CPU core pinning** (e.g., via `taskset -c <core-list>` / configurable `BOT_CPU_CORES` env var or CLI flag) so Chromium encoding/crypto load can be isolated from KinD/Mediasoup worker cores, preventing observer-effect measurement skew.
3. **Bounded Concurrency & Clean Teardown:** Enforce configurable max bot concurrency and always close `BrowserContext` / `Browser` instances on `SIGINT`/`SIGTERM` or test failure so orphaned Chromium processes never exhaust 8 GB RAM.

---

## 6. Code Standards & Live Viva Readiness (NON-NEGOTIABLE)

1. **Modular Design:** Keep modules small and cohesive with strict separation of concerns (`config`, `signaling`, `mediasoup`, `telemetry`, `broker`, `types`). Do not accumulate all logic into `src/server.ts`.
2. **Strong Typing:** Enable TypeScript `strict: true` across all TS packages. Avoid `any` in public APIs, Socket.IO handlers, Prometheus metric registries, and Kafka message schemas. Use versioned, strongly typed interfaces/discriminated unions for all wire events.
3. **Viva-Ready Comments:** Write clear, concise comments explaining the **architectural rationale ("why")** behind critical code paths—specifically mapping code to the 3-tier WebRTC state model, pre-crash death signature, Zero-Trust SFrame boundary, and failover latency measurement points—so any file can be walked through live during a viva examination.
4. **Configuration Hygiene:** Externalize all ports, IPs (`ANNOUNCED_IP`), broker URLs, Kafka topics, and anomaly thresholds to typed config modules backed by environment variables. Never commit secrets (`.env`), compiled Mediasoup worker binaries (`sfu-node/worker/`), `node_modules/`, or `dist/`.
5. **Lifecycle & Fault Hygiene:** Every service must implement graceful shutdown (`SIGTERM`/`SIGINT`), cleanly closing Mediasoup transports/workers, Socket.IO connections, timers, and broker clients.

---

## 7. Key Commands

```bash
# Type-check SFU node (without emitting)
cd sfu-node && npx tsc --noEmit

# Compile SFU node TypeScript -> dist/
cd sfu-node && npx tsc

# Run SFU node locally in dev mode
cd sfu-node && npx tsx src/server.ts

# Create / delete local KinD cluster (once infra/kind-config.yaml is scaffolded)
kind create cluster --name sfu-testbed --config infra/kind-config.yaml
kind delete cluster --name sfu-testbed
```
