---
tags:
  - fyp
  - architecture
  - webrtc
  - distributed-systems
  - cloud-native
aliases:
  - sfugue Glossary
  - Technical Concepts
date: 2026-10-20
---

# `sfugue` Core Technical Concepts & Glossary

## 1. SFU Failure Patterns & The "Death Signature"
Selective Forwarding Units (SFUs) like Mediasoup follow cascading degradation patterns rather than crashing instantaneously under load.

### Key Cascading Failure Mechanisms:
1. **Keyframe (PLI/FIR) Storm Death Spiral:** Minor packet drops cause client decoders to lose sync, spamming the SFU with Picture Loss Indication (PLI) packets. Because keyframes are 5–10× larger than delta frames, duplicating them across subscribers causes an immediate egress CPU and bandwidth spike.
2. **Kernel Socket Buffer Overflow & NACK Flooding:** Single-threaded C++ workers experience event-loop starvation, overflowing Linux kernel UDP receive buffers. Dropped packets prompt NACK retransmission requests, overwhelming worker CPU cycles.
3. **Thundering Herd Handshakes:** Simultaneous peer reconnections or ICE restarts trigger heavy asymmetric cryptographic handshakes (DTLS), starving active media forwarding.
4. **Simulcast/SVC Layer Thrashing:** Rapid oscillation between video quality layers compounds RTP header-rewriting overhead.

### The Algorithmic "Death Signature" Progression (`[[RQ1]]`):
$$\text{Network Jitter} > 40\text{ ms} \longrightarrow \text{Kernel UDP Buffer Drops} \longrightarrow \text{Exponential PLI/NACK Spike} \longrightarrow \text{CPU Saturation } (100\%) \longrightarrow \text{Container Termination}$$

* **Organic Flash Crowd vs. UDP DDoS (`[[RQ2]]`):** Filtered by tracking valid DTLS/RTCP session ratios versus unauthenticated UDP packet drops at the socket boundary.

---

## 2. [[SFrame|SFrame (Secure Frame) & E2EE in WebRTC]]
* **Problem:** Standard DTLS-SRTP encrypts media in transit between client and SFU, but the SFU must decrypt packets to forward them, exposing plaintext media to the server.
* **Solution:** **SFrame (Secure Frame)** and W3C Insertable Streams provide true application-layer End-to-End Encryption (E2EE).
* **Mechanism:** Clients encrypt media payloads (VP8/Opus frames) using AES-GCM or ChaCha20-Poly1305 *before* transport packetization. SFU routers inspect only plaintext RTP headers to forward packets blindly without possessing decryption keys.
* **The `[[sfugue]]` Challenge:** Migrating clients to a standby node during a crash requires re-establishing transport connections (ICE restart) *and* forcing client crypto-workers to resynchronize rolling cryptographic **ratchets**, creating an unmeasured compound recovery blackout (`[[RQ3]]`, `[[RQ4]]`).

---

## 3. [[Prometheus|Prometheus (Telemetry & Observability)]]
* **Architecture:** An open-source, pull-based monitoring toolkit by the CNCF.
* **Operation:** Periodically **scrapes** plaintext performance metrics from service endpoints (e.g., `/metrics`).
* **Role in `sfugue`:** Continuously monitors container CPU utilization, memory I/O, kernel UDP buffer drops, active transport counts, and RTCP PLI frequencies from Mediasoup pods. It evaluates PromQL expressions to detect the pre-crash "death signature" and trigger proactive Kafka alerts.

---

## 4. [[Kubernetes|Kubernetes (K8s) & KinD]]
* **Kubernetes (K8s):** A container orchestrator that manages, scales, and heals Docker containers (`Pods`) across clusters. Essential for `sfugue` because tools like **Chaos Mesh** (fault injection) and **Prometheus** are native to K8s.
* **KinD (Kubernetes in Docker):** A lightweight tool that runs a full Kubernetes cluster *inside* a local Docker container on an Arch Linux machine. It provides full cloud-native parity without the heavy RAM/CPU overhead of local Virtual Machines.

---

## 5. [[Kafka|Apache Kafka (Distributed Message Broker)]]
* **Definition:** A high-throughput, append-only, distributed event streaming platform built on a publish-subscribe model.
* **Role in `sfugue`:** Acts as the Control Plane Message Broker. When Prometheus detects the telemetry death signature on Primary Pod A, it publishes a migration event to Kafka. Consumer listeners (standby pods and client orchestrators) consume the event, executing a proactive ICE restart *while Primary Pod A is still barely alive* to eliminate the cryptographic blackout.