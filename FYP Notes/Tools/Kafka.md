**Apache Kafka** is an open-source, distributed event streaming platform. In system architecture, it acts as a high-throughput, highly reliable **distributed message broker**.

Think of it as a real-time, industrial-grade conveyor belt for data. Instead of services calling each other directly (which creates tight coupling and fragility), services drop messages onto Kafka streams (topics), and other services pick them up whenever they are ready.
### 1. The Core Concepts of Kafka
To understand how Kafka works, you just need to know its four main building blocks:
- **Producer:** An application or service that _creates_ and sends messages (events) into Kafka.
- **Consumer:** An application or service that _subscribes_ to Kafka and reads those messages.
- **Topic:** A named category or feed where messages are stored (e.g., a topic named `sfu-health-events`). Producers write to topics, and consumers read from them.
- **Broker:** A Kafka server that stores the messages reliably, handles replication, and ensures data isn't lost even if a server crashes.
### 2. Why is Kafka Different from a Traditional Queue or Database?
- **Pub/Sub (Publish-Subscribe) Model:** Unlike a standard database where you write a record and update it, Kafka is **append-only**. Once data is written to a Kafka log, it is immutable (read-only) and persists for a set period of time.
- **Multiple Consumers Can Read the Same Data:** If Service A, Service B, and Service C all need to know when an SFU node is failing, Kafka allows all three of them to read the exact same stream of failure events independently without deleting the data.
- **Blazing Fast & Distributed:** Kafka is written in Java and Scala and is optimized for sequential disk I/O and zero-copy data transfer, allowing it to handle millions of messages per second with sub-millisecond latency.
### 3. Its Role in Your Project `sfugue`
In your distributed WebRTC failover architecture, Kafka serves as your **Control Plane Message Broker**:
1. **The Trigger:** Your Prometheus telemetry watcher detects the pre-crash "death signature" (sudden spike in PLI keyframe requests and kernel socket drops) on the primary Mediasoup pod.
2. **The Publish:** Prometheus (acting as a **Producer**) immediately publishes a migration event (e.g., `Node-A-Failing`) onto a Kafka topic.
3. **The Consume:** The standby Mediasoup node and client orchestration listeners (acting as **Consumers**) read that event instantly from Kafka.
4. **The Handover:** This triggers the client-side ICE restart and End-to-End Encryption (SFrame) state pre-negotiation with the standby pod _while the primary node is still barely alive_, completely bypassing the reactive crash penalty.
Because Kafka decouples your observability layer from your migration execution layer, your control plane remains fast, decoupled, and fault-tolerant.