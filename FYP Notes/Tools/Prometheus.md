**Prometheus** is an open-source systems monitoring, metrics collection, and alerting toolkit originally built at SoundCloud and now a graduated project of the Cloud Native Computing Foundation (CNCF).
In modern cloud-native and [[Kubernetes]] environments, it is the de facto standard for observability.
### 1. How Prometheus Works (The Pull Model)
Unlike traditional monitoring systems where applications push data to a central server, Prometheus uses a **pull-based architecture**:
- Services (like your Mediasoup SFU node) expose an HTTP endpoint (traditionally `/metrics`) formatted as plain text containing key-value performance counters and gauges.
- At fixed intervals (e.g., every 5, 10, or 15 seconds), Prometheus actively **scrapes** those endpoints, gathers the data, and stores it in its internal **Time-Series Database (TSDB)**.
### 2. Core Components
- **Time-Series Data:** Every metric scraped by Prometheus is stored as a timestamped numerical value alongside labels (e.g., `sfugue_pli_requests_total{node_id="sfu-primary"}`).
- **PromQL (Prometheus Query Language):** A built-in functional query language that lets you analyze time-series data on the fly. You can calculate rates of change, standard deviations, and rolling averages (e.g., _“What is the per-second rate of PLI keyframe requests over the last 30 seconds?”_).
- **Alertmanager:** A companion service that takes alerts triggered by PromQL expressions and routes them to external systems (such as Slack, pagers, or in your project's case, an event broker).
### 3. Its Role in Your Project (`sfugue`)
In your testbed, Prometheus is the core engine responsible for detecting the pre-crash state:
1. It continuously scrapes CPU utilization, memory I/O, kernel UDP packet drops, active transport counts, and RTCP Picture Loss Indication (PLI) frequencies from your containerized Mediasoup pods.
2. It evaluates your defined **"death signature"** thresholds using PromQL.
3. The moment it detects the cascading pre-crash inflection point (e.g., a steep surge in PLI/NACK requests combined with rising socket buffer drops), it triggers an alert hook that sends a migration signal to Apache Kafka, prompting the system to execute a proactive ICE restart before the node collapses.