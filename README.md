# 🚀 Observability Sandbox: Payment Gateway Pipeline

A production-grade observability sandbox designed to monitor a containerized FastAPI payment gateway. This project demonstrates a "multi-signal" telemetry approach using modern industry-standard tools for logs, metrics, and business events.

## 🏗️ Architecture: Sandbox Version
This is the architecture currently running in this repository.

```mermaid
graph TD
    subgraph "Application Layer"
        App[FastAPI Payment App]
        LoadTest[Load Test Script]
    end

    subgraph "Ingestion & Routing (Vector)"
        Vector{Vector Central Hub}
        DockerLogs[Docker Log Source]
        HostMetrics[Host Metrics Source]
        OTLP[OTLP Metrics Source]
        KafkaEvents[Kafka Consumer Source]
    end

    subgraph "Data Pipeline"
        Kafka[Kafka Broker]
    end

    subgraph "Storage Layer"
        ClickHouse[(ClickHouse - Logs & Events)]
        VictoriaMetrics[(VictoriaMetrics - Metrics)]
    end

    subgraph "Visualization"
        Grafana[Grafana Dashboard]
    end

    %% Flows
    LoadTest -->|HTTP POST| App
    App -->|JSON Logs| DockerLogs
    App -->|Kafka Producer| Kafka
    App -->|OTLP/HTTP| OTLP
    
    Kafka -->|Events| KafkaEvents
    
    DockerLogs --> Vector
    HostMetrics --> Vector
    OTLP --> Vector
    KafkaEvents --> Vector

    Vector -->|SQL Insert| ClickHouse
    Vector -->|Prometheus Remote Write| VictoriaMetrics

    ClickHouse -->|SQL Query| Grafana
    VictoriaMetrics -->|PromQL| Grafana
```

---

## 🌎 Architecture: "LinkedIn" Scale (Future Growth)
How this system evolves when scaling to 10,000+ servers.

```mermaid
graph TD
    subgraph "Edge Tier (10,000+ Servers)"
        App[Microservices] -->|Log/Metric| Agent[Vector Agent - Rust]
    end

    subgraph "Messaging Backbone"
        Agent -->|Protobuf/Avro| KafkaCluster[Massive Kafka Cluster]
    end

    subgraph "Stream Processing"
        KafkaCluster -->|Raw Stream| FlinkFraud[Flink - Fraud Detection]
        KafkaCluster -->|Raw Stream| FlinkMetrics[Flink - Real-time Aggregations]
        FlinkMetrics -->|Derived Stats| KafkaProcessed[Kafka - Processed Events]
    end

    subgraph "Storage & OLAP"
        KafkaCluster -->|Batch Load| ClickHouse[(ClickHouse - Operational Logs)]
        KafkaCluster -->|Full Text Index| Elastic[Elasticsearch - Search]
        KafkaProcessed -->|Real-time Analytics| Pinot[(Apache Pinot - User Stats)]
        Agent -->|Direct Push| VM[(VictoriaMetrics - Global Metrics)]
    end

    subgraph "Visualization & Action"
        ClickHouse --> Grafana[Grafana - Ops]
        VM --> Grafana
        Pinot --> UserApp[User Profile Stats]
        FlinkFraud --> PagerDuty[Emergency Alerts]
    end
```

---

## 🚀 Scaling to the Enterprise: The Role of Apache Flink

As the system grows from a sandbox to a global platform (like LinkedIn or Uber), simple data routing isn't enough. This is where **Apache Flink** enters the pipeline as the "Stateful Brain."

### What Flink adds to the Pipeline:
While **Vector** is a high-speed router (Stateless), **Flink** is a complex processor (Stateful). It allows you to:

*   **Real-time Fraud Detection**: Identify suspicious patterns (e.g., "User paying from NYC and London within 5 minutes") by maintaining state across thousands of events.
*   **Complex Windowing**: Calculate rolling metrics that don't exist in the raw data, such as "Average Transaction Value per city over the last 15 minutes."
*   **Dynamic Alerting**: Trigger emergency notifications directly from the stream based on complex logic (e.g., "Alert if success rate drops below 95% only for users on iOS").
*   **Stream Joining**: Enrich incoming payment logs with user metadata from a separate "Customer Info" stream in real-time.

### The "LinkedIn Scale" Workflow:
1.  **Vector Agents** collect raw data at the edge.
2.  **Kafka** acts as the high-durability backbone.
3.  **Flink** consumes from Kafka, performs heavy mathematical computations and fraud checks, and writes the *processed* results back to a new Kafka topic.
4.  **Vector Aggregators** consume those final results and batch-load them into **ClickHouse** or **Apache Pinot** for high-speed visualization.

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **App** | FastAPI (Python) | High-performance payment gateway simulation. |
| **Data Router** | Vector (Rust) | Lightweight, fast telemetry collector and router. |
| **Messaging** | Apache Kafka | Event bus for decoupled business transaction events. |
| **Log Storage** | ClickHouse | OLAP database for blazing-fast log and event queries. |
| **Metric Storage** | VictoriaMetrics | High-efficiency storage for system and app metrics. |
| **Visualization** | Grafana | Unified dashboard for all telemetry signals. |

## 🚀 Getting Started

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.9+ (for load testing)

### 2. Spin up the Stack
```bash
docker compose up -d
```

### 3. Generate Traffic
Run the load test to see data flow into the dashboard:
```bash
python load_test.py
```

### 4. Access the Command Center
- **Grafana**: [http://localhost:3000](http://localhost:3000) (Admin / Admin)
- **ClickHouse Play**: [http://localhost:8123/play](http://localhost:8123/play)
- **VictoriaMetrics**: [http://localhost:8428](http://localhost:8428)

## 📊 Key Features
- **Multi-Signal Monitoring**: Correlate business events (Kafka) with operational logs (ClickHouse) and system metrics (VictoriaMetrics).
- **VRL Transformation**: Vector Remap Language used for real-time log sanitization and schema enforcement.
- **ARM64 Optimized**: Specifically configured to run smoothly on Apple Silicon (M1/M2/M3) using stable community plugins.
- **Provisioned Infrastructure**: Dashboard and data sources are pre-configured via code for instant deployment.

## 🤝 Troubleshooting
- **No Logs?**: Ensure the time range in Grafana is set to "Last 3 hours" or "Last 6 hours" to account for UTC/Local time differences.
- **Connection Refused?**: Kafka takes ~30 seconds to fully initialize its listeners. Vector will automatically retry.

---
*Built with ❤️ for modern Observability.*
