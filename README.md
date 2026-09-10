# Cloud-Native Streaming Data Pipeline 🛰️📊

A Dockerized, Kafka + Spark Structured Streaming pipeline that ingests simulated telecom server logs, computes live windowed metrics, and serves them over a REST API with a live browser dashboard.

## 📋 Description

This project simulates a telecom server fleet emitting logs (CPU usage, latency, status, server ID), streams them through Apache Kafka, processes them in real time with Spark Structured Streaming using 10-second tumbling windows, and pushes the aggregated metrics to a FastAPI microservice. The metrics can then be viewed live in a browser or queried directly through the REST API. The entire stack runs with a single `docker-compose up` command.

## 🎯 Project Objectives

1. **Stream Data at Scale**: Ingest continuous telecom log events through Kafka
2. **Process in Real Time**: Aggregate metrics using Spark Structured Streaming with watermarking
3. **Expose Metrics via REST**: Serve live aggregated metrics through a FastAPI microservice
4. **Containerize the Full Stack**: Run producer, broker, processing, and API as isolated Docker services

## ✨ Features

- **Kafka Message Broker**: Single-node Kafka (KRaft mode, no ZooKeeper) with automatic topic initialization
- **Synthetic Log Producer**: Continuously generates randomized server logs (CPU usage, latency, status, timestamp)
- **Spark Structured Streaming**: Parses JSON events and computes windowed aggregates with a 10-second watermark
- **REST Metrics API**: FastAPI service that receives windowed metrics from Spark and serves them as JSON
- **Live Browser Dashboard**: Auto-refreshing web view of the metrics, served directly by the API
- **Fully Containerized**: Every component runs as its own Docker service

## 📊 Computed Metrics

For each 10-second window, the pipeline calculates:

- Average CPU usage
- Average latency (ms)
- Total event count
- Error count and error rate
- Unique server count (approximate distinct)

## 🏗️ Architecture

```
Producer (Python) → Kafka (telecom_logs topic) → Spark Structured Streaming → Metrics API (FastAPI) → Browser Dashboard
```

- **producer**: Publishes randomized JSON log events to the `telecom_logs` Kafka topic every second
- **kafka / kafka-init**: Runs the broker and creates the topic on startup
- **spark**: Consumes the topic, parses and aggregates events in tumbling windows, then POSTs results to the metrics API
- **metrics-api**: Receives and stores the latest metrics windows, exposing them via REST endpoints and a live browser dashboard

## 🛠️ Tech Stack

- **Apache Kafka** (KRaft mode) — message broker
- **Apache Spark** (Structured Streaming) — real-time processing
- **FastAPI** — REST metrics microservice
- **Python** — producer script
- **Docker & Docker Compose** — containerization and orchestration

## 🚀 Getting Started

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)

### Run the Pipeline

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/telecom-streaming-pipeline.git
cd telecom-streaming-pipeline
```

2. **Start everything in the background**
```bash
docker compose up -d --build
```

3. **View the live dashboard** at [http://localhost:8000](http://localhost:8000) — auto-refreshes every second.

4. **(Optional) Query the API directly**
```bash
curl http://localhost:8000/metrics
curl http://localhost:8000/metrics/history
```

5. **(Optional) View the Spark UI** at [http://localhost:4040](http://localhost:4040) while the pipeline is running.

6. **Stop the pipeline**
```bash
docker compose down
```

## 📁 Key Components

| Service | Role |
|---------|------|
| `producer/producer.py` | Generates and publishes synthetic telecom log events |
| `kafka/init-topic.sh` | Waits for Kafka and creates the `telecom_logs` topic |
| `spark/streaming.py` | Consumes, parses, aggregates, and POSTs windowed metrics |
| `metrics-api/main.py` | FastAPI service storing and serving the latest metrics, plus the live dashboard |

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## <!-- CONTACT -->
<!-- END CONTACT -->
