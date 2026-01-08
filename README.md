# 🛡️ FinGuard: Real-Time Financial Fraud Detection System

**FinGuard** is a real-time data engineering project that simulates banking transactions, detects potential fraud using **Apache Spark Streaming**, and visualizes alerts instantly via a **Streamlit Dashboard**.

## 🚀 Architecture

`Python Simulator` -> `Kafka (Confluent)` -> `Spark Streaming` -> `Redis` -> `Streamlit Dashboard`

## 🛠️ Tech Stack
- **Ingestion:** Apache Kafka (Confluent) & Zookeeper
- **Processing:** Apache Spark (PySpark Structured Streaming)
- **Storage (Speed Layer):** Redis
- **Visualization:** Streamlit
- **Containerization:** Docker & Docker Compose

## ⚙️ Setup & Run

### 1. Start Infrastructure
Run the following command to start Kafka, Spark, and Redis containers:
```bash
docker-compose up -d