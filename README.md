Markdown
# High-Concurrency Event-Driven Flash Sale Engine

A robust, asynchronous backend system built to handle high-traffic flash sale events. This system ensures data consistency and prevents the common "oversell" bug under heavy concurrent loads by leveraging an event-driven architecture and atomic in-memory database operations.

## 🛠️ Tech Stack & Architecture

- **Framework:** FastAPI (Asynchronous Python)
- **Message Broker:** Apache Kafka (For request buffering and decoupled background processing)
- **In-Memory Data Store:** Redis (Utilizing Lua Scripting for atomic stock deduction and reservation management)
- **Monitoring & Observability:** Prometheus & Grafana (Real-time tracking of request rates, processing status, and latencies)
- **Containerization:** Docker & Docker Compose (Seamless multi-container orchestration)

---

## 📐 System Architecture Flow

1. **Ingress Layer (API):** Client requests hit the FastAPI gateway. The API performs immediate, non-blocking operations and pushes the reservation payload onto an Apache Kafka topic, returning a `202 Accepted` status code.
2. **Buffering Layer (Kafka):** Kafka acts as a shock absorber, managing huge traffic spikes and preventing the core database from being overwhelmed.
3. **Processing Layer (Worker):** Asynchronous workers consume tasks from the Kafka queue sequentially.
4. **Atomicity Layer (Redis Lua):** The worker executes a pre-loaded Lua script inside Redis. This script reads, checks, and decrements stock atomically in a single step, eliminating race conditions entirely. It also handles basic duplicate request protection.

---

## 📊 Observability & Metrics

The system exposes a custom Prometheus metrics endpoint (`/metrics`) to monitor infrastructure health and business KPIs:
- `flash_sale_requests_total`: Total number of HTTP requests hitting the API gateway.
- `flash_sale_request_latency_seconds`: Histogram measuring the exact distribution of API response times.
- `flash_sale_orders_processed_total`: Breakdown of worker task outcomes (`SUCCESS`, `SOLD_OUT`, `ALREADY_RESERVED`).

---

## 🚀 Getting Started

### Prerequisites
Make sure **Docker**, **Docker Compose**, and **Python 3.10+** are installed on the system.

### 1. Spin Up Infrastructure
Launch the Kafka, Redis, Prometheus, and Grafana containers in detached mode:
```bash
docker compose up -d
2. Set Up the Python Environment
Create a virtual environment and install the required asynchronous dependencies:

Bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
3. Run the Services
Start the main FastAPI application:

Bash
uvicorn main:app --reload --port 5000
In a separate terminal window, start the background consumer worker:

Bash
python workers.py
4. Port Reference Map
Interactive API Documentation: http://localhost:5000/docs

Prometheus Dashboard: http://localhost:9090

Grafana Analytics UI: http://localhost:3000 (Default Credentials: admin / admin)
