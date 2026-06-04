# E-Commerce Streaming Pipeline

![CI](https://github.com/INKCLEANING/ecommerce-streaming-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-black?logo=apachekafka)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE?logo=apacheairflow)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Athena-FF9900?logo=amazonaws)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana)

A production-style real-time data pipeline that ingests synthetic e-commerce events, validates them with Pydantic, streams them through Kafka, stores them in a partitioned AWS S3 data lake, and makes them queryable via Athena — with data quality checks, orchestration, monitoring, and two live dashboards.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker (Local)                           │
│                                                                 │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────────────┐   │
│  │ Producer │───▶│    Kafka    │───▶│  Consumer            │   │
│  │ (Faker + │    │ (+ ZooKeep.)│    │  (Kafka → S3)        │   │
│  │ Pydantic)│    └──────┬──────┘    └──────────┬───────────┘   │
│  └──────────┘           │                      │               │
│       │                 ▼                      ▼               │
│   DLQ topic      ┌────────────┐       ┌────────────────┐       │
│  (bad events)    │ Kafka      │       │   AWS S3       │       │
│                  │ Exporter   │       │ (JSON, partitio│       │
│                  └─────┬──────┘       │  ned by date)  │       │
│                        │              └───────┬────────┘       │
│                  ┌─────▼──────┐               │               │
│                  │ Prometheus │      ┌─────────▼───────┐       │
│                  └─────┬──────┘      │   AWS Athena    │       │
│                        │             │  (SQL on S3)    │       │
│                  ┌─────▼──────┐      └─────────┬───────┘       │
│                  │  Grafana   │                │               │
│                  │ Dashboards │     ┌──────────▼──────────┐    │
│                  └────────────┘     │ Great Expectations  │    │
│                                     │ (data quality)      │    │
│  ┌──────────────────────────┐       └─────────────────────┘    │
│  │  Airflow                 │◀──────────────────────────────┘  │
│  │  ├── quality_checks      │                                   │
│  │  ├── reporting_refresh   │                                   │
│  │  └── export_tableau      │                                   │
│  └──────────────────────────┘                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴──────────────┐
              ▼                              ▼
     ┌────────────────┐            ┌──────────────────┐
     │  Looker Studio │            │  Tableau Public  │
     │  (live Athena) │            │  (CSV export)    │
     └────────────────┘            └──────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Event generation | Python + Faker | Synthetic orders, shipments, page views at ~100 events/s |
| Schema validation | Pydantic v2 | Validates every event before it hits Kafka; bad events → DLQ |
| Message broker | Apache Kafka | Decouples producer from consumer; 4 topics |
| Cloud storage | AWS S3 | Partitioned JSON landing zone (`year/month/day/hour/`) |
| Query layer | AWS Athena | Serverless SQL over S3 — no ETL needed |
| Data quality | Great Expectations | Expectation suites run hourly via Airflow |
| Orchestration | Apache Airflow | Schedules GE checks, partition repair, Tableau CSV export |
| Monitoring | Prometheus + Grafana | Kafka throughput, consumer lag, DLQ rate, S3 latency |
| Dashboards | Looker Studio + Tableau Public | Live Athena queries and scheduled CSV exports |
| CI | GitHub Actions | Lint (ruff), unit tests (pytest), docker-compose validation |

---

## Dashboards

| Tool | Link | Data |
|---|---|---|
| Looker Studio | [View Dashboard](https://datastudio.google.com/reporting/2ff1aadd-6609-48dd-8ce7-0f90d21822aa) | Order volume, product performance, customer behavior |
| Tableau Public | [View Dashboard](https://public.tableau.com/views/E-CommerceStreamingPipeline/CustomerBehavior) | Sales overview, product performance, customer behavior |
| Grafana | http://localhost:3000 | Real-time Kafka + pipeline metrics |

---

## CI Pipeline

Every push to a `feature/**` branch and every PR to `main` runs three checks:

| Job | What it does |
|---|---|
| **Lint** | `ruff check` across all Python source |
| **Unit Tests** | 15 pytest tests covering Pydantic schema validation |
| **Validate docker-compose** | `docker compose config` syntax check |

---

## Event Schema

Three event types produced to Kafka at ~100 events/second:

| Event | Key Fields |
|---|---|
| `order_placed` | order_id, customer_id, product_id, category, quantity, price |
| `order_shipped` | order_id, carrier (UPS/FedEx/USPS/DHL), tracking_number, estimated_delivery |
| `page_view` | session_id, customer_id, product_id, page_url, device_type |

Invalid events (wrong types, out-of-range values, bad enums) are routed to `orders.dlq` and tracked in Grafana.

---

## Data Quality

Three layers of validation:

1. **Pydantic at source** — every event validated before reaching Kafka
2. **Great Expectations on S3** — hourly Airflow DAG checks price ranges, carrier names, device types, null fields
3. **Grafana DLQ monitoring** — real-time visibility if the dead-letter queue spikes

---

## Grafana Dashboards

| Dashboard | Metrics |
|---|---|
| Kafka Throughput | Events/s by topic, consumer lag, S3 write latency p99 |
| Data Quality | DLQ count, validation failure rate, quality score % |
| Pipeline Health | End-to-end throughput, S3 latency p50/p95/p99, active topics |

---

## Project Structure

```
ecommerce-streaming-pipeline/
├── .github/workflows/ci.yml       # GitHub Actions: lint, test, compose validate
├── producer/
│   ├── schemas.py                 # Pydantic models for 3 event types
│   └── producer.py                # Faker → Pydantic → Kafka (~100 events/s)
├── consumer/
│   └── consumer.py                # Kafka → S3 (batched, partitioned JSON)
├── dags/
│   ├── quality_checks.py          # Hourly Great Expectations validation
│   ├── reporting.py               # Daily MSCK REPAIR for Athena partitions
│   └── export_tableau.py          # Daily Athena → CSV export for Tableau
├── great_expectations/
│   └── expectations/              # Expectation suites per event type
├── monitoring/
│   ├── prometheus.yml             # Scrape configs
│   └── grafana/dashboards/        # Pre-built dashboard JSON files
├── athena/
│   └── create_tables.sql          # External table DDL for all 4 topics
├── scripts/
│   └── setup_aws.py               # Creates S3 bucket + Athena DB
├── tests/
│   └── test_schemas.py            # 15 unit tests for Pydantic schemas
├── docker-compose.yml             # 10-service stack
├── Dockerfile.airflow             # Airflow + boto3 + Great Expectations
├── requirements.txt
└── .env.example                   # Environment variable template
```

---

## Local Setup

### Prerequisites
- Docker Desktop
- Python 3.11+
- AWS account (free tier)

### 1. Clone and configure

```bash
git clone https://github.com/INKCLEANING/ecommerce-streaming-pipeline.git
cd ecommerce-streaming-pipeline
cp .env.example .env
# fill in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env
```

### 2. Create AWS resources

```bash
pip install boto3 python-dotenv
python scripts/setup_aws.py
```

Expected output:
```
[OK] AWS credentials valid — account: xxxxxxxxx
[OK] Created S3 bucket: s3://ecommerce-streaming-raw
[OK] Athena database ready: ecommerce
```

### 3. Create Athena tables

Run `athena/create_tables.sql` in the [Athena console](https://console.aws.amazon.com/athena), replacing `YOUR_BUCKET` with `ecommerce-streaming-raw`. Run each `CREATE TABLE` block and `MSCK REPAIR TABLE` statement separately.

### 4. Start the pipeline

```bash
docker compose up -d --build
```

Services and ports:

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) |
| Airflow | http://localhost:8081 (admin / admin) |
| Prometheus | http://localhost:9090 |
| Kafka | localhost:9092 |

### 5. Verify data is flowing

```sql
-- Run in Athena console after ~2 minutes
SELECT COUNT(*) FROM ecommerce.orders_placed;
```

---

## Running Tests

```bash
pip install pytest pydantic faker
pytest tests/ -v
```

---

## Key Design Decisions

**Why Kafka over direct S3 writes?**
Kafka decouples ingestion rate from write rate. The consumer batches events and flushes to S3 every 30 seconds or 500 events — reducing S3 API calls and enabling replay if the consumer crashes.

**Why Athena over a traditional data warehouse?**
Athena is serverless and charges per query scan. With Hive-style partitioning (`year/month/day/hour/`), queries that filter by date scan only the relevant files — keeping costs near zero on free-tier volumes.

**Why two dashboard tools?**
Looker Studio connects live to Athena for real-time queries. Tableau Public requires file-based data but is a more recognized BI tool — the `export_tableau` Airflow DAG generates daily CSVs so both are always current.
