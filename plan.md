# E-Commerce Streaming Pipeline — Project Plan

## Goal
Build a real-time streaming data pipeline that ingests synthetic e-commerce events,
stores them in AWS S3, queries them with Athena, validates data quality with Great
Expectations, monitors pipeline health with Grafana, and orchestrates batch jobs with
Airflow — all free.

---

## Tech Stack (100% Free)

| Layer | Tool | Cost |
|---|---|---|
| Event generation | Python + Faker (Docker) | Free |
| Message broker | Apache Kafka (Docker) | Free |
| Cloud storage | AWS S3 (free tier) | 5 GB + 20K requests/month free |
| Query layer | AWS Athena (free tier) | 1 TB queries/month free |
| Data quality | Great Expectations | Free open-source |
| Schema validation | Pydantic | Free open-source |
| Orchestration | Apache Airflow (Docker) | Free |
| Monitoring | Grafana + Prometheus (Docker) | Free |
| Dashboards (live) | Looker Studio | Free |
| Dashboards (portfolio) | Tableau Public | Free |
| IDE | VS Code | Free |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker (Local)                           │
│                                                                 │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────────────┐   │
│  │ Producer │───▶│    Kafka    │───▶│  Consumer            │   │
│  │ (Faker)  │    │ (+ ZooKeep.)│    │  (Kafka → S3)        │   │
│  └──────────┘    └─────────────┘    └──────────┬───────────┘   │
│                        │                       │               │
│                        ▼                       ▼               │
│                  ┌──────────┐         ┌────────────────┐       │
│                  │Prometheus│         │   AWS S3       │       │
│                  │(metrics) │         │ (raw JSON,     │       │
│                  └────┬─────┘         │  partitioned)  │       │
│                       │               └───────┬────────┘       │
│                  ┌────▼─────┐                 │               │
│                  │ Grafana  │         ┌────────▼────────┐      │
│                  │(dashbrd) │         │  AWS Athena     │      │
│                  └──────────┘         │  (SQL on S3)    │      │
│                                       └────────┬────────┘      │
│  ┌──────────────────────────┐                  │               │
│  │  Airflow                 │         ┌────────▼────────┐      │
│  │  ├── quality_checks DAG  │────────▶│Great Expectations│      │
│  │  └── reporting DAG       │         │(data quality)   │      │
│  └──────────────────────────┘         └─────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │  Looker Studio  │
                                         │  (live dashbrd) │
                                         └────────┬────────┘
                                                  │ CSV export
                                         ┌────────▼────────┐
                                         │ Tableau Public  │
                                         │(portfolio dash) │
                                         └─────────────────┘
```

---

## Dataset — Synthetic E-Commerce Events

Generated with Python's `Faker` library — no API key, runs forever, fully controlled.

**Event types produced to Kafka:**

| Event | Fields |
|---|---|
| `order_placed` | order_id, customer_id, product_id, category, quantity, price, timestamp |
| `order_shipped` | order_id, carrier, tracking_number, estimated_delivery, timestamp |
| `page_view` | session_id, customer_id, product_id, page_url, device_type, timestamp |

Producer emits ~100 events/second to demonstrate real throughput.

---

## Data Quality Strategy

**Layer 1 — Schema validation at source (Pydantic)**
- Every event is validated before being sent to Kafka
- Invalid events are routed to a dead-letter topic (`orders.dlq`)
- Catches: missing fields, wrong types, out-of-range values

**Layer 2 — Great Expectations on S3 landing data**
- Runs after each Airflow batch load
- Expectations per dataset:
  - `order_placed`: price > 0, quantity between 1–100, valid category values
  - `order_shipped`: valid carrier names, delivery date > order date
  - `page_view`: valid device types, session_id not null
- Generates HTML data docs report saved to S3

**Layer 3 — Airflow task-level monitoring**
- Tasks alert on failure via Airflow's built-in email/slack hooks
- SLA misses tracked for time-sensitive quality check tasks

---

## Monitoring Strategy (Grafana + Prometheus)

**Kafka metrics (via JMX Exporter → Prometheus):**
- Messages produced per second
- Consumer lag (how far behind the consumer is)
- Topic partition offsets
- Error rate on producer/consumer

**Pipeline metrics (custom Python → Prometheus):**
- Events processed per second
- Dead-letter queue (DLQ) message count — spikes = data quality issues
- S3 write latency
- Great Expectations validation pass/fail rate

**Grafana dashboards:**
- Dashboard 1: Kafka throughput and consumer lag
- Dashboard 2: Data quality — DLQ rate, GE pass rates over time
- Dashboard 3: Pipeline health — task durations, success/failure rates

---

## Project Structure

```
ecommerce-streaming-pipeline/
├── docker-compose.yml              # All services: Kafka, Airflow, Grafana, Prometheus
├── Dockerfile.airflow              # Custom Airflow image
├── producer/
│   ├── Dockerfile
│   ├── producer.py                 # Faker → Kafka (with Pydantic validation)
│   └── schemas.py                  # Pydantic models for each event type
├── consumer/
│   ├── Dockerfile
│   └── consumer.py                 # Kafka → AWS S3 (JSON, partitioned by date)
├── dags/
│   ├── quality_checks.py           # Great Expectations checks on S3 data
│   ├── reporting.py                # Athena → Looker Studio refresh
│   └── export_tableau.py           # Athena → CSV export for Tableau Public
├── great_expectations/
│   ├── great_expectations.yml
│   └── expectations/               # Expectation suites per event type
├── monitoring/
│   ├── prometheus.yml              # Scrape configs
│   └── grafana/
│       └── dashboards/             # Pre-built dashboard JSON files
├── athena/
│   └── create_tables.sql           # DDL to create Athena tables on S3
├── scripts/
│   └── setup_aws.py                # Creates S3 bucket and Athena DB
├── requirements.txt
├── .env.example
└── README.md
```

---

## Execution Phases

### Phase 1 — AWS + Environment Setup (Day 1, ~2 hours)
1. Create AWS account (free tier) or use existing
2. Create IAM user with S3 + Athena permissions — download access keys
3. Create S3 bucket: `ecommerce-streaming-raw`
4. Create Athena database: `ecommerce`
5. Set up `.env` with AWS credentials
6. Initialize git repo, push to GitHub

### Phase 2 — Kafka + Producer + Consumer (Days 1–3, ~4 hours)
1. Write `docker-compose.yml` with Kafka + Zookeeper
2. Write `producer/schemas.py` — Pydantic models for 3 event types
3. Write `producer/producer.py` — Faker generates events → Pydantic validates → Kafka
4. Write `consumer/consumer.py` — Kafka → S3 (JSON, partitioned as `year/month/day/`)
5. Verify events flowing: `kafka-console-consumer` on the topic
6. Verify S3: objects appearing in correct partition paths

### Phase 3 — Athena Query Layer (Day 3–4, ~2 hours)
1. Run `athena/create_tables.sql` to define external tables over S3
2. Verify queries: `SELECT COUNT(*) FROM ecommerce.orders` returns rows
3. Test partition pruning: queries by date are fast and cheap

### Phase 4 — Data Quality with Great Expectations (Days 4–6, ~4 hours)
1. Initialize Great Expectations: `great_expectations init`
2. Connect GE to Athena as the data source
3. Create expectation suites for each event type
4. Write Airflow DAG `quality_checks.py` that runs GE after each batch
5. View HTML data docs report — screenshot for README

### Phase 5 — Monitoring with Grafana (Days 6–8, ~3 hours)
1. Add Prometheus + Grafana to `docker-compose.yml`
2. Add JMX Exporter sidecar to Kafka container for metrics
3. Add custom Prometheus metrics to consumer (events/sec, DLQ count)
4. Build 3 Grafana dashboards (Kafka health, data quality, pipeline)
5. Export dashboard JSON files to `monitoring/grafana/dashboards/`

### Phase 6 — Airflow Orchestration (Days 8–9, ~2 hours)
1. Wire `quality_checks` DAG to run after each S3 batch lands
2. Add `reporting` DAG to refresh Athena partition metadata daily
3. Test full end-to-end: producer → Kafka → S3 → Athena → GE checks → Grafana

### Phase 7 — Looker Studio Dashboard (Day 9–10, ~2 hours)
1. Connect Looker Studio to Athena (via AWS Data Source connector)
2. Build 3 pages: Order Volume, Product Performance, Customer Behavior
3. Make public, add URL to README

### Phase 7b — Tableau Public Dashboard (Day 10–11, ~2 hours)
1. Add `export_tableau.py` Airflow DAG — runs Athena queries and saves results as CSV to S3
2. Download the CSV export locally
3. Open Tableau Public Desktop (free download), connect to the CSV
4. Build matching dashboards: Order Volume, Product Performance, Customer Behavior
5. Publish to Tableau Public (tableau.com/public) — anyone can view without an account
6. Add Tableau Public URL to README alongside Looker Studio URL

### Phase 8 — Portfolio Polish (Days 10–12, ~2 hours)
1. Record a short screen capture of the pipeline running (Kafka + Grafana + Airflow)
2. Write README with architecture diagram, setup instructions, key insights
3. Final commit and push

---

## Timeline

| Phase | Days | Output |
|---|---|---|
| AWS + Environment | Day 1 | S3 bucket, Athena DB, credentials |
| Kafka + Producer + Consumer | Day 1–3 | Events flowing to S3 |
| Athena Query Layer | Day 3–4 | SQL queries working on S3 |
| Great Expectations | Day 4–6 | Data quality reports in S3 |
| Grafana Monitoring | Day 6–8 | 3 live dashboards |
| Airflow Orchestration | Day 8–9 | Full automated pipeline |
| Looker Studio | Day 9–10 | Live public dashboard URL |
| Tableau Public | Day 10–11 | Portfolio dashboard URL |
| Portfolio Polish | Day 11–13 | README, screenshots, GitHub |

**Total: 2–3 weeks part-time (~20 hours of actual work)**

---

## What This Demonstrates to Employers

| Skill | How it's shown |
|---|---|
| Streaming architecture | Kafka producer/consumer with multiple event types |
| Schema validation | Pydantic models with dead-letter queue pattern |
| Cloud data lake | S3 partitioned storage + Athena query layer |
| Data quality | Great Expectations suites with automated reporting |
| Monitoring | Grafana + Prometheus with real Kafka and pipeline metrics |
| Orchestration | Airflow DAGs coordinating quality checks |
| Docker | Multi-service compose file (8+ containers) |
| AWS | S3, Athena, IAM — core services in every data job |
| BI / Dashboards | Looker Studio (live Athena) + Tableau Public (portfolio) |

---

## Free Tier Limits to Watch

| Service | Free limit | Risk |
|---|---|---|
| AWS S3 | 5 GB storage | Events are small JSON — will take months to hit |
| AWS Athena | 1 TB queries/month | Only charged per query scan — use partitions |
| AWS Free Tier | 12 months for some services | S3 and Athena are always free, not 12-month |
