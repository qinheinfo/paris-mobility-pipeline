# Paris Mobility Pipeline

A real-time data pipeline for Paris Vélib' bike-share availability: streaming ingestion with Kafka and Spark Structured Streaming into an Apache Iceberg lakehouse, followed by batch transformations and data-quality tests in dbt.

## Architecture

```
Vélib' GBFS API (station_status, every ~60 s)
        │  producer.py (Python)
        ▼
Kafka topic: velib.status.raw (3 partitions, keyed by station_id)
        │  Spark Structured Streaming (1-minute micro-batches, checkpointed)
        ▼
┌──────────────────────── Iceberg lakehouse ────────────────────────┐
│                                                                    │
│  bronze: station_status_bronze          station_info (reference)   │
│       │                                       │                    │
│       │  dbt (SQL executed by Spark via Thrift)                    │
│       ├──► silver: station_status_silver      ▼                    │
│       │                                  dim_station               │
│       └──────────► gold: mart_station_availability ◄──┘            │
└────────────────────────────────────────────────────────────────────┘
```

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `requests`, `confluent-kafka` |
| Message broker | Apache Kafka 3.9 (KRaft mode) |
| Stream processing | Spark 3.5 Structured Streaming |
| Storage | Apache Iceberg (REST catalog, S3-compatible object store) |
| Transformation & tests | dbt (dbt-spark, Thrift connection) |
| Infrastructure | Docker Compose |

## Data model

| Layer | Table | Grain | Built by |
|---|---|---|---|
| Bronze | `station_status_bronze` | One row per station per snapshot (~1 min) | Spark Structured Streaming |
| Reference | `station_info` | One row per station | `ingest_station_info.py` (batch) |
| Silver | `station_status_silver` | One row per station state change | dbt |
| Dimension | `dim_station` | One row per station | dbt |
| Gold | `mart_station_availability` | One row per station | dbt |

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for field definitions and [`docs/design_decisions.md`](docs/design_decisions.md) for the reasoning behind each choice.

## Key findings

### 1. `last_reported` is not a reliable deduplication key
Station bike counts sometimes changed while `last_reported` stayed the same, so deduplicating on `(station_id, last_reported)` would silently drop real state changes. Silver deduplicates by **content** instead: a row is kept only when bikes, docks, or renting/returning status differ from the previous snapshot.

### 2. Most raw rows are duplicates
| Window | Bronze rows | Silver rows | Removed |
|---|---|---|---|
| ~3 hours, daytime | 41,040 | 7,947 | 80.6% |
| ~18 hours, incl. night | 1,673,520 | 124,973 | 92.5% |

The higher ratio over the longer window is consistent with quiet overnight periods, when station states barely change.

### 3. Ranking by "empty" time alone is misleading
Stations out of service are excluded (`is_renting = 1`), but some stations operate only part of the day. In the unfiltered ranking, **6 of the top 10 "most often empty" stations were in service less than 25% of the time**. The gold table therefore exposes `coverage_pct`, and a coverage filter (≥ 80%) surfaces stations that are consistently empty.

### 4. Fault tolerance
The streaming job was stopped for about an hour and its container was recreated. After restart, it resumed from the persisted checkpoint: **0 duplicates across 4M rows**, and every snapshot produced during the downtime was recovered from Kafka. A separate 51-minute gap was traced to the producer side (most likely the host machine sleeping): Kafka can only buffer data that reaches it.

## Data quality

9 dbt tests run on every build:

- `not_null` / `unique` on primary keys of `dim_station` and `mart_station_availability`
- `not_null` on key columns of `station_status_silver`
- A custom test asserting that all percentage metrics stay within 0–100

## Repository structure

```
├── producer/            # Kafka producer polling the Vélib' API
├── spark/               # Streaming and batch Spark jobs
├── dbt/                 # dbt project: sources, models (silver, gold), tests
├── docs/                # Data dictionary and design decisions
├── docker-compose.yml   # Spark, Iceberg REST catalog, object store, Kafka, Kafka UI
└── requirements.txt
```

## How to run

```bash
# 1. Start the infrastructure
docker compose up -d

# 2. Python environment
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# 3. Start the producer (keep running)
python producer/producer.py

# 4. Start the streaming job (keep running, in another terminal)
docker exec -it spark-iceberg spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/iceberg/jobs/stream_to_bronze.py

# 5. Load station reference data
docker exec -it spark-iceberg spark-submit /home/iceberg/jobs/ingest_station_info.py

# 6. Build and test the dbt models
docker exec spark-iceberg spark-sql -e "CREATE NAMESPACE IF NOT EXISTS demo.default;"
cd dbt
dbt run
dbt test
```

Kafka UI is available at http://localhost:8090.

## Roadmap

- [x] Streaming ingestion: API → Kafka → Spark → Iceberg bronze
- [x] dbt models: silver, dimension, gold
- [x] dbt tests, including a custom range check
- [x] Persisted streaming checkpoint and fault-tolerance test
- [ ] Scheduled dbt runs to keep the gold layer fresh
- [ ] Iceberg table maintenance (compaction, snapshot expiration)
- [ ] Persist object storage on the host
- [ ] Visualisation of the results

## Limitations

- Runs on a single machine: when the host sleeps, the producer stops and no data reaches Kafka.
- The observation window is a few days, so station rankings are indicative rather than conclusive.
- Each producer cycle takes slightly more than 60 seconds, so an hour occasionally contains 59 snapshots instead of 60.