# Real-Time Stock Data Pipeline

A production-grade streaming data pipeline that ingests live US stock trade data, processes it with Apache Spark, and displays results on a real-time dashboard.

```
Finnhub WebSocket → Kafka → Spark Structured Streaming → Redis + PostgreSQL → Streamlit
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌───────────┐   ┌───────────┐   ┌──────────┐  ┌──────────┐     │
│  │ ZooKeeper │   │   Kafka   │   │  Spark   │  │ Postgres │     │
│  │  :2181    │◄──│   :9092   │──►│  :8080   │─►│  :5432   │     │
│  └───────────┘   └───────────┘   └──────────┘  └──────────┘     │
│                        ▲               │                        │
│                        │               ▼                        │
│                        │         ┌──────────┐                   │
│                        │         │  Redis   │                   │
│                        │         │  :6379   │                   │
│                        │         └──────────┘                   │
└────────────────────────┼───────────────┼────────────────────────┘
                         │               │
              ┌──────────┴──┐    ┌───────┴──────────┐
              │   Python    │    │    Streamlit      │
              │  Producer   │    │    Dashboard      │
              │ (your host) │    │   localhost:8501  │
              └─────────────┘    └──────────────────-┘
```

### Data Flow

1. **Finnhub WebSocket** — pushes live US stock trade ticks in real time
2. **Python Producer** — receives ticks, serializes to JSON, publishes to Kafka keyed by symbol
3. **Kafka** — stores messages across 3 partitions (`stock-prices` topic), one partition per symbol group
4. **Spark Structured Streaming** — consumes Kafka, runs 3 concurrent streaming queries:
   - Raw ticks → Redis every 3 seconds
   - 5-minute windowed aggregations → PostgreSQL every 10 seconds
   - Volume spike alerts → PostgreSQL every 5 seconds
5. **Redis** — stores latest prices (hash), tick history per symbol (list), and pushes live ticks via pub/sub
6. **PostgreSQL** — stores windowed aggregations and alerts permanently
7. **Streamlit** — reads from Redis (live prices + chart) and PostgreSQL (aggregations + alerts), refreshes via pub/sub background thread

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka | 7.5.0 (Confluent) |
| Cluster Coordinator | Apache ZooKeeper | 7.5.0 (Confluent) |
| Stream Processor | Apache Spark | 3.5.3 |
| In-Memory Store | Redis | 7.2 |
| Database | PostgreSQL | 16 |
| Data Source | Finnhub WebSocket API | — |
| Producer / Consumer | Python + kafka-python-ng | 3.11+ |
| Dashboard | Streamlit + Plotly | — |
| Container Runtime | Docker + Docker Compose | — |

---

## Project Structure

```
stock-pipeline/
├── docker-compose.yml
├── producer/
│   ├── producer.py          # Finnhub WebSocket → Kafka
│   ├── consumer.py          # Plain Python consumer (learning/debug)
│   └── requirements.txt
├── spark/
│   ├── streaming_job.py     # Spark Structured Streaming job
│   ├── startup.sh           # Container startup script
│   ├── jars/                # Kafka + PostgreSQL connector JARs
│   │   ├── spark-sql-kafka.jar
│   │   ├── kafka-clients.jar
│   │   ├── spark-token-provider-kafka.jar
│   │   ├── commons-pool2.jar
│   │   └── postgresql-jdbc.jar
│   └── packages/            # Python packages installed into Spark
├── postgres/
│   └── init.sql             # Database schema (auto-runs on first start)
├── dashboard/
│   └── dashboard.py         # Streamlit dashboard
└── data/
    └── checkpoints/         # Spark streaming checkpoints
```

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/stock-pipeline.git
cd stock-pipeline
```

### 2. Add your Finnhub API key

Open `producer/producer.py` and replace the placeholder:

```python
FINNHUB_API_KEY = "your_api_key_here"
```

### 3. Create required directories

```powershell
# Windows PowerShell
New-Item -ItemType Directory -Force -Path ".\spark\jars"
New-Item -ItemType Directory -Force -Path ".\spark\packages"
New-Item -ItemType Directory -Force -Path ".\data\checkpoints"
New-Item -ItemType Directory -Force -Path ".\postgres"
```

```bash
# Linux / macOS
mkdir -p spark/jars spark/packages data/checkpoints postgres
```

### 4. Download Spark connector JARs

```powershell
# Windows PowerShell
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" -OutFile "spark\jars\spark-sql-kafka.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" -OutFile "spark\jars\kafka-clients.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" -OutFile "spark\jars\spark-token-provider-kafka.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -OutFile "spark\jars\commons-pool2.jar"
Invoke-WebRequest -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar" -OutFile "spark\jars\postgresql-jdbc.jar"
```

```bash
# Linux / macOS
curl -o spark/jars/spark-sql-kafka.jar "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar"
curl -o spark/jars/kafka-clients.jar "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
curl -o spark/jars/spark-token-provider-kafka.jar "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
curl -o spark/jars/commons-pool2.jar "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
curl -o spark/jars/postgresql-jdbc.jar "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
```

### 5. Install Python dependencies

```bash
pip install kafka-python-ng websocket-client redis psycopg2-binary streamlit plotly
```

### 6. Add Kafka to Windows hosts file (Windows only)

Open PowerShell as Administrator:

```powershell
Add-Content -Path "C:\Windows\System32\drivers\etc\hosts" -Value "127.0.0.1 kafka"
```

---

## Running the Pipeline

Run each of the following in a **separate terminal**, in order.

### Terminal 1 — Start all Docker containers

```bash
docker-compose up -d
```

Verify all 6 containers are running:

```bash
docker-compose ps
```

Expected output:
```
NAME            STATUS
zookeeper       Up
kafka           Up
spark-master    Up
spark-worker    Up
postgres        Up
redis           Up
```

Wait ~20 seconds for Kafka to fully initialise, then check it is healthy:

```bash
docker logs kafka 2>&1 | grep "started"
```

### Terminal 2 — Create the Kafka topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic stock-prices \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Verify:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Terminal 3 — Install Python packages inside Spark container

```bash
docker exec -it spark-master python3 -m pip install redis --target=/opt/spark-apps/packages
docker exec -it spark-master python3 -m pip install psycopg2-binary --target=/opt/spark-apps/packages
```

### Terminal 4 — Start the Python producer

```bash
cd producer
python producer.py
```

You should see ticks streaming:
```
[CONNECTED] WebSocket open → subscribing to symbols

  📈 AAPL   $182.5100  vol=100
  📈 TSLA   $248.2300  vol=250
```

### Terminal 5 — Start the Spark streaming job

```bash
docker exec -it spark-master bash -c "PYSPARK_PYTHON=python3 /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark-apps/jars/spark-sql-kafka.jar,/opt/spark-apps/jars/kafka-clients.jar,/opt/spark-apps/jars/spark-token-provider-kafka.jar,/opt/spark-apps/jars/commons-pool2.jar,/opt/spark-apps/jars/postgresql-jdbc.jar \
  --driver-class-path /opt/spark-apps/jars/postgresql-jdbc.jar \
  /opt/spark-apps/streaming_job.py"
```

You should see:
```
  Streaming queries active:
  → redis_ticks
  → postgres_aggregations
  → postgres_alerts

  Waiting for data...

  [Redis] Published 6 ticks (batch 0)
  [Postgres] Upserted 6 aggregation rows (batch 1)
```

### Terminal 6 — Start the Streamlit dashboard

```bash
python -m streamlit run dashboard/dashboard.py
```

Open your browser at **http://localhost:8501**

---

## Verifying Data Flow

### Check Kafka messages are flowing

```bash
docker exec -it kafka kafka-console-consumer \
  --topic stock-prices \
  --bootstrap-server localhost:9092\
  --from-beginning \
  --property print.key=true
```

### Check Redis has live data

```bash
docker exec -it redis redis-cli HGETALL latest_prices
docker exec -it redis redis-cli LRANGE history:AAPL 0 4
```

### Check PostgreSQL has aggregations

```bash
docker exec -it postgres psql -U stockuser -d stocks \
  -c "SELECT symbol, window_start, avg_price, tick_count FROM stock_aggregations ORDER BY window_start DESC LIMIT 10;"
```

### Check consumer group lag

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-analytics-group
```

### Open Spark UI

```
http://localhost:8080
```

---

## Stopping the Pipeline

```bash
# Stop producer and Spark job with Ctrl+C in their terminals, then:
docker-compose down
```

To wipe all stored data (topics, PostgreSQL, Redis) and start completely fresh:

```bash
docker-compose down -v
```

> ⚠️ `down -v` permanently deletes all volumes. Only use this if you want a clean slate.

---

## Dashboard Features

| Feature | Data Source | Update Frequency |
|---------|------------|-----------------|
| Live price cards | Redis pub/sub | On every tick (~instant) |
| Multi-symbol price chart | Redis history list | Every 5s (poll fallback) |
| 5-minute aggregations table | PostgreSQL | Every 5 minutes (window close) |
| Volume spike alerts | PostgreSQL | Every 5s |
| Pub/sub status indicator | Session state | Real-time |

---

## Key Concepts Demonstrated

- **Kafka producer/consumer** with manual offset commits and consumer groups
- **Kafka partitioning** by symbol key for ordered per-symbol processing
- **Spark Structured Streaming** with micro-batch processing
- **Tumbling windows** with watermarking for late data handling
- **`foreachBatch`** for writing to non-native sinks (Redis, PostgreSQL upserts)
- **Redis pub/sub** with a background listener thread for sub-second UI updates
- **Checkpointing** for Spark fault tolerance and exactly-once semantics
- **Docker networking** with internal and external Kafka listeners

---

## Troubleshooting

**Kafka connection timeout (`KafkaTimeoutError`)**
- Make sure `127.0.0.1 kafka` is in your hosts file (Windows)
- Make sure port `29092` is mapped in `docker-compose.yml`

**`ModuleNotFoundError: No module named 'redis'` in Spark**
```bash
docker exec -it spark-master python3 -m pip install redis --target=/opt/spark-apps/packages
```

**`ClassNotFoundException: org.postgresql.Driver`**
- Make sure `--driver-class-path` flag is included in the spark-submit command

**Topic not found after restarting Docker**
- Topics are now persistent via named volumes — this should not happen
- If it does: `docker exec -it kafka kafka-topics --create --topic stock-prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`

**Spark job exits immediately with no error**
- Add `PYSPARK_PYTHON=python3` before the spark-submit command
- Check logs: `docker logs spark-master`

**No data in dashboard after Spark is running**
- Wait at least one micro-batch cycle (5-10 seconds)
- Check Redis: `docker exec -it redis redis-cli HGETALL latest_prices`
- Check Spark logs for errors in the terminal running spark-submit
