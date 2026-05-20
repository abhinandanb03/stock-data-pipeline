from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, max, min,
    count, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)
import json

# CONFIG 
KAFKA_BROKER = "kafka:29092"
TOPIC        = "stock-prices"
CHECKPOINT   = "/opt/spark-data/checkpoints"

POSTGRES_URL  = "jdbc:postgresql://postgres:5432/stocks"
POSTGRES_PROPS = {
    "user":   "stockuser",
    "password": "stockpass",
    "driver": "org.postgresql.Driver",
}

REDIS_HOST = "redis"     # inside Docker → use container name
REDIS_PORT = 6379


# SPARK SESSION 
def create_spark_session():
    return (
        SparkSession.builder
        .appName("StockStreamProcessor")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars", "/opt/spark-apps/jars/spark-sql-kafka.jar,"
                              "/opt/spark-apps/jars/kafka-clients.jar,"
                              "/opt/spark-apps/jars/spark-token-provider-kafka.jar,"
                              "/opt/spark-apps/jars/commons-pool2.jar")
        .config("spark.driver.extraClassPath", "/opt/spark-apps/jars/postgresql-jdbc.jar")
        .getOrCreate()
    )


# SCHEMA 
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),  # ISO string from producer
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("source", StringType(), True),
])


# READ FROM KAFKA 
def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# PARSE AND TRANSFORM
def parse_stream(raw_df):
    return (
        raw_df
        # cast binary value to string
        .withColumn("value_str", col("value").cast(StringType()))

        # parse JSON string into struct using schema from_json returns a single struct column
        .withColumn("data", from_json(col("value_str"), stock_schema))

        # expand struct into individual columns
        .select(
            col("data.symbol").alias("symbol"),
            col("data.price").alias("price"),
            col("data.volume").alias("volume"),
            col("data.source").alias("source"),
            # Cast ISO timestamp string to proper TimestampType
            col("data.timestamp").cast(TimestampType()).alias("timestamp"),
            # Kafka's own timestamp for comparison
            col("timestamp").alias("kafka_timestamp"),
        )
        # Drop rows where parsing failed (malformed JSON)
        .filter(col("symbol").isNotNull())
    )


# ─── SINK 1: RAW TICKS → REDIS ───────────────────────────────────────
class RedisTickWriter:
    def __call__(self, batch_df, batch_id):
        import sys
        sys.path.insert(0, "/opt/spark-apps/packages")  # ← must be FIRST
        import redis
        import json

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        rows = batch_df.collect()
        for row in rows:
            tick = {
                "symbol":    row["symbol"],
                "price":     row["price"],
                "volume":    row["volume"],
                "timestamp": str(row["timestamp"]),
                "source":    row["source"],
            }
            msg = json.dumps(tick)

            r.publish(f"ticks:{row['symbol']}", msg)
            r.hset("latest_prices", row["symbol"], row["price"])
            r.lpush(f"history:{row['symbol']}", msg)
            r.ltrim(f"history:{row['symbol']}", 0, 99)

        if rows:
            print(f"  [Redis] Published {len(rows)} ticks")


def write_ticks_to_redis(parsed_df):
    return (
        parsed_df
        .writeStream
        .foreachBatch(RedisTickWriter())
        .outputMode("append")
        .trigger(processingTime="3 seconds")
        .option("checkpointLocation", f"{CHECKPOINT}/redis_ticks")
        .queryName("redis_ticks")
        .start()
    )


# WINDOWED AGGREGATIONS → POSTGRESQL
class PostgresAggWriter:
    def __call__(self, batch_df, batch_id):
        import sys
        sys.path.insert(0, "/opt/spark-apps/packages")
        import psycopg2

        # Use raw JDBC connection for upsert (INSERT ... ON CONFLICT DO NOTHING)
        # Spark's built-in .jdbc() only does plain INSERT which fails on duplicates
        rows = batch_df.collect()

        import psycopg2
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="stocks",
            user="stockuser",
            password="stockpass"
        )
        cursor = conn.cursor()

        upsert_sql = """
            INSERT INTO stock_aggregations
                (window_start, window_end, symbol, avg_price, min_price, max_price, tick_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (window_start, symbol) DO UPDATE SET
                avg_price  = EXCLUDED.avg_price,
                min_price  = EXCLUDED.min_price,
                max_price  = EXCLUDED.max_price,
                tick_count = EXCLUDED.tick_count
        """

        for row in rows:
            cursor.execute(upsert_sql, (
                row["window_start"],
                row["window_end"],
                row["symbol"],
                row["avg_price"],
                row["min_price"],
                row["max_price"],
                row["tick_count"],
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"  [Postgres] Upserted {len(rows)} aggregation rows (batch {batch_id})")


def write_aggregations_to_postgres(parsed_df):
    windowed = (
        parsed_df
        .withWatermark("timestamp", "1 minute")
        .groupBy(
            window(col("timestamp"), "5 minutes"),  # tumbling window
            col("symbol")
        )
        .agg(
            spark_round(avg("price"), 4).alias("avg_price"),
            spark_round(min("price"), 4).alias("min_price"),
            spark_round(max("price"), 4).alias("max_price"),
            count("price").alias("tick_count"),
        )
        # Flatten the window struct into readable columns
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            col("tick_count"),
        )
    )

    return (
        windowed
        .writeStream
        .foreachBatch(PostgresAggWriter())
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT}/postgres_agg")
        .queryName("postgres_aggregations")
        .start()
    )


# PRICE ALERTS
class PostgresAlertWriter:
    def __call__(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df
            .write
            .jdbc(
                url=POSTGRES_URL,
                table="stock_alerts",
                mode="append",
                properties=POSTGRES_PROPS,
            )
        )
        print(f"  [Postgres] Wrote {batch_df.count()} alerts")


def write_alerts_to_postgres(parsed_df):
    alerts = (
        parsed_df
        .filter(col("volume") > 1_000_000)
        .select(
            col("symbol"),
            col("price"),
            col("volume"),
            col("timestamp").alias("timestamp"),
        )
        .withColumn("alert_type", col("symbol").cast(StringType()))  # placeholder
    )

    # Add alert_type column properly
    from pyspark.sql.functions import lit
    alerts = (
        parsed_df
        .filter(col("volume") > 1_000_000)
        .select(
            col("symbol"),
            col("price"),
            col("volume"),
            col("timestamp"),
            lit("VOLUME_SPIKE").alias("alert_type"),
        )
    )

    return (
        alerts
        .writeStream
        .foreachBatch(PostgresAlertWriter())
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT}/postgres_alerts")
        .queryName("postgres_alerts")
        .start()
    )


# ─── MAIN ────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Spark Structured Streaming — Phase 5")
    print("  Sinks: Redis (ticks) + PostgreSQL (aggs + alerts)")
    print("=" * 55)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df    = read_kafka_stream(spark)
    parsed_df = parse_stream(raw_df)

    q1 = write_ticks_to_redis(parsed_df)
    q2 = write_aggregations_to_postgres(parsed_df)
    q3 = write_alerts_to_postgres(parsed_df)

    print(f"\n  Streaming queries active:")
    print(f"  → {q1.name}")
    print(f"  → {q2.name}")
    print(f"  → {q3.name}")
    print(f"\n  Waiting for data...\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()