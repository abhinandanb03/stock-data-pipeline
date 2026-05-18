from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, max, min,
    count, round as spark_round, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)

# ─── CONFIG ──────────────────────────────────────────────────────────
KAFKA_BROKER  = "kafka:29092"    # inside Docker → use internal listener
TOPIC         = "stock-prices"
CHECKPOINT    = "/opt/spark-data/checkpoints"


# ─── SPARK SESSION ───────────────────────────────────────────────────
def create_spark_session():
    """
    SparkSession is the entry point to all Spark functionality.

    .master("local[*]")
        Run Spark locally using all available CPU cores.
        local[2] = 2 cores, local[*] = all cores.
        For our learning setup this is fine — in production
        you'd point this to spark://spark-master:7077

    .config("spark.sql.shuffle.partitions", "4")
        When Spark shuffles data (e.g. for groupBy), it creates
        this many partitions. Default is 200 which is way too many
        for our tiny dataset — 4 is plenty.

    .config("spark.streaming.stopGracefullyOnShutdown", "true")
        When you Ctrl+C, Spark finishes the current micro-batch
        before stopping instead of killing it mid-way.
    """
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
        .getOrCreate()
    )


# ─── SCHEMA ──────────────────────────────────────────────────────────
# Matches exactly what your producer sends
# Spark uses this to parse the JSON bytes from Kafka
stock_schema = StructType([
    StructField("symbol",    StringType(),    True),
    StructField("timestamp", StringType(),    True),  # ISO string from producer
    StructField("price",     DoubleType(),    True),
    StructField("volume",    LongType(),      True),
    StructField("source",    StringType(),    True),
])


# ─── READ FROM KAFKA ─────────────────────────────────────────────────
def read_kafka_stream(spark):
    """
    Creates a streaming DataFrame from Kafka.

    Kafka gives Spark these columns automatically:
    ┌──────────┬───────────────────────────────────────────┐
    │ column   │ description                               │
    ├──────────┼───────────────────────────────────────────┤
    │ key      │ message key (symbol) as bytes             │
    │ value    │ message payload (JSON) as bytes           │
    │ topic    │ topic name                                │
    │ partition│ which partition                           │
    │ offset   │ offset within partition                   │
    │ timestamp│ when Kafka received the message           │
    └──────────┴───────────────────────────────────────────┘

    startingOffsets="latest" → only process new messages
    startingOffsets="earliest" → reprocess from beginning
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ─── PARSE AND TRANSFORM ─────────────────────────────────────────────
def parse_stream(raw_df):
    """
    Raw Kafka DataFrame has value as bytes.
    We need to:
    1. Cast bytes → string
    2. Parse JSON string → typed columns using our schema
    3. Cast timestamp string → proper TimestampType
    4. Select only the columns we need
    """
    return (
        raw_df
        # Step 1: cast binary value to string
        .withColumn("value_str", col("value").cast(StringType()))

        # Step 2: parse JSON string into struct using schema
        # from_json returns a single struct column
        .withColumn("data", from_json(col("value_str"), stock_schema))

        # Step 3: expand struct into individual columns
        .select(
            col("data.symbol").alias("symbol"),
            col("data.price").alias("price"),
            col("data.volume").alias("volume"),
            col("data.source").alias("source"),
            # Cast ISO timestamp string to proper TimestampType
            # Spark needs TimestampType for windowing functions
            col("data.timestamp").cast(TimestampType()).alias("timestamp"),
            # Also keep Kafka's own timestamp for comparison
            col("timestamp").alias("kafka_timestamp"),
        )
        # Drop rows where parsing failed (malformed JSON)
        .filter(col("symbol").isNotNull())
    )


# ─── STREAM 1: RAW TICKS ─────────────────────────────────────────────
def write_raw_ticks(parsed_df):
    """
    Output mode 'append': only new rows written each micro-batch.
    Good for raw events — you never update or delete a tick.

    This stream prints every single tick to console.
    In Phase 6 this becomes a Redis write instead.
    """
    return (
        parsed_df
        .select("symbol", "price", "volume", "timestamp", "source")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .queryName("raw_ticks")
        .trigger(processingTime="5 seconds")
        .start()
    )


# ─── STREAM 2: 5-MINUTE WINDOWED AGGREGATIONS ────────────────────────
def write_windowed_aggregations(parsed_df):
    """
    Groups ticks into 5-minute tumbling windows per symbol.
    Computes avg/min/max price and total volume per window.

    withWatermark: tells Spark to wait up to 1 minute for
    late-arriving data before closing a window.

    Output mode 'update': output rows that changed in this batch.
    Required for aggregations with watermark.
    'complete' would output ALL windows every batch (too noisy).
    'append' not allowed for aggregations (results can update).
    """
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
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .queryName("windowed_aggregations")
        .trigger(processingTime="10 seconds")
        # Checkpoint saves progress so job can resume after crash
        .option("checkpointLocation", f"{CHECKPOINT}/windowed")
        .start()
    )


# ─── STREAM 3: PRICE ALERTS ──────────────────────────────────────────
def write_price_alerts(parsed_df):
    """
    Simple stateless alert: flag any tick where price moves
    more than 0.3% from the previous known price.

    This is stateless (per-tick) so output mode is 'append'.
    In Phase 5 we'll make this stateful using Spark's
    mapGroupsWithState for proper cross-batch memory.
    """
    # For now: flag ticks with unusually high volume as alerts
    # (volume spike often precedes price movement)
    alerts = (
        parsed_df
        .filter(col("volume") > 1_000_000)
        .select(
            col("symbol"),
            col("price"),
            col("volume"),
            col("timestamp"),
        )
    )

    return (
        alerts
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .queryName("price_alerts")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT}/alerts")
        .start()
    )


# ─── MAIN ────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Spark Structured Streaming — Stock Pipeline")
    print("=" * 55)

    spark = create_spark_session()

    # Suppress noisy Spark INFO logs — only show warnings + errors
    spark.sparkContext.setLogLevel("WARN")

    # Build the streaming pipeline
    raw_df    = read_kafka_stream(spark)
    parsed_df = parse_stream(raw_df)

    # Start all three streams concurrently
    q1 = write_raw_ticks(parsed_df)
    q2 = write_windowed_aggregations(parsed_df)
    q3 = write_price_alerts(parsed_df)

    print(f"\n  Active streaming queries:")
    print(f"  → {q1.name} (trigger: 5s)")
    print(f"  → {q2.name} (trigger: 10s)")
    print(f"  → {q3.name} (trigger: 5s)")
    print(f"\n  Waiting for data... (Ctrl+C to stop)\n")

    # awaitAnyTermination blocks until any query fails or is stopped
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()