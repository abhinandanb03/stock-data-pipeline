from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, max, min,
    count, round as spark_round, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)

# CONFIG 
KAFKA_BROKER = "kafka:29092"
TOPIC = "stock-prices"
CHECKPOINT = "/opt/spark-data/checkpoints"


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


# RAW TICKS
def write_raw_ticks(parsed_df):
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


# 5-MINUTE WINDOWED AGGREGATIONS
def write_windowed_aggregations(parsed_df):
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


# PRICE ALERTS
def write_price_alerts(parsed_df):
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


# MAIN
def main():
    print("=" * 55)
    print("  Spark Structured Streaming — Stock Pipeline")
    print("=" * 55)

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    raw_df    = read_kafka_stream(spark)
    parsed_df = parse_stream(raw_df)

    # three streams concurrently
    q1 = write_raw_ticks(parsed_df)
    q2 = write_windowed_aggregations(parsed_df)
    q3 = write_price_alerts(parsed_df)

    print(f"\n  Active streaming queries:")
    print(f"  → {q1.name} (trigger: 5s)")
    print(f"  → {q2.name} (trigger: 10s)")
    print(f"  → {q3.name} (trigger: 5s)")
    print(f"\n  Waiting for data... (Ctrl+C to stop)\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()