import json
from collections import defaultdict, deque
from datetime import datetime
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# CONFIG 
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "stock-prices"
GROUP_ID       = "stock-analytics-group"

SMA_WINDOW     = 10      # rolling window size for moving average
ALERT_THRESHOLD = 0.005  # alert if price deviates > 0.5% from SMA


# deque with maxlen automatically drops oldest item when full. e.g. maxlen=10 → always holds last 10 prices
price_windows = defaultdict(lambda: deque(maxlen=SMA_WINDOW))

session_stats = defaultdict(lambda: {
    "ticks": 0,
    "min": float("inf"),
    "max": float("-inf"),
    "alerts": 0,
})


# ANALYTICS FUNCTIONS 
def compute_sma(symbol):
    window = price_windows[symbol]
    if len(window) < 2:
        return None
    return sum(window) / len(window)


def check_alert(symbol, price, sma):
    # deviation = |price - sma| / sma
    if sma is None:
        return None

    deviation = abs(price - sma) / sma
    if deviation > ALERT_THRESHOLD:
        direction = "⬆ SURGE" if price > sma else "⬇ DROP"
        return f"{direction} {deviation*100:.3f}% from SMA ${sma:.4f}"
    return None


def process_tick(record):
    # record.key and record.value are bytes — decode and parse JSON to get symbol and price
    symbol = record.key.decode("utf-8")
    data   = json.loads(record.value.decode("utf-8"))
    price  = data["price"]

    # Update rolling window
    price_windows[symbol].append(price)

    # Update session stats
    stats = session_stats[symbol]
    stats["ticks"] += 1
    stats["min"]    = min(stats["min"], price)
    stats["max"]    = max(stats["max"], price)

    # Compute SMA
    sma   = compute_sma(symbol)
    alert = check_alert(symbol, price, sma)

    if alert:
        stats["alerts"] += 1

    # Format output line
    sma_str   = f"SMA=${sma:.4f}" if sma else "SMA=building..."
    alert_str = f"  🚨 ALERT: {alert}" if alert else ""

    print(
        f"  [{record.partition}:{record.offset:>6}] "
        f"{symbol:6s} "
        f"${price:.4f}  "
        f"{sma_str}"
        f"{alert_str}"
    )


# CONSUMER SETUP ─
def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers = KAFKA_BROKER, # initial connection point to Kafka cluster
        group_id = GROUP_ID, # consumer group name for offset tracking and partition assignment
        auto_offset_reset = "earliest", # start from earliest message if no committed offset
        enable_auto_commit = False, # disable auto-commit to ensure we only commit after processing
        max_poll_records = 50, # limit messages per poll to control batch size
        session_timeout_ms = 30000, # if no heartbeat received in 30s, broker considers consumer dead and reassigns partitions
        heartbeat_interval_ms = 10000, # send heartbeat every 10s to keep session alive (should be < session_timeout_ms
    )


# PRINT SESSION SUMMARY 
def print_summary():
    print("\n" + "=" * 55)
    print("  SESSION SUMMARY")
    print("=" * 55)
    print(f"  {'Symbol':<8} {'Ticks':>6} {'Min':>10} {'Max':>10} {'Alerts':>7}")
    print("-" * 55)
    for symbol, stats in sorted(session_stats.items()):
        mn = f"${stats['min']:.4f}" if stats["min"] != float("inf") else "N/A"
        mx = f"${stats['max']:.4f}" if stats["max"] != float("-inf") else "N/A"
        print(
            f"  {symbol:<8} "
            f"{stats['ticks']:>6} "
            f"{mn:>10} "
            f"{mx:>10} "
            f"{stats['alerts']:>7}"
        )
    print("=" * 55)


# MAIN LOOP 
def main():
    print("=" * 55)
    print("  Stock Analytics Consumer")
    print("=" * 55)
    print(f"  Broker   : {KAFKA_BROKER}")
    print(f"  Topic    : {TOPIC}")
    print(f"  Group    : {GROUP_ID}")
    print(f"  SMA Win  : {SMA_WINDOW} ticks")
    print(f"  Alert    : >{ALERT_THRESHOLD*100}% deviation from SMA")
    print("=" * 55)
    print(f"\n  {'Part:Offset':<15} {'Symbol':<8} {'Price':<12} {'SMA'}")
    print("-" * 55)

    consumer = create_consumer()

    try:
        while True:
            # poll() returns a dict of {TopicPartition: [ConsumerRecord, ...]}  
            records = consumer.poll(timeout_ms=1000)

            if not records:
                # No messages - outside market hours
                print("  ... waiting for messages (market may be closed)")
                continue

            batch_count = 0
            for topic_partition, batch in records.items():
                for record in batch:
                    process_tick(record)
                    batch_count += 1

            # After processing the batch, commit offsets to mark messages as consumed
            consumer.commit()
            print(f"  ✓ Committed offsets for {batch_count} messages\n")

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
        print_summary()
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()