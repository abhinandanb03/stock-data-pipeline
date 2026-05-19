import json
import time
import threading
from datetime import datetime, timezone
import websocket
from kafka import KafkaProducer

# CONFIG 
FINNHUB_API_KEY = "d85lgh9r01qitd92jp2gd85lgh9r01qitd92jp30"
KAFKA_BROKER    = "127.0.0.1:9092"
TOPIC           = "stock-prices"

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA"]

# KAFKA PRODUCER SETUP 
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        retries=3,
        retry_backoff_ms=300, # retry after 300ms if send fails
        linger_ms=50,
    )

producer = create_producer()


# MESSAGE HANDLER 
def on_message(ws, raw_message):
    # finnhub sends various message types (trade ticks, pings, etc) — we only care about trade ticks
    msg = json.loads(raw_message)

    if msg.get("type") != "trade":
        return

    # Each message can contain multiple trade ticks batched together
    for tick in msg.get("data", []):
        symbol    = tick["s"]
        price     = tick["p"]
        volume    = tick["v"]
        # Finnhub timestamps are in milliseconds — convert to ISO string
        ts_ms     = tick["t"]
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()

        kafka_msg = {
            "symbol":    symbol,
            "timestamp": timestamp,
            "price":     price,
            "volume":    volume,
            "source":    "finnhub_live",
        }

        print(f"  📈 {symbol:6s} ${price:.4f}  vol={volume}")

        producer.send(
            topic=TOPIC,
            key=symbol,
            value=kafka_msg
        )

    # Flush after processing each batch of ticks
    producer.flush()


def on_error(ws, error):
    print(f"[ERROR] WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSED] WebSocket closed: {close_status_code} {close_msg}")


def on_open(ws):
    # Subscribe to trade ticks for each symbol when connection opens
    print(f"[CONNECTED] WebSocket open → subscribing to {SYMBOLS}\n")
    for symbol in SYMBOLS:
        ws.send(json.dumps({
            "type":   "subscribe",
            "symbol": symbol
        }))
        print(f"  Subscribed to {symbol}")
    print()


# RECONNECT LOOP
def run_with_reconnect():
    # Finnhub WebSocket endpoint with API key for authentication
    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    while True:
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')} UTC] Connecting to Finnhub...")

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(ping_interval=30, ping_timeout=10)

        print("[RECONNECT] Connection lost — retrying in 5s...")
        time.sleep(5)


# MAIN
if __name__ == "__main__":
    print("=" * 50)
    print("  Finnhub → Kafka Real-Time Stock Producer")
    print("=" * 50)
    print(f"  Broker : {KAFKA_BROKER}")
    print(f"  Topic  : {TOPIC}")
    print(f"  Symbols: {SYMBOLS}")
    print("=" * 50 + "\n")

    try:
        run_with_reconnect()
    except KeyboardInterrupt:
        print("\nShutting down...")
        producer.close()
        print("Producer closed.")