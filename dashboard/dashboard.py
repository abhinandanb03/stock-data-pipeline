import json
import time
import threading

import streamlit as st
import plotly.graph_objects as go
import redis
import psycopg2
import psycopg2.extras

# CONFIG
REDIS_HOST   = "localhost"
REDIS_PORT   = 6379
POSTGRES_DSN = "host=localhost port=5432 dbname=stocks user=stockuser password=stockpass"
SYMBOLS      = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA"]
HISTORY_LEN  = 100

SYMBOL_COLORS = {
    "AAPL": "#00C896",
    "GOOGL": "#FF6B6B",
    "MSFT": "#4ECDC4",
    "TSLA": "#FFE66D",
    "AMZN": "#A78BFA",
    "NVDA": "#F97316",
}

# PAGE CONFIG
st.set_page_config(
    page_title = "Stock Pipeline Dashboard",
    page_icon = "📈",
    layout = "wide",
)

# CONNECTIONS
@st.cache_resource
def get_redis():
    return redis.Redis(host = REDIS_HOST, port = REDIS_PORT, decode_responses = True)

@st.cache_resource
def get_postgres():
    return psycopg2.connect(POSTGRES_DSN)


# BACKGROUND PUBSUB LISTENER
def start_redis_listener():
    """
    Runs in a daemon background thread — starts once, lives for the
    entire Streamlit session.

    How it works:
      1. Opens a SEPARATE Redis connection dedicated to pub/sub.
         (You cannot use a pub/sub connection for normal commands.)
      2. Subscribes to ticks:SYMBOL for every symbol.
      3. Blocks on pubsub.listen() — an iterator that yields messages
         as Spark publishes them. No polling, pure push.
      4. On each message, updates st.session_state with the new tick.
      5. Calls st.rerun() to tell Streamlit to re-render immediately.

    Why a daemon thread?
      daemon=True means this thread dies automatically when the main
      Streamlit process exits. No cleanup needed.

    Why cache_resource on start_listener_once()?
      st.cache_resource persists across reruns. Without it, a new
      thread would be spawned every time Streamlit reruns (every tick),
      creating hundreds of listener threads that all fight each other.
    """
    r_pubsub = redis.Redis(host = REDIS_HOST, port = REDIS_PORT, decode_responses = True)
    pubsub   = r_pubsub.pubsub()

    # Subscribe to one channel per symbol
    channels = [f"ticks:{s}" for s in SYMBOLS]
    pubsub.subscribe(*channels)
    print(f"[PubSub] Subscribed to: {channels}")

    for message in pubsub.listen():
        # pubsub.listen() yields two message types:
        #   type="subscribe"  → confirmation that subscription worked (ignore)
        #   type="message"    → actual tick from Spark (handle this)
        if message["type"] != "message":
            continue

        try:
            tick = json.loads(message["data"])
            symbol = tick.get("symbol")
            if not symbol:
                continue

            # Update session state with the latest tick per symbol
            # session_state persists across reruns within the same session
            if "live_ticks" not in st.session_state:
                st.session_state["live_ticks"] = {}

            st.session_state["live_ticks"][symbol] = tick
            st.session_state["last_update"] = time.time()

            print(f"[PubSub] {symbol} ${tick.get('price'):.4f}")

        except Exception as e:
            print(f"[PubSub] Error: {e}")
            continue


@st.cache_resource
def start_listener_once(): # Ensures the listener thread starts only once per session
    thread = threading.Thread(target = start_redis_listener, daemon = True)
    thread.start()
    print("[PubSub] Background listener thread started")
    return thread


# DATA FETCHERS
def fetch_latest_prices(r):
    """
    Fallback: reads the latest_prices hash from Redis.
    Used for symbols not yet received via pub/sub,
    and for the initial page load before any pub/sub messages arrive.
    """
    raw = r.hgetall("latest_prices")
    return {k: float(v) for k, v in raw.items()}


def fetch_price_history(r, symbol):
    raw   = r.lrange(f"history:{symbol}", 0, HISTORY_LEN - 1)
    ticks = []
    for item in reversed(raw):
        try:
            d = json.loads(item)
            ticks.append({
                "timestamp": d["timestamp"],
                "price":     float(d["price"]),
            })
        except Exception:
            continue
    return ticks


def fetch_aggregations(conn):
    try:
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT DISTINCT ON (symbol)
                symbol, window_start, window_end,
                avg_price, min_price, max_price, tick_count
            FROM stock_aggregations
            ORDER BY symbol, window_start DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.commit()
        return rows
    except Exception as e:
        st.warning(f"Postgres read error: {e}")
        conn.rollback()
        return []


def fetch_alerts(conn):
    try:
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT symbol, price, volume, alert_type, timestamp
            FROM stock_alerts
            ORDER BY timestamp DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.commit()
        return rows
    except Exception as e:
        st.warning(f"Alerts read error: {e}")
        conn.rollback()
        return []


# CHART BUILDER
def build_multi_symbol_chart(r, normalize):
    fig = go.Figure()
    has_data = False

    for symbol in SYMBOLS:
        ticks = fetch_price_history(r, symbol)
        if not ticks:
            continue

        has_data = True
        timestamps = [t["timestamp"] for t in ticks]
        prices = [t["price"] for t in ticks]
        color = SYMBOL_COLORS.get(symbol, "#ffffff")

        if normalize and prices:
            base   = prices[0]
            y_vals = [round((p - base) / base * 100, 4) for p in prices]
        else:
            y_vals = prices

        fig.add_trace(go.Scatter(
            x = timestamps,
            y = y_vals,
            mode = "lines",
            name = symbol,
            line = dict(color=color, width=2),
            hovertemplate = (
                f"<b>{symbol}</b><br>"
                + ("%{y:.4f}%" if normalize else "$%{y:.4f}")
                + "<br>%{x}<extra></extra>"
            ),
        ))

    if not has_data:
        fig.add_annotation(
            text = "Waiting for data from Spark...",
            xref = "paper", yref = "paper",
            x = 0.5, y = 0.5, showarrow = False,
            font = dict(size = 16, color = "#888888"),
        )

    fig.update_layout(
        margin = dict(l = 0, r = 0, t = 40, b = 0),
        height = 400,
        xaxis = dict(showgrid = False, title = "Time"),
        yaxis = dict(
            showgrid = True,
            gridcolor = "#2a2a2a",
            title = "% Change from Start" if normalize else "Price ($)",
            zeroline = True,
            zerolinecolor = "#444444",
        ),
        plot_bgcolor = "#0e1117",
        paper_bgcolor = "#0e1117",
        font = dict(color = "#ffffff"),
        title = dict(text = "All Symbols — Last 100 Ticks", font = dict(size = 15)),
        legend = dict(orientation = "h", yanchor = "bottom", y = 1.02, xanchor = "right", x = 1),
        hovermode = "x unified",
    )
    return fig


# MAIN DASHBOARD
def main():
    # Start background pub/sub listener (only runs once per session)
    start_listener_once()

    r = get_redis()
    conn = get_postgres()

    # Header
    st.title("Real-Time Stock Pipeline Dashboard")

    # Show whether live pub/sub data is arriving or we're on fallback
    live_ticks = st.session_state.get("live_ticks", {})
    last_update = st.session_state.get("last_update")

    if live_ticks and last_update:
        age = time.time() - last_update
        st.caption(
            f"🟢 Live pub/sub active — last tick {age:.1f}s ago | "
            f"Finnhub → Kafka → Spark → Redis → here"
        )
    else:
        st.caption("🟡 Waiting for pub/sub data... (using Redis hash fallback)")

    # Row 1: Live price cards
    st.subheader("Live Prices")

    # Merge: pub/sub prices (freshest) override hash prices (fallback)
    hash_prices = fetch_latest_prices(r)
    merged_prices = {**hash_prices}
    for symbol, tick in live_ticks.items():
        merged_prices[symbol] = tick["price"]

    if not merged_prices:
        st.info("Waiting for data from Spark...")
    else:
        cols = st.columns(len(SYMBOLS))
        for i, symbol in enumerate(SYMBOLS):
            price     = merged_prices.get(symbol)
            is_live   = symbol in live_ticks
            with cols[i]:
                st.metric(
                    label=f"{symbol} {'🟢' if is_live else '⚪'}",
                    value=f"${price:.4f}" if price else "—",
                )

    st.divider()

    # Row 2: Multi-symbol chart
    st.subheader("Price Chart — All Symbols")
    normalize = st.toggle(
        "Normalize to % change (recommended — makes all symbols comparable)",
        value = True,
    )
    fig = build_multi_symbol_chart(r, normalize)
    st.plotly_chart(fig, use_container_width = True)

    st.divider()

    # Row 3: Latest pub/sub ticks (new section)
    if live_ticks:
        st.subheader("Latest Pub/Sub Ticks")
        tick_cols = st.columns(len(SYMBOLS))
        for i, symbol in enumerate(SYMBOLS):
            tick = live_ticks.get(symbol)
            with tick_cols[i]:
                if tick:
                    st.markdown(f"**{symbol}**")
                    st.markdown(f"`${tick['price']:.4f}`")
                    st.caption(f"vol: {tick['volume']:,}")
                    ts = tick.get("timestamp", "")[:19]
                    st.caption(ts)
        st.divider()

    # Row 4: 5-minute aggregations
    st.subheader("5-Minute Aggregations")
    aggs = fetch_aggregations(conn)

    if not aggs:
        st.info("No aggregations yet — windows close every 5 minutes...")
    else:
        table_data = []
        for row in aggs:
            table_data.append({
                "Symbol": row["symbol"],
                "Window Start": str(row["window_start"])[:19],
                "Window End": str(row["window_end"])[:19],
                "Avg Price": f"${row['avg_price']:.4f}",
                "Min Price": f"${row['min_price']:.4f}",
                "Max Price": f"${row['max_price']:.4f}",
                "Ticks": row["tick_count"],
            })
        st.dataframe(table_data, use_container_width = True, hide_index = True)

    st.divider()

    # Row 5: Alerts
    st.subheader("🚨 Recent Alerts")
    alerts = fetch_alerts(conn)

    if not alerts:
        st.success("No alerts — all quiet!")
    else:
        for alert in alerts:
            st.warning(
                f"**{alert['symbol']}** | "
                f"{alert['alert_type']} | "
                f"Price: ${alert['price']:.4f} | "
                f"Volume: {alert['volume']:,} | "
                f"Time: {str(alert['timestamp'])[:19]}"
            )

    # Auto-rerun
    # Even with pub/sub pushing updates, we still poll every 5s as a
    # safety net for the chart and aggregations table (which read from
    # Redis lists and PostgreSQL, not the pub/sub channel).
    # The pub/sub thread also calls st.rerun() on every tick,
    # so in practice the UI updates much faster than 3s.
    time.sleep(3)
    st.rerun()


if __name__ == "__main__":
    main()