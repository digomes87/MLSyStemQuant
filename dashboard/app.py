import importlib
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

import common
import common.base_consumer
from common import BaseKafkaConsumer

importlib.reload(common.base_consumer)
importlib.reload(common)

load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_MARKET = os.getenv("KAFKA_TOPIC_MARKET_DATA", "market_data")
KAFKA_TOPIC_NAV = os.getenv("KAFKA_TOPIC_ETF_NAV", "etf_nav")
KAFKA_GROUP_ID = "dashboard_group_" + str(time.time())

st.set_page_config(
    page_title="Real-Time ETF NAV Monitor",
    layout="wide",
)

st.title("Real-Time ETF NAV Monitor")

with st.sidebar:
    st.header("Configuration")
    st.write(f"**Kafka Broker:** {KAFKA_BOOTSTRAP_SERVERS}")
    st.write(f"**Topics:** {KAFKA_TOPIC_MARKET}, {KAFKA_TOPIC_NAV}")

    if st.button("Clear History"):
        st.session_state.nav_history = []
        st.session_state.market_history = {}
        st.rerun()

if "nav_history" not in st.session_state:
    st.session_state.nav_history = []
if "market_history" not in st.session_state:
    st.session_state.market_history = {}


class DashboardConsumer(BaseKafkaConsumer):
    """
    Kafka Consumer for Streamlit Dashboard.
    Inherits from BaseKafkaConsumer for connection handling but allows

    """

    def __init__(self):
        super().__init__(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            topics=[KAFKA_TOPIC_MARKET, KAFKA_TOPIC_NAV],
            auto_offset_reset="latest",
            handle_signals=False,  # Streamlit handles signals
            poll_timeout=0.1,
        )

    def update_nav(self, data, current_time):
        """Updates NAV history and metrics."""
        nav_entry = {"Time": current_time, "NAV": data["nav"], "ETF": data["etf"]}
        st.session_state.nav_history.append(nav_entry)

        if len(st.session_state.nav_history) > 100:
            st.session_state.nav_history.pop(0)

        current_nav = data["nav"]
        trigger = (
            f"{data.get('trigger_symbol', 'N/A')} @ {data.get('trigger_price', 0)}"
        )
        nav_metric_placeholder.metric(
            "Current NAV", f"${current_nav:.2f}", delta=trigger
        )

    def update_market_data(self, data, current_time):
        """Updates market data history."""
        symbol = data["symbol"]
        price = data["price"]

        if symbol not in st.session_state.market_history:
            st.session_state.market_history[symbol] = {
                "price": price,
                "last_update": current_time,
            }
        else:
            prev_price = st.session_state.market_history[symbol]["price"]
            st.session_state.market_history[symbol] = {
                "price": price,
                "change": price - prev_price,
                "last_update": current_time,
            }

    def render_visualizations(self):
        """Renders charts and tables."""
        if st.session_state.nav_history:
            df_nav = pd.DataFrame(st.session_state.nav_history)

            chart = (
                alt.Chart(df_nav)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Time", axis=alt.Axis(format="%H:%M:%S")),
                    y=alt.Y("NAV", scale=alt.Scale(zero=False)),
                    tooltip=["Time", "NAV"],
                )
                .properties(height=300)
                .interactive()
            )

            nav_chart_placeholder.altair_chart(chart, use_container_width=True)

        if st.session_state.market_history:
            market_data = []
            for sym, info in st.session_state.market_history.items():
                market_data.append(
                    {
                        "Symbol": sym,
                        "Price": f"${info['price']:.2f}",
                        "Change": f"{info.get('change', 0):.2f}",
                    }
                )
            df_market = pd.DataFrame(market_data)
            market_table_placeholder.dataframe(df_market, hide_index=True)

    def process_message(self, msg):
        """Processes incoming Kafka messages and updates state."""
        data = self.decode_json(msg)
        if not data:
            return

        topic = msg.topic()
        ts = data.get("timestamp", time.time())
        current_time = pd.to_datetime(ts, unit="s")

        if topic == KAFKA_TOPIC_NAV:
            self.update_nav(data, current_time)
        elif topic == KAFKA_TOPIC_MARKET:
            self.update_market_data(data, current_time)

        self.render_visualizations()


col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("ETF Net Asset Value (NAV)")
    nav_chart_placeholder = st.empty()
    nav_metric_placeholder = st.empty()

with col2:
    st.subheader("Component Prices")
    market_table_placeholder = st.empty()

try:
    consumer_service = DashboardConsumer()
    status_placeholder = st.empty()
    status_placeholder.success("Connected to Kafka Stream")

    # Debug: List available topics to verify connection
    try:
        cluster_metadata = consumer_service.consumer.list_topics(timeout=5.0)
        topics = list(cluster_metadata.topics.keys())
        st.sidebar.markdown("**Kafka Connection:** OK")
        st.sidebar.markdown(f"**Topics Found:** {', '.join(topics)}")
    except Exception as e:
        st.sidebar.error(f"Failed to connect to Kafka: {e}")

    msg_count = 0
    msg_placeholder = st.sidebar.empty()
    last_msg_placeholder = st.sidebar.empty()
    error_placeholder = st.sidebar.empty()

    while True:
        msg = consumer_service.consumer.poll(0.1)

        if not consumer_service._is_valid_message(msg):
            continue

        msg_count += 1
        if msg_count % 10 == 0:
            msg_placeholder.markdown(f"**Messages Received:** {msg_count}")

        try:
            # Debug: Try to decode manually to show in sidebar if needed
            try:
                raw_val = msg.value().decode("utf-8")
                if msg_count % 10 == 0:
                    last_msg_placeholder.text(f"Last raw: {raw_val[:50]}...")
            except Exception as e:
                print(f"Error {e}")

            consumer_service.process_message(msg)
        except Exception as e:
            error_placeholder.error(f"Process Error: {e}")


except Exception as e:
    st.error(f"Could not initialize Kafka Consumer: {e}")
