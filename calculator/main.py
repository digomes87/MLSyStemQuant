import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_MARKET = os.getenv("KAFKA_TOPIC_MARKET_DATA", "market_data")
KAFKA_TOPIC_NAV = os.getenv("KAFKA_TOPIC_ETF_NAV", "etf_nav")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "nav_calculator_group")
CONFIG_FILE = "etf_config.json"  # Assuming it's in the same directory or passed


class NAVCalculator:
    def __init__(self, config_file):
        self.running = True
        self.config = self._load_config(config_file)

        # kafka setup
        self.consumer = Consumer(
            {
                "bootstrap.server": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
            }
        )

        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

        # State
        self.components = self.config["components"]
        self.current_prices = {
            symbol: data["initial_price"] for symbol, data in self.components.items()
        }

        self.shares_outstanding = self.config["outstanding_shares"]
        self.cash_component = self.config["cash_component"]

        # subscribe to market data
        self.consumer.subscribe({KAFKA_TOPIC_MARKET})

    def _load_config(self, config_file):
        path = os.path.join(os.path.dirname(__file__), "..", "producer", config_file)
        with open(path, "r") as f:
            return json.load(f)

    def calculate_nav(self):
        """Calculates the current NAV based on latest component prices"""
        total_asset_value = self.cash_component

        for symbol, data in self.components.items():
            price = self.current_prices.get(symbol, 0)
            holdings = data["shares"]
            total_asset_value += price * holdings

        nav = total_asset_value / self.shares_outstanding
        return round(nav, 4)

    def process_message(self, msg):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    calculator = NAVCalculator(CONFIG_FILE)

    def signal_handler(*_):
        print("\nGracefully shutting donw ... ")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    calculator.run()
