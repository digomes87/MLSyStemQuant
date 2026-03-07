import json
import os
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

from common import BaseKafkaConsumer

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_MARKET = os.getenv("KAFKA_TOPIC_MARKET_DATA", "market_data")
KAFKA_TOPIC_NAV = os.getenv("KAFKA_TOPIC_ETF_NAV", "etf_nav")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "nav_calculator_group")
CONFIG_FILE = "etf_config.json"


class NAVCalculator(BaseKafkaConsumer):
    def __init__(self, config_file):
        # Initialize base consumer
        super().__init__(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            topics=[KAFKA_TOPIC_MARKET],
        )

        self.config = self._load_config(config_file)
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

        # State
        self.components = self.config["components"]
        self.current_prices = {
            symbol: data["initial_price"] for symbol, data in self.components.items()
        }
        self.shares_outstanding = self.config["outstanding_shares"]
        self.cash_component = self.config["cash_component"]

    def _load_config(self, config_file):
        path = os.path.join(os.path.dirname(__file__), "..", "producer", config_file)
        with open(path, "r") as f:
            return json.load(f)

    def calculate_nav(self):
        """Calculates the current NAV based on latest component prices."""
        total_asset_value = self.cash_component

        for symbol, data in self.components.items():
            price = self.current_prices.get(symbol, 0)
            holdings = data["shares"]
            total_asset_value += price * holdings

        nav = total_asset_value / self.shares_outstanding
        return round(nav, 4)

    def process_message(self, msg):
        data = self.decode_json(msg)
        if not data:
            return

        try:
            symbol = data["symbol"]
            price = data["price"]

            # Update state
            if symbol in self.current_prices:
                self.current_prices[symbol] = price

                # Recalculate NAV
                nav = self.calculate_nav()

                # Produce NAV update
                nav_update = {
                    "etf": self.config["symbol"],
                    "nav": nav,
                    "timestamp": time.time(),
                    "trigger_symbol": symbol,
                    "trigger_price": price,
                }

                self.producer.produce(
                    KAFKA_TOPIC_NAV,
                    key=self.config["symbol"],
                    value=json.dumps(nav_update),
                )
                # Poll to handle delivery reports
                self.producer.poll(0)

                self.logger.info(f"Updated NAV: {nav} (Trigger: {symbol} @ {price})")

        except KeyError as e:
            self.logger.error(f"Missing key in data: {e}")

    def teardown(self):
        """Ensure producer is flushed on shutdown."""
        self.logger.info("Flushing producer...")
        self.producer.flush()


if __name__ == "__main__":
    calculator = NAVCalculator(CONFIG_FILE)
    calculator.run()
