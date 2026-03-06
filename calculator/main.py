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
        if msg.error():
            if msg.error().code == KafkaError._PARTITION_EOF:
                sys.stderr.write(
                    "%% %s [%d] reached and at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                raise KafkaError(msg.error())
        else:
            try:
                data = json.loads(msg.value().decode("utf-8"))
                symbol = data["symbol"]
                price = data["price"]
                # timestamp = data["timestamp"]

                if symbol in self.current_prices:
                    self.current_prices[symbol] = price

                    nav = self.calculate_nav()

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

                    self.producer.poll(0)

                    print(f"Updated NAV: {nav} (Trigger: {symbol} @ {price})")
            except json.JSONDecodeError:
                print(f"Error decoding JSON: {msg.value()}")
            except KeyError as e:
                print(f"Missing key in data: {e}")

    def run(self):
        print(
            f"Starting NAV Calculator. Consuming: {KAFKA_TOPIC_MARKET}, Producing: {KAFKA_TOPIC_NAV}"
        )

        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                self.process_message(msg)

        except KeyboardInterrupt:
            print("\nstopping calculator ...")
        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == "__main__":
    calculator = NAVCalculator(CONFIG_FILE)

    def signal_handler(*_):
        print("\nGracefully shutting donw ... ")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    calculator.run()
