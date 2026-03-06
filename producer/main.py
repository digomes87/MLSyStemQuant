import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_MARKET_DATA", "market_data")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "etf_config.json")


class MarketDataProducer:
    def __init__(self, config_file):
        self.running = True
        self.config = self._load_config(config_file)
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        self.components = self.config["components"]

        # Initialize current prices
        self.current_prices = {
            symbol: data["initial_price"] for symbol, data in self.components.items()
        }

    def _load_config(self, config_file):
        with open(config_file, "r") as f:
            return json.load(f)

    def delivery_report(self, err):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            # Commenting out for less verbose output
            # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            pass

    def generate_price_update(self):
        """Simulates a market tick for a random component."""
        symbol = random.choice(list(self.components.keys()))
        current_price = self.current_prices[symbol]

        # Random walk: -0.5% to +0.5% change
        change_pct = random.uniform(-0.005, 0.005)
        new_price = round(current_price * (1 + change_pct), 2)
        self.current_prices[symbol] = new_price

        return {"symbol": symbol, "price": new_price, "timestamp": time.time()}

    def run(self):
        print(f"Starting Market Data Producer. Target topic: {KAFKA_TOPIC}")
        print(f"Simulating market for: {', '.join(self.components.keys())}")

        try:
            while self.running:
                event = self.generate_price_update()

                # Produce to Kafka
                self.producer.produce(
                    KAFKA_TOPIC,
                    key=event["symbol"],
                    value=json.dumps(event),
                    callback=self.delivery_report,
                )

                # Poll to handle delivery reports
                self.producer.poll(0)

                print(f"Sent: {event['symbol']} @ {event['price']}")

                # Sleep to simulate real-time frequency (10-100ms)
                time.sleep(random.uniform(0.01, 0.1))

        except KeyboardInterrupt:
            print("\nStopping producer...")
        finally:
            self.producer.flush()


if __name__ == "__main__":
    producer = MarketDataProducer(CONFIG_FILE)

    def signal_handler(*_):
        print("\nGracefully shutting down...")
        producer.running = False
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    producer.run()
