import json
import os
import sys
import random
import time

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv

from common import BaseKafkaProducer

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_MARKET_DATA", "market_data")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "etf_config.json")


class MarketDataProducer(BaseKafkaProducer):
    def __init__(self, config_file):
        super().__init__(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="MarketDataSimulator"
        )
        self.config = self._load_config(config_file)
        self.components = self.config["components"]

        # Initialize current prices
        self.current_prices = {
            symbol: data["initial_price"] for symbol, data in self.components.items()
        }

    def _load_config(self, config_file):
        with open(config_file, "r") as f:
            return json.load(f)

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
        self.logger.info(f"Starting Market Data Producer. Target topic: {KAFKA_TOPIC}")
        self.logger.info(f"Simulating market for: {', '.join(self.components.keys())}")

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

                self.logger.info(f"Sent: {event['symbol']} @ {event['price']}")

                # Sleep to simulate real-time frequency (10-100ms)
                time.sleep(random.uniform(0.01, 0.1))

        except KeyboardInterrupt:
            self.logger.info("Stopping producer...")
        finally:
            self.shutdown()


if __name__ == "__main__":
    producer = MarketDataProducer(CONFIG_FILE)
    producer.run()
