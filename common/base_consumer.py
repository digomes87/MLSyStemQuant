import json
import logging
import signal
from abc import ABC, abstractmethod

from confluent_kafka import Consumer, KafkaError


class BaseKafkaConsumer(ABC):
    """
    Abstract base class for Kafka Consumer services.
    Encapsulates common logic for connection, polling, and signal handling.
    """

    def __init__(
        self,
        bootstrap_servers,
        group_id,
        topics,
        auto_offset_reset="earliest",
        handle_signals=True,
        poll_timeout=1.0,
    ):
        self.running = True
        self.poll_timeout = poll_timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        self.consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe(topics)
        self.logger.info(f"Subscribed to topics: {topics}")

        # Register signal handlers
        if handle_signals:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, *_):
        """Handles shutdown signals gracefully."""
        self.logger.info("Received shutdown signal. Stopping...")
        self.running = False

    def _is_valid_message(self, msg):
        """Checks if a message is valid and handles errors."""
        if msg is None:
            self.on_poll_timeout()
            return False

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                self.logger.error(f"Kafka error: {msg.error()}")
            return False

        return True

    def run(self):
        """Main consumption loop."""
        self.logger.info("Starting consumer loop...")
        try:
            while self.running:
                msg = self.consumer.poll(self.poll_timeout)

                if not self._is_valid_message(msg):
                    continue

                try:
                    self.process_message(msg)
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)

        except Exception as e:
            self.logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self):
        """Cleanup resources."""
        self.logger.info("Closing consumer...")
        self.consumer.close()

    def on_poll_timeout(self):
        """Hook for actions when poll times out (no message received)."""
        pass

    @abstractmethod
    def process_message(self, msg):
        """
        Abstract method to process a valid Kafka message.
        Must be implemented by subclasses.
        """
        pass

    def decode_json(self, msg):
        """Helper to decode JSON message value."""
        try:
            return json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON Decode Error: {e}")
            return None
