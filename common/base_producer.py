import logging
import signal
from abc import ABC, abstractmethod

from confluent_kafka import Producer


class BaseKafkaProducer(ABC):
    """
    Abstract base class for Kafka Producers.
    Encapsulates connection setup, signal handling, and delivery reports.
    """

    def __init__(self, bootstrap_servers, client_id=None):
        self.running = True
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id or self.__class__.__name__,
        }
        self.producer = Producer(conf)
        self.logger.info(f"Producer initialized: {conf}")

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, *_):
        self.logger.info("Received shutdown signal. Stopping...")
        self.running = False

    def delivery_report(self, err):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        # else:
        # self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def shutdown(self):
        """Clean shutdown."""
        self.logger.info("Flushing producer...")
        self.producer.flush()
        self.logger.info("Producer stopped.")

    @abstractmethod
    def run(self):
        """Main loop to be implemented by subclasses."""
        pass
