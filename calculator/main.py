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
