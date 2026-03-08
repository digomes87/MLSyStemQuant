import json
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3
import snowflake.connector
from dotenv import load_dotenv

from common import BaseKafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAV = os.getenv("KAFKA_TOPIC_ETF_NAV", "etf_nav")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "etl_group")
BATCH_SIZE = 100  # Number of messages to buffer before upload
BATCH_INTERVAL = 30  # Seconds to wait before upload

MOCK_AWS = os.getenv("AWS_ACCESS_KEY_ID") == "mock_access_key"
MOCK_SNOWFLAKE = os.getenv("SNOWFLAKE_USER") == "mock_user"


class ETLService(BaseKafkaConsumer):
    def __init__(self):
        super().__init__(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            topics=[KAFKA_TOPIC_NAV],
        )

        self.buffer = []
        self.last_upload_time = time.time()

    def upload_to_s3(self, data):
        """Sure this is a simulate uploading data to s3"""
        if MOCK_AWS:
            self.logger.info(
                f"[MOCK S3] Uploading batch of {len(data)} records to s3://{os.getenv('S3_BUCKET_NAME')}/nav_data"
            )
            output_dir = "data/nav_history"
            os.makedirs(output_dir, exist_ok=True)

            filename = f"{output_dir}/nav_data_{int(time.time())}.json"
            with open(filename, "w") as f:
                json.dump(data, f)
            self.logger.info(f"[MOCK S3] Data written to local file: {filename}")
        else:
            s3 = boto3.client("s3")
            print(f"Local of S3 {s3}")
            pass

    def load_to_snowflake(self, data):
        """Another simulate"""
        if MOCK_SNOWFLAKE:
            self.logger.info(
                f"[MOCK SNOWFLAKE] loading {len(data)} records into TABLE ETF_NAV_HISTORY"
            )
        else:
            ctx = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
            )

            cs = ctx.cursor()
            cs.close()
            ctx.close()

    def flush_buffer(self):
        if not self.buffer:
            return

        self.logger.info(f"Flushing buffer ({len(self.buffer)}) records")

        try:
            self.upload_to_s3(self.buffer)
            self.load_to_snowflake(self.buffer)
            self.buffer = []
            self.last_upload_time = time.time()
        except Exception as e:
            self.logger.error(f"Error during flush: {e}", exc_info=True)

    def process_message(self, msg):
        data = self.decode_json(msg)
        if data:
            self.buffer.append(data)

            if len(self.buffer) >= BATCH_SIZE:
                self.flush_buffer()

    def on_poll_timeout(self):
        if time.time() - self.last_upload_time > BATCH_INTERVAL and self.buffer:
            self.flush_buffer()

    def teardown(self):
        self.flush_buffer()


if __name__ == "__main__":
    etl = ETLService()
    etl.run()
