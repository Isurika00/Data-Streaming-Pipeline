from confluent_kafka import Consumer, Producer
import json
from datetime import datetime, timedelta
import pymongo
from bson.objectid import ObjectId
import psycopg2
import logging
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get configuration from environment variables
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

# MongoDB connection for raw, unprocessed data
mongo_client = pymongo.MongoClient(mongodb_uri)
raw_db = mongo_client["smartwatch_raw"]
raw_collection = raw_db["events"]

# TimescaleDB (PostgreSQL) connection for processed data
timescale_conn = psycopg2.connect(postgres_uri)
timescale_cursor = timescale_conn.cursor()

# Initialize Kafka consumer
consumer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'smartwatch-processor',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['smartwatch-raw-data'])

# Initialize Kafka producer for processed data
producer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers
}
producer = Producer(producer_conf)

# In-memory storage for temporary aggregations
user_daily_metrics = {}  


def store_raw_data(data):
    """Store raw, unprocessed data in MongoDB only"""
    try:
        # Add metadata
        data['received_at'] = datetime.now().isoformat()
        
        # Insert into MongoDB
        result = raw_collection.insert_one(data)
        mongo_id = result.inserted_id
        logger.debug(f"Stored raw data in MongoDB with ID: {mongo_id}")
        
        return True
    except Exception as e:
        logger.error(f"Error storing raw data in MongoDB: {e}")
        return False

# Main processing loop
def main():
    logger.info("Starting processing...")

    try:
        logger.info("Starting stream processor...")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode('utf-8'))
                raw_id = store_raw_data(value)

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")

    finally:
        consumer.close()
        timescale_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()