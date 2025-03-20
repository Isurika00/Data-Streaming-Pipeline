from confluent_kafka import Consumer, Producer
import json
from datetime import datetime
import pymongo
import psycopg2
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get configuration from environment variables
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

# MongoDB connection
mongo_client = pymongo.MongoClient(mongodb_uri)
raw_db = mongo_client["smartwatch_raw"]
raw_collection = raw_db["events"]

# PostgreSQL (TimescaleDB) connection
timescale_conn = psycopg2.connect(postgres_uri)
timescale_cursor = timescale_conn.cursor()

# Kafka Consumer
consumer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'smartwatch-processor',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['smartwatch-raw-data'])

def store_raw_data(data):
    """Store new raw data in MongoDB only if it does not already exist."""
    try:
        event_id = data.get('event_id')
        if event_id and raw_collection.find_one({"event_id": event_id}):
            logger.info(f"Skipping duplicate event: {event_id}")
            return False  # Skip duplicate records

        data['received_at'] = datetime.now().isoformat()
        result = raw_collection.insert_one(data)
        logger.info(f"Stored new raw data in MongoDB with ID: {result.inserted_id}")
        return True
    except Exception as e:
        logger.error(f"Error storing raw data in MongoDB: {e}")
        return False

def check_client_status():
    """Check client status every minute."""
    logger.info("Checking client status...")
    # Implement the logic for client checking (e.g., checking a database, API call, etc.)

def main():
    last_check_time = datetime.now()
    
    try:
        logger.info("Starting data pipeline...")
        while True:
            msg = consumer.poll(1.0)
            
            current_time = datetime.now()
            if (current_time - last_check_time).total_seconds() >= 60:
                check_client_status()
                last_check_time = current_time
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                logger.info("Received new Kafka message.")

                if store_raw_data(data):
                    logger.info("Stored in MongoDB.")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        consumer.close()
        timescale_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()
