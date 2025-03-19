from confluent_kafka import Consumer, Producer
import json
from datetime import datetime
import pymongo
import psycopg2
import logging
import os
import numpy as np

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
        # Check if the event_id already exists in MongoDB
        event_id = data.get('event_id')
        if event_id and raw_collection.find_one({"event_id": event_id}):
            logger.info(f"Skipping duplicate event: {event_id}")
            return False  # Skip duplicate records

        # Add metadata
        data['received_at'] = datetime.now().isoformat()
        
        # Insert into MongoDB
        result = raw_collection.insert_one(data)
        mongo_id = result.inserted_id
        logger.info(f"Stored new raw data in MongoDB with ID: {mongo_id}")
        
        return True
    except Exception as e:
        logger.error(f"Error storing raw data in MongoDB: {e}")
        return False

def insert_into_postgres(data):
    """Insert the cleaned data into PostgreSQL without referring back to MongoDB."""
    try:
        # Extract fields safely
        device_id = data.get("device_id") or data.get("Device ID", "unknown")
        event_id = data.get("event_id", "unknown")
        ingestion_timestamp = data.get("ingestion_timestamp")
        received_at = data.get("received_at")

        # Handle different device types
        if "timestamp" in data:  # NeuraWrist (Unix timestamp)
            event_time = datetime.utcfromtimestamp(float(data["timestamp"]))
            heart_rate = data.get("Heart Rate", None)
            steps = data.get("Step Count", 0)
            sleep_state = data.get("Sleeping State", "unknown")
        elif "date" in data:  # PulseON (string date)
            try:
                event_time = datetime.strptime(data["date"], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                logger.warning(f"Invalid date format: {data['date']}")
                return False  # Skip invalid records
            heart_rate = data.get("hr", None)
            steps = data.get("steps", 0)
            sleep_state = data.get("state", "unknown")
        else:
            logger.warning(f"Skipping record with missing timestamp/date: {data}")
            return False

        # Insert into PostgreSQL
        insert_query = """
            INSERT INTO smartwatch_events (device_id, event_time, heart_rate, steps, sleep_state, ingestion_timestamp, event_id, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        timescale_cursor.execute(insert_query, (
            device_id, event_time, heart_rate, steps, sleep_state,
            ingestion_timestamp, event_id, received_at
        ))
        timescale_conn.commit()
        logger.info(f"Inserted event {event_id} into PostgreSQL.")
        return True

    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")
        return False


def update_aggregates():
    """Aggregate data to store daily summaries in PostgreSQL."""
    try:
        logger.info("Updating aggregate metrics...")

        # Aggregate: Average heart rate during sleep
        sql_avg_hr_sleep = """
            INSERT INTO daily_heart_rate (date, device_id, avg_hr_during_sleep)
            SELECT 
                DATE(event_time) AS date,
                device_id,
                AVG(heart_rate) AS avg_hr_during_sleep
            FROM smartwatch_events
            WHERE sleep_state = 'sleeping' AND heart_rate IS NOT NULL
            GROUP BY date, device_id
            ON CONFLICT (date, device_id) DO UPDATE
            SET avg_hr_during_sleep = EXCLUDED.avg_hr_during_sleep;
        """

        # Aggregate: Total steps per day
        sql_total_steps = """
            INSERT INTO daily_steps (date, device_id, total_steps)
            SELECT 
                DATE(event_time) AS date,
                device_id,
                SUM(steps) AS total_steps
            FROM smartwatch_events
            WHERE steps IS NOT NULL
            GROUP BY date, device_id
            ON CONFLICT (date, device_id) DO UPDATE
            SET total_steps = EXCLUDED.total_steps;
        """


        # Execute queries
        timescale_cursor.execute(sql_avg_hr_sleep)
        timescale_cursor.execute(sql_total_steps)
        timescale_conn.commit()
        logger.info("Aggregates updated successfully.")

    except Exception as e:
        timescale_conn.rollback()
        logger.error(f"Error updating aggregates: {e}")


def main():
    logger.info("Starting data pipeline...")

    try:
        while True:
            msg = consumer.poll(1.0)
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
                    
                    insert_into_postgres(data)  # Corrected indentation
                    logger.info("Inserted into PostgreSQL.")

                    update_aggregates()  # Corrected indentation
                    logger.info("Aggregates updated.")

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

