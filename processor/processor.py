from confluent_kafka import Consumer, Producer
import json
from datetime import datetime, timedelta
import pymongo
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

# MongoDB connection
mongo_client = pymongo.MongoClient(mongodb_uri)
raw_db = mongo_client["smartwatch_raw"]
raw_collection = raw_db["events"]

# TimescaleDB (PostgreSQL) connection
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
user_daily_metrics = {}  # Format: {user_id: {date: {steps: 0, heart_rate_sum: 0, heart_rate_count: 0, sleep_hr_sum: 0, sleep_hr_count: 0}}}

def clean_data(data):
    """Clean and normalize the data"""
    cleaned = data.copy()
    
    # Handle missing values
    if 'heart_rate' not in cleaned:
        cleaned['heart_rate'] = None
    elif cleaned['heart_rate'] is not None:
        # Handle outliers - basic range check
        if cleaned['heart_rate'] < 30 or cleaned['heart_rate'] > 220:
            cleaned['heart_rate'] = None
    
    if 'step_count' not in cleaned:
        cleaned['step_count'] = 0
    
    if 'sleeping_state' not in cleaned:
        cleaned['sleeping_state'] = None
    
    return cleaned

def process_event(data):
    """Process a single event"""
    user_id = data.get('user_id')
    device_id = data.get('device_id')
    timestamp = data.get('timestamp', datetime.now().isoformat())
    
    # Clean the data
    cleaned_data = clean_data(data)
    
    # Store raw data in MongoDB
    raw_collection.insert_one(cleaned_data)
    
    # Process for aggregation
    if user_id:
        event_date = datetime.fromisoformat(timestamp).date().isoformat()
        
        # Initialize user and date in the aggregation dict if not exists
        if user_id not in user_daily_metrics:
            user_daily_metrics[user_id] = {}
        
        if event_date not in user_daily_metrics[user_id]:
            user_daily_metrics[user_id][event_date] = {
                'steps': 0,
                'heart_rate_sum': 0,
                'heart_rate_count': 0,
                'sleep_hr_sum': 0,
                'sleep_hr_count': 0
            }
        
        # Update metrics
        daily_stats = user_daily_metrics[user_id][event_date]
        
        # Add steps
        if cleaned_data['step_count'] is not None:
            daily_stats['steps'] += cleaned_data['step_count']
        
        # Add heart rate data
        if cleaned_data['heart_rate'] is not None:
            daily_stats['heart_rate_sum'] += cleaned_data['heart_rate']
            daily_stats['heart_rate_count'] += 1
            
            # If sleeping, also track sleep heart rate
            if cleaned_data['sleeping_state'] == 'SLEEPING':
                daily_stats['sleep_hr_sum'] += cleaned_data['heart_rate']
                daily_stats['sleep_hr_count'] += 1

def persist_aggregations():
    """Persist aggregated data to TimescaleDB"""
    current_time = datetime.now()
    
    for user_id, dates in user_daily_metrics.items():
        for date_str, metrics in dates.items():
            # Calculate averages
            avg_heart_rate = None
            if metrics['heart_rate_count'] > 0:
                avg_heart_rate = metrics['heart_rate_sum'] / metrics['heart_rate_count']
            
            avg_sleep_heart_rate = None
            if metrics['sleep_hr_count'] > 0:
                avg_sleep_heart_rate = metrics['sleep_hr_sum'] / metrics['sleep_hr_count']
            
            # Insert or update daily summary in TimescaleDB
            sql = """
                INSERT INTO daily_metrics 
                (user_id, date, total_steps, avg_heart_rate, avg_sleep_heart_rate, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, date) 
                DO UPDATE SET 
                    total_steps = EXCLUDED.total_steps,
                    avg_heart_rate = EXCLUDED.avg_heart_rate,
                    avg_sleep_heart_rate = EXCLUDED.avg_sleep_heart_rate,
                    last_updated = EXCLUDED.last_updated
            """
            
            timescale_cursor.execute(sql, (
                user_id, 
                date_str, 
                metrics['steps'], 
                avg_heart_rate, 
                avg_sleep_heart_rate,
                current_time
            ))
            
    # Commit the transaction
    timescale_conn.commit()
    logger.info(f"Persisted aggregations at {current_time}")

# Main processing loop
def main():
    last_persist_time = datetime.now()
    try:
        logger.info("Starting stream processor...")
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # No message, check if we need to persist aggregations
                current_time = datetime.now()
                if (current_time - last_persist_time).total_seconds() >= 60:  # Persist every minute
                    persist_aggregations()
                    last_persist_time = current_time
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            # Process the message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                process_event(value)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        # Close connections
        consumer.close()
        timescale_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()