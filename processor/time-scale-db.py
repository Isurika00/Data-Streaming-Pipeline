import pymongo
import psycopg2
from datetime import datetime, timezone
import os

# Environment Variables
mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

# MongoDB Connection
mongo_client = pymongo.MongoClient(mongodb_uri)
raw_db = mongo_client["smartwatch_raw"]
raw_collection = raw_db["events"]

# PostgreSQL Connection
timescale_conn = psycopg2.connect(postgres_uri)
timescale_cursor = timescale_conn.cursor()

# Fetch data from MongoDB
mongo_data = raw_collection.find()

# Insert data into PostgreSQL
for record in mongo_data:
    try:
        # Normalize data structure
        device_id = record.get("device_id") or record.get("Device ID")
        event_id = record.get("event_id")
        ingestion_timestamp = record.get("ingestion_timestamp")
        received_at = record.get("received_at")
        
        # Data Cleaning: Handle missing critical fields
        if not device_id or not event_id:
            print("Skipping record due to missing device_id or event_id.")
            continue
        
        # Standardize device_id format
        device_id = str(device_id).strip().lower()

        # Handling different device types
        if "timestamp" in record:  # NeuraWrist
            event_time = datetime.fromtimestamp(record["timestamp"], tz=timezone.utc)
            heart_rate = record.get("Heart Rate")
            steps = record.get("Step Count")
            sleep_state = record.get("Sleeping State")
        else:  # PulseON
            event_time = datetime.strptime(record["date"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
            heart_rate = record.get("hr")
            steps = record.get("steps")
            sleep_state = record.get("state")
        
        # Extract additional columns
        event_date = event_time.date()  # YYYY-MM-DD
        event_week = event_time.strftime("%Y-W%U")  # Format: YYYY-WEEKNUMBER
        event_month = event_time.strftime("%Y-%m")  # Format: YYYY-MM
        
        # Data Cleaning: Handle missing or outlier values
        try:
            heart_rate = int(heart_rate) if heart_rate and 30 <= int(heart_rate) <= 220 else None  # Set to NULL if out of range
        except ValueError:
            heart_rate = None  # Handle non-numeric values
        
        try:
            steps = int(steps) if steps and int(steps) >= 0 else 0  # Convert to int and set negative values to 0
        except ValueError:
            steps = 0  # Handle non-numeric values
        
        sleep_flag = 1 if sleep_state in ["sleeping", "asleep", 1] else 0  # Convert sleep state to binary
        
        # Insert into PostgreSQL
        insert_query = """
            INSERT INTO SmartWatchEventFacts (DeviceID, EventDate, EventWeek, EventMonth, TotalSteps, HeartRate, SleepState, IngestionTimestamp, EventID, ReceivedAt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        timescale_cursor.execute(insert_query, (device_id, event_date, event_week, event_month, steps, heart_rate, sleep_flag, ingestion_timestamp, event_id, received_at))
    
    except Exception as e:
        print(f"Error inserting record: {e}")

# Commit and close connections
timescale_conn.commit()
timescale_cursor.close()
timescale_conn.close()
mongo_client.close()

print("Data transfer completed!")
