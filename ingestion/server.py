from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import logging
import datetime
import uuid
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get Kafka bootstrap servers from environment or use default
kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/stream', methods=['POST'])
def receive_stream():
    """Endpoint to receive streaming data from the client."""
    try:
        data = request.json
        
        # Add timestamp and unique event ID
        event_time = datetime.datetime.now().isoformat()
        data['ingestion_timestamp'] = event_time
        data['event_id'] = str(uuid.uuid4())
        
        # Log the received data
        logger.info(f"Received data: {data}")
        
        # Send to Kafka
        producer.send('smartwatch-raw-data', data)
        
        return jsonify({"status": "success", "message": "Data received"}), 200
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "up", "service": "streaming-server"}), 200

if __name__ == '__main__':
    logger.info("Starting streaming server on port 62333...")
    app.run(host='0.0.0.0', port=62333)