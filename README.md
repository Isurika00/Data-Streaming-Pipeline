
# Smartwatch Data Pipeline Pipeline

This project implements a real-time data ingestion and processing pipeline for smartwatch data. It receives data from a smartwatch simulation service, processes it, and provides analytics capabilities.

## Architecture

The system consists of the following components:

- **Ingestion Server**: A Flask application that receives data from the smartwatch simulation service and sends it to Kafka
- **Stream Processor**: A service that consumes data from Kafka, processes it, and stores it in databases
- **Query API**: A REST API for querying processed data
- **Storage**: MongoDB for raw data storage and TimescaleDB for processed time-series data

## Prerequisites

- Docker and Docker Compose

## Getting Started

1. Clone the repository:
   ```
   git clone <https://github.com/Isurika00/Data-Streaming-Pipeline.git>
   cd Data-Streaming-Pipeline
   ```

2. Start the services:
   ```
   docker-compose up -d
   ```

3. Run the smartwatch simulation client (in a separate terminal):
   ```
   # For macOS/Windows
   docker run -it --rm streaming-client --server_url http://host.docker.internal:62333/stream --speed 10.0

   # For Linux
   docker run -it --rm --network="host" streaming-client --server_url http://localhost:62333/stream --speed 10.0
   ```

## Components

### Ingestion Server

Flask application that:
- Listens on port 62333
- Receives JSON data from the smartwatch simulation service
- Adds metadata (timestamp, event ID)
- Publishes data to Kafka

### Processing Layer
A data transformation and enrichment layer that:
- Consumes raw data from Kafka.
- Applies minimal transformations (such as data type conversions, normalization, or validation).
- Ensures data is correctly formatted and enriched before being stored in MongoDB or TimescaleDB.
- Acts as a bridge between the raw data in Kafka and the final storage systems (MongoDB for raw data and TimescaleDB for processed data).

### Query API

REST API that provides:
- Endpoint for retrieving daily steps
- Endpoint for retrieving sleep heart rate data
- Endpoint for analytics across users

## API Documentation

### GET /api/user/{user_id}/steps/daily

Returns daily step summary for past month.

Query parameters:
- `days`: Number of days to look back (default: 30)

### GET /api/analytics/heart-rate/by-age

Returns aggregated heart rate statistics by age group.


## Project Structure

```
data-streaming-pipeline/
docker-compose.yml    # Docker Compose configuration
 __ingestion/         # Ingestion server
     Dockerfile
     requirements.txt
     server.py
 __processor/     # Stream processor
     Dockerfile
     requirements.txt
     processor.py
     mongo-db.py
     time-scale-db.py
 __storage/      # Database setup
     schema.sql
 __api/          # Query API
     Dockerfile
     requirements.txt
     api.py
```

##### Smartwatch Data Pipeline Architecture : Justification


## Data Sources & Ingestion
Used Flask as Ingestion server, because it provides a lightweight, easy-to-implement REST API for receiving data from devices. Also, there’s a low overhead, quick to deploy, and sufficient for handling HTTP requests from IoT devices.

## Message Broker
Used Kafka as message broker. It is ideal for handling high-volume, streaming data from numerous devices. Also, it 
provides buffering between ingestion and processing, 
ensures data durability with replication 
allows for multiple consumers of the same data stream.

## MongoDB for raw data
NoSQL document store is perfect for raw, semi-structured IoT data. Because its schema flexibility handling various data types horizontal scaling for handling large volumes and efficient storage of JSON-like data.

## TimescaleDB for processed data
TimescaleDB is optimized for time-series data, which is exactly what the smartwatch data where data points associated with timestamps.
Also, its fast queries, aggregations, and time-based indexing, make it a perfect fit for efficiently querying.

## Data Processing & Transformation 
# Minimal Transformations
   The data receiving from client is mostly in a raw format and needs only basic cleaning and simple transformations according this scenario.
   There for no need for complex processing such as joins from other sources, aggregation across different datasets, or real-time event-driven operations.

   So, decided to go with direct transfer approach. (MongoDB to TimescaleDB)
   Direct transfer from MongoDB to TimescaleDB simplifies the pipeline by minimizing overhead. We can avoid unnecessary complexity from stream processors since the data doesn’t require real-time processing or 
   complex joins.
   Data from MongoDB will be cleaned up and restructured before insertion into TimescaleDB using SQL scripts. This minimizes the need for a complex processing layer.

## Query API
The Query API is built using Flask, which exposes endpoints for querying time-series data stored in TimescaleDB (PostgreSQL).
The APIs can retrieve data from the database based on time-based queries 
e.g., user’s daily steps summary for the past month, avg heart rate among different age categories

Rest API used to check and data retrieval purposes.

