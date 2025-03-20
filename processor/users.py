import json
import psycopg2
import os
import json
from datetime import datetime
import pymongo
import psycopg2
import logging


postgres_uri = os.environ.get('POSTGRES_URI', 'postgresql://postgres:postgres@localhost:5432/smartwatch_analytics')

timescale_conn = psycopg2.connect(postgres_uri)
cur = timescale_conn.cursor()

# Load JSON file
with open(r'streaming-client\user_profiles.json', 'r') as file:
    users = json.load(file)

# Insert data into the table
for user in users:
    cur.execute("""
        INSERT INTO userDim (user_id, device_id, user_email, gender, age)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
    """, (user['user_id'], user['device_id'], user['user_email'], user['gender'], user['age']))

# Commit changes and close connection
timescale_conn.commit()
cur.close()
timescale_conn.close()

print("Data inserted successfully!")

