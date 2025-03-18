-- Create extension if not exists
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    age INT,
    gender VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create daily metrics table
CREATE TABLE IF NOT EXISTS daily_metrics (
    user_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    total_steps INT DEFAULT 0,
    avg_heart_rate FLOAT,
    avg_sleep_heart_rate FLOAT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, date)
);

-- Convert daily_metrics to a hypertable partitioned by date
SELECT create_hypertable('daily_metrics', 'date', if_not_exists => TRUE);

-- Create hourly metrics table for more granular data
CREATE TABLE IF NOT EXISTS hourly_metrics (
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    hour_of_day INT NOT NULL,
    steps_in_hour INT DEFAULT 0,
    avg_heart_rate FLOAT,
    sleeping_state VARCHAR(20),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, timestamp)
);

-- Convert hourly_metrics to a hypertable
SELECT create_hypertable('hourly_metrics', 'timestamp', if_not_exists => TRUE);

-- Create indices for common queries
CREATE INDEX IF NOT EXISTS idx_daily_metrics_user_date ON daily_metrics (user_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_user_timestamp ON hourly_metrics (user_id, timestamp DESC);

-- Insert some test data for users if table is empty
INSERT INTO users (user_id, age, gender)
SELECT 'user_' || i, 
       20 + (i % 50),  -- Ages between 20 and 69
       CASE WHEN i % 2 = 0 THEN 'Male' ELSE 'Female' END
FROM generate_series(1, 10) i
ON CONFLICT (user_id) DO NOTHING;