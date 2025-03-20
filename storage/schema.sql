DROP TABLE IF EXISTS SmartWatchEventFacts CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Create SmartWatchEventFacts table
CREATE TABLE SmartWatchEventFacts (
    DeviceID TEXT NOT NULL,
    EventDate DATE NOT NULL,
    EventWeek TEXT NOT NULL,  -- Format: YYYY-WEEKNUMBER (e.g., 2025-W11)
    EventMonth TEXT NOT NULL, -- Format: YYYY-MM (e.g., 2025-03)
    TotalSteps INT,
    HeartRate INT,
    SleepState SMALLINT CHECK (SleepState IN (0, 1)), -- 0 = Awake, 1 = Sleeping
    IngestionTimestamp TIMESTAMP,
    EventID TEXT PRIMARY KEY,
    ReceivedAt TIMESTAMP
);

-- Convert SmartWatchEventFacts table into a hypertable
SELECT create_hypertable('SmartWatchEventFacts', 'EventDate');

-- Indexes for fast lookups
CREATE INDEX idx_smartwatch_events_device ON SmartWatchEventFacts (DeviceID);
CREATE INDEX idx_smartwatch_events_date ON SmartWatchEventFacts (EventDate DESC);
CREATE INDEX idx_smartwatch_events_week ON SmartWatchEventFacts (EventWeek);
CREATE INDEX idx_smartwatch_events_month ON SmartWatchEventFacts (EventMonth);

-- Create users table
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    device_id TEXT,
    user_email TEXT UNIQUE,
    gender TEXT,
    age INTEGER
);

-- Indexes for fast lookups
CREATE INDEX idx_users_device ON users (device_id);
CREATE INDEX idx_users_email ON users (user_email);