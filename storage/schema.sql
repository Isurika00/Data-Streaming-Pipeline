DROP TABLE IF EXISTS smartwatch_raw_events CASCADE;

-- Create the base table
CREATE TABLE smartwatch_raw_events (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    device_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    raw_data JSONB NOT NULL
);

-- Convert table into a hypertable
SELECT create_hypertable('smartwatch_raw_events', 'event_time');

-- Indexes for fast lookups
CREATE INDEX idx_smartwatch_events_user ON smartwatch_raw_events (user_id);
CREATE INDEX idx_smartwatch_events_device ON smartwatch_raw_events (device_id);
CREATE INDEX idx_smartwatch_events_type ON smartwatch_raw_events (event_type);
CREATE INDEX idx_smartwatch_events_time ON smartwatch_raw_events (event_time DESC);