-- Setup for high-scale observability logs and events

-- Create the database
CREATE DATABASE IF NOT EXISTS sandbox;

-- 1. Table for Application Logs
-- Using MergeTree for high-velocity inserts and background merges.
-- Partitioning by day to optimize retention and queries.
CREATE TABLE IF NOT EXISTS sandbox.logs (
    timestamp DateTime64(3, 'UTC'),
    level Enum8('DEBUG' = 1, 'INFO' = 2, 'WARNING' = 3, 'ERROR' = 4, 'CRITICAL' = 5),
    service_name LowCardinality(String),
    message String,
    request_id String,
    user_id String,
    payload String,
    
    -- Bloom Filter for high-performance searching on high-cardinality IDs
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, timestamp, level);

-- 2. Table for Transaction Events (from Kafka)
-- Using MergeTree with specialized indexes for payment forensics.
CREATE TABLE IF NOT EXISTS sandbox.transaction_events (
    timestamp DateTime64(3, 'UTC'),
    transaction_id String,
    order_id String,
    user_id String,
    amount Float64,
    currency LowCardinality(String),
    status LowCardinality(String),
    latency_ms UInt32,
    
    -- Bloom Filter for sub-second lookup of specific orders/transactions
    INDEX idx_transaction_id transaction_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_order_id order_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (status, timestamp, transaction_id);
