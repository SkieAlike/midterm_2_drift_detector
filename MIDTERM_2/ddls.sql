-- PostgreSQL

-- 1) Default Metric Thresholds
CREATE TYPE metric_type AS ENUM (
  'cpu_usage',
  'memory_usage',
  'request_latency'
);

CREATE TABLE metric_thresholds (
    metric                  metric_type PRIMARY KEY,

    rolling_window_seconds   INTEGER NOT NULL,
    eval_interval_seconds    INTEGER NOT NULL,
    min_samples              INTEGER NOT NULL,

    max_lateness_seconds     INTEGER NOT NULL,

    z_threshold              DOUBLE PRECISION NOT NULL,
    std_ratio_threshold      DOUBLE PRECISION NOT NULL,

    enter_consecutive_hits   INTEGER NOT NULL,
    exit_consecutive_hits    INTEGER NOT NULL,

    enabled                  BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO metric_thresholds (
    metric,
    rolling_window_seconds,
    eval_interval_seconds,
    min_samples,
    max_lateness_seconds,
    z_threshold,
    std_ratio_threshold,
    enter_consecutive_hits,
    exit_consecutive_hits,
    enabled
) VALUES
-- cpu_usage
('cpu_usage', 30, 5, 100, 10, 3.0, 2.0, 3, 3, TRUE),

-- memory_usage
('memory_usage', 30, 5, 100, 10, 3.0, 2.0, 3, 3, TRUE),

-- request_latency (often noisier, you can loosen/tighten as you like)
('request_latency', 30, 5, 100, 10, 3.0, 2.0, 3, 3, TRUE);


-- 2) Baseline Stats
CREATE TABLE baseline_stats (
    source_id        TEXT NOT NULL,
    metric           metric_type NOT NULL,

    baseline_count   BIGINT NOT NULL,
    baseline_mean    DOUBLE PRECISION NOT NULL,
    baseline_std     DOUBLE PRECISION NOT NULL,

    -- NEW: required to merge variance correctly across days (Option B)
    baseline_m2      DOUBLE PRECISION NOT NULL,

    window_start     TIMESTAMPTZ NULL,
    window_end       TIMESTAMPTZ NULL,

    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_id, metric)
);

CREATE INDEX idx_baseline_stats_metric
ON baseline_stats (metric);

CREATE INDEX idx_baseline_stats_updated_at
ON baseline_stats (updated_at);



-- 3) Source Metadata
CREATE TABLE source_metadata (
    source_id       TEXT PRIMARY KEY,
    source_type     TEXT NOT NULL,          -- 'stable' | 'dynamic'
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_source_metadata_last_seen
ON source_metadata (last_seen_at);

-- CLICKHOUSE

-- 1) Raw Events
-- kafka engine
CREATE TABLE kafka_telemetry_events
(
    source_id   String,
    metric      String,
    value       Float64,
    event_time  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:19092,localhost:19094,localhost:19096',
    kafka_topic_list = 'telemetry.events',
    kafka_group_name = 'clickhouse_telemetry_events_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536;

-- Table
CREATE TABLE raw_events
(
    event_date   Date MATERIALIZED toDate(event_time),

    source_id    LowCardinality(String),
    metric       LowCardinality(String),

    value        Float64 CODEC(ZSTD(3)),
    event_time   DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    ingest_time  DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(3)),

    event_id     String CODEC(ZSTD(3)),
    producer_type LowCardinality(String) DEFAULT 'unknown'
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (source_id, metric, event_time)
SETTINGS index_granularity = 8192;


-- Materialized View
CREATE MATERIALIZED VIEW mv_kafka_telemetry_events
TO raw_events
AS
SELECT
    source_id,
    metric,
    value,

    -- Parse ISO-8601 "2026-02-10T12:34:56.789Z" reliably
    parseDateTime64BestEffort(event_time, 3, 'UTC') AS event_time,

    -- Ingest timestamp
    now64(3) AS ingest_time,

    -- Deterministic event_id (helps downstream dedupe/debug)
    lower(hex(SHA256(concat(
        source_id, '|', metric, '|', event_time, '|', toString(round(value, 4))
    )))) AS event_id,

    -- If you don't provide producer_type, default
    'unknown' AS producer_type
FROM kafka_telemetry_events;



-- 2) Drift Events
CREATE TABLE drift_events
(
    -- partition helper
    event_date        Date MATERIALIZED toDate(detected_at),

    source_id         LowCardinality(String),
    metric            LowCardinality(String),

    drift_event_type  LowCardinality(String),  -- 'DRIFT_START' | 'DRIFT_END'

    -- IDEMPOTENCY KEY (deterministic from your detector)
    drift_event_id    String,

    drift_started_at  DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(3)),
    drift_ended_at    DateTime64(3, 'UTC') NULL CODEC(Delta(8), ZSTD(3)),

    -- version column for ReplacingMergeTree
    detected_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(3)),

    -- Debug context
    z_score           Float64 NULL CODEC(ZSTD(3)),
    std_ratio         Float64 NULL CODEC(ZSTD(3)),

    rolling_mean      Float64 NULL CODEC(ZSTD(3)),
    rolling_std       Float64 NULL CODEC(ZSTD(3)),
    rolling_count     UInt32 NULL,

    baseline_mean     Float64 NULL CODEC(ZSTD(3)),
    baseline_std      Float64 NULL CODEC(ZSTD(3)),
    baseline_count    UInt32 NULL
)
ENGINE = ReplacingMergeTree(detected_at)
PARTITION BY event_date
ORDER BY (drift_event_id)
SETTINGS index_granularity = 8192;


-- 3) Drift Status Current
CREATE TABLE drift_status_current
(
    source_id          LowCardinality(String),
    metric             LowCardinality(String),

    state              LowCardinality(String), -- 'INITIALIZING' | 'NORMAL' | 'DRIFTING'

    drift_started_at   DateTime64(3, 'UTC') NULL CODEC(Delta(8), ZSTD(3)),
    drift_ended_at     DateTime64(3, 'UTC') NULL CODEC(Delta(8), ZSTD(3)),

    last_score_z       Float64 NULL CODEC(ZSTD(3)),
    last_score_ratio   Float64 NULL CODEC(ZSTD(3)),

    last_updated_at    DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(3))
)
ENGINE = ReplacingMergeTree(last_updated_at)
ORDER BY (source_id, metric)
SETTINGS index_granularity = 8192;

