-- =============================================================================
-- TimescaleDB Watermark Tracking Table
-- =============================================================================
-- Purpose: Tracks watermark values for incremental loads from TimescaleDB
-- Location: wakecap_prod.migration._timescaledb_watermarks
--
-- This table stores the last successfully loaded watermark value for each
-- source table, enabling incremental extraction of only new/changed records.
-- =============================================================================

-- Create the catalog and schema if they don't exist
CREATE CATALOG IF NOT EXISTS wakecap_prod;
CREATE SCHEMA IF NOT EXISTS wakecap_prod.migration;

-- Drop existing table if needed (uncomment if recreating)
-- DROP TABLE IF EXISTS wakecap_prod.migration._timescaledb_watermarks;

-- Create the watermark tracking table
CREATE TABLE IF NOT EXISTS wakecap_prod.migration._timescaledb_watermarks (
    -- Primary identification
    source_system STRING NOT NULL
        COMMENT 'Source system identifier (e.g., timescaledb)',
    source_schema STRING NOT NULL
        COMMENT 'Source schema name (e.g., public)',
    source_table STRING NOT NULL
        COMMENT 'Source table name',

    -- Watermark tracking
    watermark_column STRING NOT NULL
        COMMENT 'Column used for watermark tracking',
    watermark_type STRING NOT NULL
        COMMENT 'Data type of watermark column: timestamp, bigint, date',
    last_watermark_value STRING
        COMMENT 'Last watermark value as string (for logging)',
    last_watermark_timestamp TIMESTAMP
        COMMENT 'Last watermark value for timestamp columns',
    last_watermark_bigint BIGINT
        COMMENT 'Last watermark value for bigint/integer columns',

    -- Execution metadata
    last_load_start_time TIMESTAMP
        COMMENT 'When the last load started',
    last_load_end_time TIMESTAMP
        COMMENT 'When the last load completed',
    last_load_status STRING
        COMMENT 'Status of last load: success, failed, running, skipped',
    last_load_row_count BIGINT
        COMMENT 'Number of rows loaded in last execution',
    last_error_message STRING
        COMMENT 'Error message if last load failed',

    -- Pipeline metadata
    pipeline_id STRING
        COMMENT 'DLT pipeline ID if run via DLT',
    pipeline_run_id STRING
        COMMENT 'Specific pipeline run ID',

    -- Audit columns
    created_at TIMESTAMP DEFAULT current_timestamp()
        COMMENT 'When this record was first created',
    updated_at TIMESTAMP
        COMMENT 'When this record was last updated',
    created_by STRING DEFAULT current_user()
        COMMENT 'User who created this record',

    -- Primary key constraint
    CONSTRAINT pk_timescaledb_watermarks
        PRIMARY KEY (source_system, source_schema, source_table)
)
USING DELTA
PARTITIONED BY (source_system)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2',
    'quality' = 'system',
    'description' = 'Tracks watermark values for incremental TimescaleDB loads'
)
COMMENT 'Watermark tracking table for incremental TimescaleDB data extraction';

-- Create index-like optimization via Z-ORDER (run periodically)
-- OPTIMIZE wakecap_prod.migration._timescaledb_watermarks
--   ZORDER BY (source_schema, source_table);

-- =============================================================================
-- Helper Views
-- =============================================================================

-- View: Current load status for all tables
CREATE OR REPLACE VIEW wakecap_prod.migration.vw_timescaledb_load_status AS
SELECT
    source_schema,
    source_table,
    watermark_column,
    last_watermark_value,
    last_load_status,
    last_load_row_count,
    last_load_start_time,
    last_load_end_time,
    TIMESTAMPDIFF(SECOND, last_load_start_time, last_load_end_time) AS duration_seconds,
    last_error_message,
    updated_at
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
ORDER BY updated_at DESC;

-- View: Failed loads requiring attention
CREATE OR REPLACE VIEW wakecap_prod.migration.vw_timescaledb_failed_loads AS
SELECT
    source_schema,
    source_table,
    last_load_start_time,
    last_error_message,
    pipeline_run_id
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
  AND last_load_status = 'failed'
ORDER BY last_load_start_time DESC;

-- View: Load statistics summary
CREATE OR REPLACE VIEW wakecap_prod.migration.vw_timescaledb_load_summary AS
SELECT
    last_load_status AS status,
    COUNT(*) AS table_count,
    SUM(last_load_row_count) AS total_rows,
    AVG(TIMESTAMPDIFF(SECOND, last_load_start_time, last_load_end_time)) AS avg_duration_seconds
FROM wakecap_prod.migration._timescaledb_watermarks
WHERE source_system = 'timescaledb'
GROUP BY last_load_status;

-- =============================================================================
-- Sample Queries
-- =============================================================================

-- Check watermark for a specific table:
-- SELECT * FROM wakecap_prod.migration._timescaledb_watermarks
-- WHERE source_table = 'Worker';

-- View all tables that need to be loaded (no watermark yet):
-- SELECT source_table FROM wakecap_prod.migration._timescaledb_watermarks
-- WHERE last_watermark_value IS NULL;

-- View load history (requires Change Data Feed):
-- SELECT * FROM table_changes('wakecap_prod.migration._timescaledb_watermarks', 1)
-- WHERE source_table = 'Worker'
-- ORDER BY _commit_timestamp DESC;
