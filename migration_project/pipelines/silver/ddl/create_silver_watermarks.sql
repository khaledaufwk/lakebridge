-- Silver Layer Watermark Table
-- ==============================
-- Tracks the state of each Silver table load for incremental processing
-- Created: 2026-01-23

-- Create migration schema if not exists
CREATE SCHEMA IF NOT EXISTS wakecap_prod.migration;

-- Silver watermarks table
CREATE TABLE IF NOT EXISTS wakecap_prod.migration._silver_watermarks (
    -- Table identification
    table_name STRING NOT NULL COMMENT 'Silver table name (e.g., silver_organization)',
    source_bronze_table STRING COMMENT 'Source Bronze table name',
    processing_group STRING COMMENT 'Processing group for dependency ordering',

    -- Watermark tracking
    last_bronze_watermark TIMESTAMP COMMENT 'Max _loaded_at from Bronze source',
    last_load_status STRING COMMENT 'Last load status: success/failed/skipped',

    -- Row count metrics
    last_load_row_count BIGINT COMMENT 'Rows written to Silver',
    rows_input BIGINT COMMENT 'Rows read from Bronze',
    rows_dropped_critical BIGINT COMMENT 'Rows dropped due to critical expectation failures',
    rows_flagged_business BIGINT COMMENT 'Rows flagged for business rule violations (not dropped)',
    rows_warned_advisory BIGINT COMMENT 'Rows with advisory warnings (not dropped)',

    -- Error tracking
    last_error_message STRING COMMENT 'Error message if load failed',

    -- Timing
    last_load_start_time TIMESTAMP COMMENT 'Start time of last load',
    last_load_end_time TIMESTAMP COMMENT 'End time of last load',
    last_load_duration_seconds DOUBLE COMMENT 'Duration in seconds',

    -- Metadata
    pipeline_run_id STRING COMMENT 'Run ID for tracking',
    updated_at TIMESTAMP COMMENT 'Record update timestamp',

    -- Primary key constraint (logical)
    CONSTRAINT pk_silver_watermarks PRIMARY KEY (table_name)
)
USING DELTA
COMMENT 'Watermark tracking for Silver layer incremental loads'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create index-like optimization for common queries
-- Note: Delta Lake doesn't have traditional indexes, but Z-ordering helps
-- This would be applied separately as: OPTIMIZE wakecap_prod.migration._silver_watermarks ZORDER BY (table_name, processing_group)
