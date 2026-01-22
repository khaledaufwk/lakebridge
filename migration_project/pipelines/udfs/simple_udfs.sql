-- =============================================================================
-- WakeCapDW Migration - Simple SQL UDFs
-- =============================================================================
-- Converted from SQL Server user-defined functions to Databricks SQL UDFs.
-- These functions use native Databricks SQL capabilities.
--
-- Source functions:
--   - dbo.fnStripNonNumerics
--   - dbo.fnExtractPattern
--   - dbo.fnAtTimeZone
--   - stg.fnCalcTimeCategory
--   - stg.fnCalcTimeCategory_3Ordered
--   - stg.fnExtSourceIDAlias
--
-- Target: wakecap_prod.migration schema
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fn_strip_non_numerics
-- Remove all non-numeric characters from a string
-- Original: dbo.fnStripNonNumerics(@input NVARCHAR(MAX))
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_strip_non_numerics(input STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Remove all non-numeric characters from a string. Converted from dbo.fnStripNonNumerics'
RETURN regexp_replace(input, '[^0-9]', '');

-- -----------------------------------------------------------------------------
-- fn_extract_pattern
-- Extract a pattern from a string using regex
-- Original: dbo.fnExtractPattern(@input NVARCHAR(MAX), @pattern NVARCHAR(255))
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_extract_pattern(input STRING, pattern STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Extract a pattern from a string using regex. Converted from dbo.fnExtractPattern'
RETURN regexp_extract(input, pattern, 0);

-- -----------------------------------------------------------------------------
-- fn_at_timezone
-- Convert UTC timestamp to a specific timezone
-- Original: dbo.fnAtTimeZone(@datetime DATETIME, @timezone NVARCHAR(50))
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_at_timezone(dt TIMESTAMP, tz STRING)
RETURNS TIMESTAMP
LANGUAGE SQL
COMMENT 'Convert UTC timestamp to a specific timezone. Converted from dbo.fnAtTimeZone'
RETURN from_utc_timestamp(dt, tz);

-- -----------------------------------------------------------------------------
-- fn_calc_time_category
-- Categorize hour of day into time categories
-- Original: stg.fnCalcTimeCategory(@hour INT)
-- Categories: 1=Morning (6-11), 2=Afternoon (12-17), 3=Evening (18-21), 4=Night
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_calc_time_category(hour_of_day INT)
RETURNS INT
LANGUAGE SQL
COMMENT 'Categorize hour of day: 1=Morning, 2=Afternoon, 3=Evening, 4=Night. Converted from stg.fnCalcTimeCategory'
RETURN CASE
    WHEN hour_of_day BETWEEN 6 AND 11 THEN 1   -- Morning
    WHEN hour_of_day BETWEEN 12 AND 17 THEN 2  -- Afternoon
    WHEN hour_of_day BETWEEN 18 AND 21 THEN 3  -- Evening
    ELSE 4                                      -- Night
END;

-- -----------------------------------------------------------------------------
-- fn_calc_time_category_3_ordered
-- Categorize hour into 3 ordered time periods
-- Original: stg.fnCalcTimeCategory_3Ordered(@hour INT)
-- Categories: 1=Day (6-17), 2=Evening (18-21), 3=Night (22-5)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_calc_time_category_3_ordered(hour_of_day INT)
RETURNS INT
LANGUAGE SQL
COMMENT 'Categorize hour into 3 periods: 1=Day, 2=Evening, 3=Night. Converted from stg.fnCalcTimeCategory_3Ordered'
RETURN CASE
    WHEN hour_of_day BETWEEN 6 AND 17 THEN 1   -- Day
    WHEN hour_of_day BETWEEN 18 AND 21 THEN 2  -- Evening
    ELSE 3                                      -- Night
END;

-- -----------------------------------------------------------------------------
-- fn_ext_source_id_alias
-- Get external source ID alias with fallback
-- Original: stg.fnExtSourceIDAlias(@source_id NVARCHAR(50))
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_ext_source_id_alias(source_id STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Get external source ID alias with UNKNOWN fallback. Converted from stg.fnExtSourceIDAlias'
RETURN COALESCE(NULLIF(TRIM(source_id), ''), 'UNKNOWN');

-- -----------------------------------------------------------------------------
-- fn_coalesce_empty
-- Return second value if first is NULL or empty string
-- Utility function commonly needed in migrations
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_coalesce_empty(val1 STRING, val2 STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Return val2 if val1 is NULL or empty string'
RETURN CASE WHEN val1 IS NULL OR TRIM(val1) = '' THEN val2 ELSE val1 END;

-- -----------------------------------------------------------------------------
-- fn_safe_divide
-- Division with NULL result on divide by zero (instead of error)
-- Utility function for safe calculations
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_safe_divide(numerator DOUBLE, denominator DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
COMMENT 'Safe division returning NULL on divide by zero'
RETURN CASE WHEN denominator = 0 OR denominator IS NULL THEN NULL ELSE numerator / denominator END;

-- -----------------------------------------------------------------------------
-- fn_seconds_to_hhmm
-- Convert seconds to HH:MM format string
-- Utility function for time display
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.migration.fn_seconds_to_hhmm(seconds INT)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Convert seconds to HH:MM format string'
RETURN CONCAT(
    LPAD(CAST(FLOOR(seconds / 3600) AS STRING), 2, '0'),
    ':',
    LPAD(CAST(FLOOR((seconds % 3600) / 60) AS STRING), 2, '0')
);

-- =============================================================================
-- Usage Examples:
-- =============================================================================
-- SELECT wakecap_prod.migration.fn_strip_non_numerics('ABC123XYZ456');  -- Returns '123456'
-- SELECT wakecap_prod.migration.fn_extract_pattern('Worker-123', '[0-9]+');  -- Returns '123'
-- SELECT wakecap_prod.migration.fn_at_timezone(current_timestamp(), 'Asia/Dubai');
-- SELECT wakecap_prod.migration.fn_calc_time_category(14);  -- Returns 2 (Afternoon)
-- SELECT wakecap_prod.migration.fn_safe_divide(100, 0);  -- Returns NULL
-- SELECT wakecap_prod.migration.fn_seconds_to_hhmm(3723);  -- Returns '01:02'
-- =============================================================================
