# Databricks notebook source
# MAGIC %md
# MAGIC # Time Category UDF
# MAGIC
# MAGIC Calculates the time category for a reading relative to scheduled shift.
# MAGIC
# MAGIC **Time Categories:**
# MAGIC - 1 = During Shift
# MAGIC - 2 = No Shift Defined
# MAGIC - 3 = During Break
# MAGIC - 4 = After Shift
# MAGIC - 5 = Before Shift
# MAGIC
# MAGIC **Original:** `stg.fnCalcTimeCategory` (54 lines)

# COMMAND ----------

from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import IntegerType, StringType, TimestampType
from datetime import datetime, timedelta, time

# COMMAND ----------

def parse_breaks(breaks_str):
    """
    Parse breaks string like '23:00-01:00;04:00-05:00' into list of (start, end) tuples.
    """
    if not breaks_str:
        return []

    breaks = []
    for break_period in breaks_str.split(';'):
        if '-' not in break_period:
            continue
        parts = break_period.strip().split('-')
        if len(parts) == 2:
            try:
                start_time = datetime.strptime(parts[0].strip(), '%H:%M').time()
                end_time = datetime.strptime(parts[1].strip(), '%H:%M').time()
                breaks.append((start_time, end_time))
            except ValueError:
                continue
    return breaks


def is_time_in_break(local_time, breaks):
    """
    Check if local_time falls within any break period.
    Handles overnight breaks (e.g., 23:00-01:00).
    """
    if not breaks or not local_time:
        return False

    for break_start, break_end in breaks:
        if break_start < break_end:
            # Normal break (e.g., 12:00-13:00)
            if break_start <= local_time <= break_end:
                return True
        else:
            # Overnight break (e.g., 23:00-01:00)
            if local_time >= break_start or local_time <= break_end:
                return True
    return False


def calc_time_category_impl(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime):
    """
    Calculate time category based on shift schedule.

    Parameters:
    - ws_start_time: Shift start time (time object or None)
    - ws_end_time: Shift end time (time object or None)
    - breaks_str: Break periods string like '23:00-01:00;04:00-05:00'
    - shift_local_date: The shift date (date object)
    - local_datetime: The timestamp to categorize (datetime object)

    Returns:
    - 1: During Shift
    - 2: No Shift Defined
    - 3: During Break
    - 4: After Shift
    - 5: Before Shift
    """
    # No shift defined
    if ws_start_time is None or ws_end_time is None:
        return 2

    if shift_local_date is None or local_datetime is None:
        return 2

    # Convert to datetime for comparison
    if isinstance(shift_local_date, datetime):
        base_date = shift_local_date.date()
    else:
        base_date = shift_local_date

    # Create shift start datetime
    ws_start_datetime = datetime.combine(base_date, ws_start_time)

    # Create shift end datetime (handle overnight shifts)
    if ws_start_time > ws_end_time:
        # Overnight shift - end is next day
        ws_end_datetime = datetime.combine(base_date + timedelta(days=1), ws_end_time)
    else:
        ws_end_datetime = datetime.combine(base_date, ws_end_time)

    # Compare local_datetime with shift boundaries
    if local_datetime < ws_start_datetime:
        return 5  # Before Shift
    elif local_datetime > ws_end_datetime:
        return 4  # After Shift
    else:
        # During shift time - check if in break
        breaks = parse_breaks(breaks_str)
        local_time = local_datetime.time() if isinstance(local_datetime, datetime) else local_datetime

        if is_time_in_break(local_time, breaks):
            return 3  # During Break
        return 1  # During Shift


# Register as Spark UDF
@udf(returnType=IntegerType())
def calc_time_category_udf(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime):
    """Spark UDF wrapper for calc_time_category_impl."""
    try:
        return calc_time_category_impl(ws_start_time, ws_end_time, breaks_str, shift_local_date, local_datetime)
    except Exception:
        return 2  # Default to "No Shift Defined" on error

# COMMAND ----------

# Alternative: SQL CASE expression for inline use (no UDF overhead)
# Use this pattern when UDF performance is a concern

def get_time_category_sql_expr():
    """
    Returns SQL expression for time category calculation.
    Use with: df.withColumn("TimeCategoryId", expr(get_time_category_sql_expr()))

    Note: This simplified version doesn't handle breaks. Use UDF for full logic.
    """
    return """
    CASE
        WHEN StartTime IS NULL OR EndTime IS NULL THEN 2
        WHEN LocalTimestamp <
             CAST(CONCAT(CAST(ShiftLocalDate AS STRING), ' ', CAST(StartTime AS STRING)) AS TIMESTAMP)
        THEN 5
        WHEN LocalTimestamp >
             CASE
                WHEN StartTime > EndTime
                THEN CAST(CONCAT(CAST(DATE_ADD(ShiftLocalDate, 1) AS STRING), ' ', CAST(EndTime AS STRING)) AS TIMESTAMP)
                ELSE CAST(CONCAT(CAST(ShiftLocalDate AS STRING), ' ', CAST(EndTime AS STRING)) AS TIMESTAMP)
             END
        THEN 4
        ELSE 1
    END
    """
