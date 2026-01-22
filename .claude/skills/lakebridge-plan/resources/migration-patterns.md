# Migration Patterns Reference

## T-SQL to Databricks Type Mappings

| T-SQL Type | Databricks Type | Notes |
|------------|-----------------|-------|
| `NVARCHAR(MAX)` | `STRING` | |
| `VARCHAR(MAX)` | `STRING` | |
| `NVARCHAR(n)` | `STRING` | Length not enforced |
| `VARCHAR(n)` | `STRING` | Length not enforced |
| `INT` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `SMALLINT` | `SMALLINT` | Same |
| `TINYINT` | `TINYINT` | Same |
| `BIT` | `BOOLEAN` | |
| `DATETIME` | `TIMESTAMP` | |
| `DATETIME2` | `TIMESTAMP` | |
| `DATE` | `DATE` | Same |
| `TIME` | `STRING` | No TIME type |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` | |
| `MONEY` | `DECIMAL(19,4)` | |
| `SMALLMONEY` | `DECIMAL(10,4)` | |
| `FLOAT` | `DOUBLE` | |
| `REAL` | `FLOAT` | |
| `UNIQUEIDENTIFIER` | `STRING` | |
| `VARBINARY(MAX)` | `BINARY` | |
| `XML` | `STRING` | Parse separately |
| `GEOGRAPHY` | `STRING` | Use H3 or WKT |

## Function Mappings

| T-SQL Function | Databricks Equivalent |
|----------------|----------------------|
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `GETUTCDATE()` | `CURRENT_TIMESTAMP()` |
| `SYSDATETIME()` | `CURRENT_TIMESTAMP()` |
| `ISNULL(a, b)` | `COALESCE(a, b)` |
| `LEN(s)` | `LENGTH(s)` |
| `CHARINDEX(sub, str)` | `LOCATE(sub, str)` |
| `SUBSTRING(s, start, len)` | `SUBSTRING(s, start, len)` |
| `LEFT(s, n)` | `LEFT(s, n)` |
| `RIGHT(s, n)` | `RIGHT(s, n)` |
| `LTRIM(s)` | `LTRIM(s)` |
| `RTRIM(s)` | `RTRIM(s)` |
| `UPPER(s)` | `UPPER(s)` |
| `LOWER(s)` | `LOWER(s)` |
| `REPLACE(s, old, new)` | `REPLACE(s, old, new)` |
| `CONCAT(a, b, ...)` | `CONCAT(a, b, ...)` |
| `CAST(x AS type)` | `CAST(x AS type)` |
| `CONVERT(type, x)` | `CAST(x AS type)` |
| `DATEADD(part, n, date)` | `DATE_ADD(date, n)` or `DATEADD(part, n, date)` |
| `DATEDIFF(part, d1, d2)` | `DATEDIFF(d1, d2)` (days only) |
| `DATEPART(part, date)` | `EXTRACT(part FROM date)` |
| `YEAR(date)` | `YEAR(date)` |
| `MONTH(date)` | `MONTH(date)` |
| `DAY(date)` | `DAY(date)` |
| `@@ROWCOUNT` | `spark.sql("SELECT COUNT(*) FROM ...").first()[0]` |
| `@@IDENTITY` | N/A (use GENERATED ALWAYS AS IDENTITY) |
| `NEWID()` | `UUID()` |
| `ROW_NUMBER()` | `ROW_NUMBER()` |
| `RANK()` | `RANK()` |
| `DENSE_RANK()` | `DENSE_RANK()` |
| `LAG()` | `LAG()` |
| `LEAD()` | `LEAD()` |

## Complex Pattern Conversions

### CURSOR to DataFrame Window Functions

**T-SQL (Before):**
```sql
DECLARE @id INT, @prev_val INT, @running_total INT = 0
DECLARE cur CURSOR FOR SELECT id, value FROM table ORDER BY id
OPEN cur
FETCH NEXT FROM cur INTO @id, @prev_val
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @running_total = @running_total + @prev_val
    UPDATE table SET running_total = @running_total WHERE id = @id
    FETCH NEXT FROM cur INTO @id, @prev_val
END
CLOSE cur
DEALLOCATE cur
```

**Databricks (After):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

window_spec = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result_df = (
    spark.table("table")
    .withColumn("running_total", spark_sum("value").over(window_spec))
)
```

### Temp Tables to CTEs or Views

**T-SQL (Before):**
```sql
SELECT * INTO #temp FROM source WHERE condition
UPDATE #temp SET col = value WHERE filter
SELECT * FROM #temp JOIN other ON #temp.id = other.id
DROP TABLE #temp
```

**Databricks (After):**
```python
# Option 1: CTE
result = spark.sql("""
WITH temp AS (
    SELECT *, CASE WHEN filter THEN value ELSE col END AS col
    FROM source
    WHERE condition
)
SELECT * FROM temp JOIN other ON temp.id = other.id
""")

# Option 2: Temp View
source_df.filter("condition").createOrReplaceTempView("temp")
spark.sql("SELECT * FROM temp JOIN other ON temp.id = other.id")
```

### MERGE INTO with DLT APPLY CHANGES

**T-SQL (Before):**
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.deleted = 1 THEN DELETE
WHEN MATCHED THEN UPDATE SET t.col = s.col, t.updated = GETDATE()
WHEN NOT MATCHED THEN INSERT (id, col, created) VALUES (s.id, s.col, GETDATE());
```

**DLT (After):**
```python
import dlt

dlt.create_streaming_table("target")

dlt.apply_changes(
    target="target",
    source="source",
    keys=["id"],
    sequence_by="updated_at",
    apply_as_deletes=expr("deleted = 1"),
    except_column_list=["deleted"]
)
```

### Dynamic SQL to Parameterized Queries

**T-SQL (Before):**
```sql
DECLARE @sql NVARCHAR(MAX)
SET @sql = 'SELECT * FROM ' + @table_name + ' WHERE ' + @filter_col + ' = @value'
EXEC sp_executesql @sql, N'@value INT', @value = @param_value
```

**Databricks (After):**
```python
def query_table(table_name: str, filter_col: str, value: int):
    # Validate inputs to prevent injection
    allowed_tables = ["table1", "table2", "table3"]
    if table_name not in allowed_tables:
        raise ValueError(f"Invalid table: {table_name}")

    return spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE {filter_col} = {value}
    """)
```

## DLT Medallion Architecture

### Bronze Layer (Raw Ingestion)

```python
@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from SQL Server",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.read.format("jdbc")
        .option("url", dbutils.secrets.get("scope", "jdbc_url"))
        .option("dbtable", "[dbo].[Customers]")
        .option("user", dbutils.secrets.get("scope", "user"))
        .option("password", dbutils.secrets.get("scope", "password"))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
```

### Silver Layer (Cleaned, Validated)

```python
@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data"
)
@dlt.expect_or_drop("valid_id", "CustomerID IS NOT NULL")
@dlt.expect_or_drop("valid_email", "Email IS NOT NULL AND Email LIKE '%@%'")
@dlt.expect("valid_name", "Name IS NOT NULL", on_violation="WARN")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .filter(col("DeletedAt").isNull())  # Soft delete filter
        .withColumn("customer_ingested_at", current_timestamp())
        .dropDuplicates(["CustomerID"])
    )
```

### Gold Layer (Business Aggregates)

```python
@dlt.table(
    name="gold_customer_summary",
    comment="Customer summary with order metrics"
)
def gold_customer_summary():
    customers = dlt.read("silver_customers")
    orders = dlt.read("silver_orders")

    return (
        customers
        .join(orders, "CustomerID", "left")
        .groupBy("CustomerID", "Name", "Email")
        .agg(
            count("OrderID").alias("total_orders"),
            sum("Amount").alias("total_spent"),
            max("OrderDate").alias("last_order_date")
        )
    )
```

## Common Error Patterns

| Error | Cause | Solution |
|-------|-------|----------|
| `NO_TABLES_IN_PIPELINE` | Missing `@dlt.table` decorators | Add decorators, check notebook format |
| `WAITING_FOR_RESOURCES` | VM quota exhausted | Use `serverless=True` |
| `AMBIGUOUS_REFERENCE` | Duplicate column names in join | Use unique prefixes |
| `Schema not found` | Target schema doesn't exist | Create schema first |
| `No suitable driver` | JDBC driver not installed | Add mssql-jdbc JAR |
| `Connection timeout` | Firewall blocking | Allow Databricks IPs |

---

## PostgreSQL/TimescaleDB to Databricks Bronze Layer

### PostgreSQL Type Mappings

| PostgreSQL Type | Databricks Type | Notes |
|-----------------|-----------------|-------|
| `TEXT` | `STRING` | |
| `VARCHAR(n)` | `STRING` | Length not enforced |
| `CHAR(n)` | `STRING` | |
| `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | Same |
| `SMALLINT` | `SMALLINT` | Same |
| `SERIAL` | `INT` | Auto-increment, use IDENTITY |
| `BIGSERIAL` | `BIGINT` | Auto-increment, use IDENTITY |
| `BOOLEAN` | `BOOLEAN` | Same |
| `TIMESTAMP` | `TIMESTAMP` | Same |
| `TIMESTAMPTZ` | `TIMESTAMP` | Timezone converted to UTC |
| `DATE` | `DATE` | Same |
| `TIME` | `STRING` | No native TIME type |
| `TIMETZ` | `STRING` | No native TIME type |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` | |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `REAL` | `FLOAT` | |
| `DOUBLE PRECISION` | `DOUBLE` | |
| `UUID` | `STRING` | Store as string |
| `BYTEA` | `BINARY` | |
| `JSON` | `STRING` | Parse with from_json() |
| `JSONB` | `STRING` | Parse with from_json() |
| `ARRAY` | `ARRAY` | Use array functions |
| `INTERVAL` | `STRING` | Store as string |

### PostgreSQL Function Mappings

| PostgreSQL Function | Databricks Equivalent |
|--------------------|----------------------|
| `NOW()` | `CURRENT_TIMESTAMP()` |
| `CURRENT_TIMESTAMP` | `CURRENT_TIMESTAMP()` |
| `CURRENT_DATE` | `CURRENT_DATE()` |
| `COALESCE(a, b)` | `COALESCE(a, b)` |
| `NULLIF(a, b)` | `NULLIF(a, b)` |
| `LENGTH(s)` | `LENGTH(s)` |
| `POSITION(sub IN str)` | `LOCATE(sub, str)` |
| `SUBSTRING(s FROM start FOR len)` | `SUBSTRING(s, start, len)` |
| `TRIM(s)` | `TRIM(s)` |
| `UPPER(s)` | `UPPER(s)` |
| `LOWER(s)` | `LOWER(s)` |
| `CONCAT(a, b)` | `CONCAT(a, b)` |
| `TO_CHAR(date, format)` | `DATE_FORMAT(date, format)` |
| `TO_DATE(str, format)` | `TO_DATE(str, format)` |
| `TO_TIMESTAMP(str, format)` | `TO_TIMESTAMP(str, format)` |
| `EXTRACT(part FROM date)` | `EXTRACT(part FROM date)` |
| `DATE_TRUNC(part, date)` | `DATE_TRUNC(part, date)` |
| `AGE(d1, d2)` | `DATEDIFF(d1, d2)` |
| `gen_random_uuid()` | `UUID()` |
| `ROW_NUMBER()` | `ROW_NUMBER()` |
| `RANK()` | `RANK()` |
| `DENSE_RANK()` | `DENSE_RANK()` |
| `LAG()` | `LAG()` |
| `LEAD()` | `LEAD()` |

### Bronze Layer JDBC Pattern (PostgreSQL)

```python
@dlt.table(
    name="bronze_tablename",
    comment="Raw data from TimescaleDB",
    table_properties={"quality": "bronze"}
)
def bronze_tablename():
    host = dbutils.secrets.get("wakecap-timescale", "timescaledb-host")
    port = dbutils.secrets.get("wakecap-timescale", "timescaledb-port")
    database = dbutils.secrets.get("wakecap-timescale", "timescaledb-database")
    user = dbutils.secrets.get("wakecap-timescale", "timescaledb-user")
    password = dbutils.secrets.get("wakecap-timescale", "timescaledb-password")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.tablename")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_source_system", lit("timescaledb"))
        .withColumn("_source_table", lit("tablename"))
    )
```

### Generic Incremental Load Pattern with Watermarks

```python
def load_table_incremental(table_name: str, primary_key: str, watermark_col: str = None):
    """Generic incremental loader using watermark tracking."""

    # Get last watermark
    last_watermark = spark.sql(f"""
        SELECT last_watermark_timestamp
        FROM wakecap_prod.migration._timescaledb_watermarks
        WHERE source_table = '{table_name}' AND source_system = 'timescaledb'
    """).first()

    # Build query with watermark filter
    if last_watermark and watermark_col:
        query = f"SELECT * FROM public.{table_name} WHERE {watermark_col} > '{last_watermark[0]}'"
    else:
        query = f"SELECT * FROM public.{table_name}"

    # Load data
    df = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )

    # Upsert using MERGE
    df.createOrReplaceTempView("source_data")

    spark.sql(f"""
        MERGE INTO wakecap_prod.source_timescaledb.public_{table_name} AS target
        USING source_data AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Update watermark
    update_watermark(table_name, df.count())
```

### High-Volume Table Handling

For tables with millions of rows (OBS, DeviceLocation, etc.):

```python
# Use partitioned reading for large tables
df = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "public.OBS")
    .option("user", user)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "50000")  # Larger fetch size
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "100000000")
    .option("numPartitions", "10")  # Parallel readers
    .load()
)
```

### TimescaleDB-Specific Patterns

For TimescaleDB hypertables with time-series data:

```python
# Time-based incremental load for hypertables
def load_hypertable(table_name: str, time_column: str = "time"):
    last_time = get_last_watermark(table_name)

    if last_time:
        # Only load recent data
        query = f"""
            SELECT * FROM public.{table_name}
            WHERE {time_column} > '{last_time}'
            ORDER BY {time_column}
        """
    else:
        # Initial load with time bounds
        query = f"""
            SELECT * FROM public.{table_name}
            WHERE {time_column} >= NOW() - INTERVAL '30 days'
            ORDER BY {time_column}
        """

    return (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("fetchsize", "50000")
        .load()
    )
```

### PostgreSQL/TimescaleDB Error Patterns

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection refused` | Firewall or wrong host | Check host/port, allow Databricks IPs |
| `FATAL: password authentication failed` | Bad credentials | Verify secrets in scope |
| `SSL connection required` | Missing sslmode | Add `?sslmode=require` to JDBC URL |
| `Out of memory` | Large result set | Reduce fetchsize, use partitioning |
| `Statement timeout` | Query too slow | Add `?statement_timeout=0` or increase |
| `Table not found` | Wrong schema | Use `public.tablename` or set search_path |
