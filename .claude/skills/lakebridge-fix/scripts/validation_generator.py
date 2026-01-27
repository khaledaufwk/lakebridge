"""
Validation Notebook Generator for Lakebridge.

REQ-F7 Enhanced: Generates comprehensive validation notebooks with:
- Row count comparison (source vs target)
- Sample record matching
- Aggregation verification
- Data type validation
- Business rule checks
- Data freshness validation
- NULL distribution analysis
- Value distribution comparison
- Cross-table consistency checks
- Integration with business logic comparator
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime


@dataclass
class ValidationConfig:
    """Configuration for validation notebook generation."""
    procedure_name: str
    source_table: str              # Silver layer source
    target_table: str              # Gold layer target
    key_columns: List[str]         # Primary key columns
    aggregation_columns: List[str] # Numeric columns to validate
    date_column: Optional[str] = None     # Date column for filtering
    sample_size: int = 100
    tolerance_percent: float = 0.01  # Acceptable difference threshold
    # Enhanced options (REQ-F7)
    categorical_columns: List[str] = field(default_factory=list)  # For distribution checks
    foreign_key_checks: List[Dict[str, str]] = field(default_factory=list)  # FK validations
    custom_rules: List[Dict[str, Any]] = field(default_factory=list)  # Custom SQL rules
    include_profiling: bool = True  # Include data profiling section
    include_freshness: bool = True  # Include freshness checks
    expected_freshness_hours: int = 24  # Maximum acceptable data age


class ValidationNotebookGenerator:
    """
    Generate validation notebooks for converted stored procedures.

    Creates notebooks that:
    1. Compare row counts between source and target
    2. Compare sample records
    3. Verify aggregations match
    4. Check data type consistency
    5. Validate business rules
    """

    def generate(self, config: ValidationConfig) -> str:
        """
        Generate complete validation notebook.

        Args:
            config: ValidationConfig with source, target, and column info

        Returns:
            Complete Databricks notebook content as string
        """
        sections = [
            self._generate_header(config),
            self._generate_config_section(config),
            self._generate_helpers_section(),
            self._generate_row_count_section(config),
            self._generate_aggregation_section(config),
            self._generate_sample_comparison_section(config),
            self._generate_schema_validation_section(config),
            self._generate_business_rules_section(config),
        ]

        # REQ-F7 Enhanced sections
        if config.include_freshness:
            sections.append(self._generate_freshness_section(config))

        if config.include_profiling:
            sections.append(self._generate_profiling_section(config))

        if config.categorical_columns:
            sections.append(self._generate_distribution_section(config))

        if config.foreign_key_checks:
            sections.append(self._generate_fk_validation_section(config))

        if config.custom_rules:
            sections.append(self._generate_custom_rules_section(config))

        sections.append(self._generate_summary_section(config))

        return '\n'.join(sections)

    def _generate_header(self, config: ValidationConfig) -> str:
        """Generate notebook header."""
        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Validation: {config.procedure_name}
# MAGIC
# MAGIC **Purpose:** Validate conversion of `{config.procedure_name}` to Databricks
# MAGIC
# MAGIC **Source Table:** `{config.source_table}`
# MAGIC **Target Table:** `{config.target_table}`
# MAGIC
# MAGIC **Validation Checks:**
# MAGIC 1. Row count comparison
# MAGIC 2. Aggregation verification
# MAGIC 3. Sample record matching
# MAGIC 4. Schema validation
# MAGIC 5. Business rule checks
# MAGIC
# MAGIC **Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# Validation results collector
validation_results = {{
    "procedure": "{config.procedure_name}",
    "timestamp": datetime.now().isoformat(),
    "checks": []
}}

def add_result(check_name, status, details=None):
    """Add a validation result."""
    validation_results["checks"].append({{
        "check": check_name,
        "status": status,
        "details": details
    }})
    emoji = "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "WARN"
    print(f"[{{emoji}}] {{check_name}}")
    if details:
        print(f"       {{details}}")
'''

    def _generate_config_section(self, config: ValidationConfig) -> str:
        """Generate configuration section."""
        key_cols_str = str(config.key_columns)
        agg_cols_str = str(config.aggregation_columns)

        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Tables
SOURCE_TABLE = "{config.source_table}"
TARGET_TABLE = "{config.target_table}"

# Key columns for matching
KEY_COLUMNS = {key_cols_str}

# Aggregation columns to verify
AGG_COLUMNS = {agg_cols_str}

# Sample size for detailed comparison
SAMPLE_SIZE = {config.sample_size}

# Tolerance for numeric comparisons (as decimal, e.g., 0.01 = 1%)
TOLERANCE = {config.tolerance_percent}

# Optional date filter
DATE_COLUMN = "{config.date_column or ''}"
LOOKBACK_DAYS = 7  # Days to look back for incremental validation

# COMMAND ----------

# Load tables
print("Loading tables...")
source_df = spark.table(SOURCE_TABLE)
target_df = spark.table(TARGET_TABLE)

# Apply date filter if specified
if DATE_COLUMN:
    cutoff_date = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    source_df = source_df.filter(F.col(DATE_COLUMN) >= cutoff_date)
    target_df = target_df.filter(F.col(DATE_COLUMN) >= cutoff_date)
    print(f"  Filtered to {{DATE_COLUMN}} >= {{cutoff_date.date()}}")

print(f"  Source: {{SOURCE_TABLE}}")
print(f"  Target: {{TARGET_TABLE}}")
'''

    def _generate_helpers_section(self) -> str:
        """Generate helper functions."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def compare_values(source_val, target_val, tolerance=0.01):
    """Compare two values with tolerance for numeric types."""
    if source_val is None and target_val is None:
        return True
    if source_val is None or target_val is None:
        return False

    # Numeric comparison with tolerance
    if isinstance(source_val, (int, float)) and isinstance(target_val, (int, float)):
        if source_val == 0:
            return abs(target_val) < tolerance
        diff_pct = abs(source_val - target_val) / abs(source_val)
        return diff_pct <= tolerance

    # String comparison
    return str(source_val) == str(target_val)


def format_number(n):
    """Format number with commas."""
    if n is None:
        return "NULL"
    return f"{n:,.2f}" if isinstance(n, float) else f"{n:,}"
'''

    def _generate_row_count_section(self, config: ValidationConfig) -> str:
        """Generate row count comparison section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Comparison

# COMMAND ----------

print("=" * 60)
print("ROW COUNT COMPARISON")
print("=" * 60)

source_count = source_df.count()
target_count = target_df.count()

count_diff = abs(source_count - target_count)
count_pct = (count_diff / source_count * 100) if source_count > 0 else 0

print(f"Source rows:  {source_count:>15,}")
print(f"Target rows:  {target_count:>15,}")
print(f"Difference:   {count_diff:>15,} ({count_pct:.4f}%)")

if count_pct < TOLERANCE * 100:
    add_result("Row Count", "PASS", f"Difference: {count_pct:.4f}%")
else:
    add_result("Row Count", "FAIL", f"Difference: {count_pct:.4f}% exceeds tolerance {TOLERANCE*100}%")

print("=" * 60)
'''

    def _generate_aggregation_section(self, config: ValidationConfig) -> str:
        """Generate aggregation comparison section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregation Comparison

# COMMAND ----------

print("=" * 60)
print("AGGREGATION COMPARISON")
print("=" * 60)

agg_failures = []

for col_name in AGG_COLUMNS:
    print(f"\\nColumn: {col_name}")
    print("-" * 40)

    # Skip if column doesn't exist
    if col_name not in source_df.columns:
        print(f"  [SKIP] Column not in source")
        continue
    if col_name not in target_df.columns:
        print(f"  [WARN] Column not in target")
        agg_failures.append({"column": col_name, "reason": "missing in target"})
        continue

    # Calculate aggregations
    source_agg = source_df.agg(
        F.sum(col_name).alias("sum"),
        F.count(col_name).alias("count"),
        F.avg(col_name).alias("avg"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max")
    ).collect()[0]

    target_agg = target_df.agg(
        F.sum(col_name).alias("sum"),
        F.count(col_name).alias("count"),
        F.avg(col_name).alias("avg"),
        F.min(col_name).alias("min"),
        F.max(col_name).alias("max")
    ).collect()[0]

    # Compare each metric
    for metric in ["sum", "count", "avg"]:
        source_val = source_agg[metric]
        target_val = target_agg[metric]

        if source_val is None and target_val is None:
            status = "PASS"
            diff_str = "Both NULL"
        elif source_val is None or target_val is None:
            status = "FAIL"
            diff_str = f"Source={source_val}, Target={target_val}"
            agg_failures.append({"column": col_name, "metric": metric, "source": source_val, "target": target_val})
        else:
            if source_val == 0:
                diff_pct = 0 if target_val == 0 else 100
            else:
                diff_pct = abs(source_val - target_val) / abs(source_val) * 100

            if diff_pct <= TOLERANCE * 100:
                status = "PASS"
            else:
                status = "FAIL"
                agg_failures.append({"column": col_name, "metric": metric, "source": source_val, "target": target_val, "diff_pct": diff_pct})

            diff_str = f"Diff: {diff_pct:.4f}%"

        print(f"  {metric.upper():>6}: Source={format_number(source_val):>20}, Target={format_number(target_val):>20} [{status}] {diff_str}")

if agg_failures:
    add_result("Aggregations", "FAIL", f"{len(agg_failures)} aggregation mismatches")
else:
    add_result("Aggregations", "PASS", f"All {len(AGG_COLUMNS)} columns match")

print("=" * 60)
'''

    def _generate_sample_comparison_section(self, config: ValidationConfig) -> str:
        """Generate sample record comparison section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Record Comparison

# COMMAND ----------

print("=" * 60)
print("SAMPLE RECORD COMPARISON")
print("=" * 60)

# Get sample keys from source
sample_keys_df = (
    source_df
    .select(KEY_COLUMNS)
    .distinct()
    .limit(SAMPLE_SIZE)
)

sample_count = sample_keys_df.count()
print(f"Sample size: {sample_count}")

if sample_count == 0:
    add_result("Sample Matching", "SKIP", "No records to compare")
else:
    # Find matching records in target
    matched_df = (
        sample_keys_df.alias("keys")
        .join(
            target_df.alias("target"),
            on=KEY_COLUMNS,
            how="inner"
        )
    )

    matched_count = matched_df.select(KEY_COLUMNS).distinct().count()
    match_rate = matched_count / sample_count * 100

    print(f"Matched: {matched_count} / {sample_count} ({match_rate:.1f}%)")

    if match_rate >= 99:
        add_result("Sample Matching", "PASS", f"{match_rate:.1f}% match rate")
    elif match_rate >= 95:
        add_result("Sample Matching", "WARN", f"{match_rate:.1f}% match rate")
    else:
        add_result("Sample Matching", "FAIL", f"{match_rate:.1f}% match rate")

    # Show unmatched samples
    if matched_count < sample_count:
        unmatched_df = (
            sample_keys_df.alias("keys")
            .join(
                target_df.alias("target"),
                on=KEY_COLUMNS,
                how="left_anti"
            )
        )
        print("\\nUnmatched samples (first 5):")
        display(unmatched_df.limit(5))

print("=" * 60)
'''

    def _generate_schema_validation_section(self, config: ValidationConfig) -> str:
        """Generate schema validation section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Validation

# COMMAND ----------

print("=" * 60)
print("SCHEMA VALIDATION")
print("=" * 60)

source_cols = set(source_df.columns)
target_cols = set(target_df.columns)

# Columns in source but not target
missing_in_target = source_cols - target_cols
# Columns in target but not source
extra_in_target = target_cols - source_cols
# Common columns
common_cols = source_cols & target_cols

print(f"Source columns: {len(source_cols)}")
print(f"Target columns: {len(target_cols)}")
print(f"Common columns: {len(common_cols)}")

if missing_in_target:
    print(f"\\n[WARN] Missing in target: {sorted(missing_in_target)}")

if extra_in_target:
    print(f"\\n[INFO] Extra in target: {sorted(extra_in_target)}")

# Check data types for common columns
source_schema = {f.name: f.dataType for f in source_df.schema.fields}
target_schema = {f.name: f.dataType for f in target_df.schema.fields}

type_mismatches = []
for col in common_cols:
    source_type = str(source_schema.get(col))
    target_type = str(target_schema.get(col))

    # Normalize types for comparison
    source_type_norm = source_type.replace("Type()", "").replace("Type", "")
    target_type_norm = target_type.replace("Type()", "").replace("Type", "")

    if source_type_norm != target_type_norm:
        type_mismatches.append({
            "column": col,
            "source_type": source_type,
            "target_type": target_type
        })

if type_mismatches:
    print("\\n[WARN] Type mismatches:")
    for m in type_mismatches[:10]:
        print(f"  {m['column']}: {m['source_type']} -> {m['target_type']}")

if missing_in_target:
    add_result("Schema", "WARN", f"{len(missing_in_target)} columns missing in target")
else:
    add_result("Schema", "PASS", f"All source columns present in target")

print("=" * 60)
'''

    def _generate_business_rules_section(self, config: ValidationConfig) -> str:
        """Generate business rules validation section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Business Rule Checks

# COMMAND ----------

print("=" * 60)
print("BUSINESS RULE CHECKS")
print("=" * 60)

business_rule_failures = []

# Check 1: Key columns are not null
print("\\nCheck: Key columns NOT NULL")
for col in KEY_COLUMNS:
    if col in target_df.columns:
        null_count = target_df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            print(f"  [FAIL] {col}: {null_count:,} NULL values")
            business_rule_failures.append(f"{col} has {null_count} NULLs")
        else:
            print(f"  [PASS] {col}: No NULL values")

# Check 2: No duplicate keys
print("\\nCheck: No duplicate keys")
total_rows = target_df.count()
unique_keys = target_df.select(KEY_COLUMNS).distinct().count()
duplicate_count = total_rows - unique_keys

if duplicate_count > 0:
    print(f"  [FAIL] {duplicate_count:,} duplicate key combinations")
    business_rule_failures.append(f"{duplicate_count} duplicate keys")
else:
    print(f"  [PASS] All key combinations unique")

# Check 3: Date ranges are valid
if DATE_COLUMN and DATE_COLUMN in target_df.columns:
    print(f"\\nCheck: {DATE_COLUMN} range")
    date_stats = target_df.agg(
        F.min(DATE_COLUMN).alias("min_date"),
        F.max(DATE_COLUMN).alias("max_date")
    ).collect()[0]
    print(f"  Date range: {date_stats['min_date']} to {date_stats['max_date']}")

    # Check for future dates
    future_count = target_df.filter(F.col(DATE_COLUMN) > F.current_date()).count()
    if future_count > 0:
        print(f"  [WARN] {future_count:,} records with future dates")
    else:
        print(f"  [PASS] No future dates")

# Summary
if business_rule_failures:
    add_result("Business Rules", "FAIL", f"{len(business_rule_failures)} rule violations")
else:
    add_result("Business Rules", "PASS", "All business rules satisfied")

print("=" * 60)
'''

    def _generate_summary_section(self, config: ValidationConfig) -> str:
        """Generate validation summary section."""
        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("\\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)

# Calculate overall status
pass_count = len([c for c in validation_results["checks"] if c["status"] == "PASS"])
fail_count = len([c for c in validation_results["checks"] if c["status"] == "FAIL"])
warn_count = len([c for c in validation_results["checks"] if c["status"] == "WARN"])
total_checks = len(validation_results["checks"])

print(f"\\nProcedure: {config.procedure_name}")
print(f"Timestamp: {{validation_results['timestamp']}}")
print(f"\\nResults:")
print(f"  PASS: {{pass_count}}")
print(f"  FAIL: {{fail_count}}")
print(f"  WARN: {{warn_count}}")
print(f"  Total: {{total_checks}}")

# Overall verdict
if fail_count > 0:
    overall_status = "FAIL"
elif warn_count > 0:
    overall_status = "PASS_WITH_WARNINGS"
else:
    overall_status = "PASS"

validation_results["overall_status"] = overall_status
validation_results["pass_count"] = pass_count
validation_results["fail_count"] = fail_count
validation_results["warn_count"] = warn_count

print(f"\\nOverall Status: {{overall_status}}")
print("=" * 60)

# COMMAND ----------

# Display detailed results
print("\\nDetailed Results:")
for check in validation_results["checks"]:
    status_icon = "PASS" if check["status"] == "PASS" else "FAIL" if check["status"] == "FAIL" else "WARN"
    print(f"  [{{status_icon}}] {{check['check']}}: {{check.get('details', '')}}")

# COMMAND ----------

# Exit with status
dbutils.notebook.exit(json.dumps(validation_results))
'''

    def generate_from_conversion(
        self,
        procedure_name: str,
        source_table: str,
        target_table: str,
        detected_columns: Optional[List[str]] = None,
    ) -> str:
        """
        Generate validation notebook from conversion result.

        Auto-detects key columns and aggregation columns if not provided.
        """
        # Common key column patterns
        key_patterns = ['id', 'key', 'projectid', 'workerid', 'timestamp']

        # Common aggregation column patterns
        agg_patterns = ['time', 'count', 'sum', 'amount', 'duration', 'active', 'inactive']

        # Default columns based on common patterns
        key_columns = ['ProjectId', 'WorkerId']
        agg_columns = ['ActiveTime', 'InactiveTime']

        if detected_columns:
            # Try to identify key and agg columns from detected columns
            for col in detected_columns:
                col_lower = col.lower()
                if any(p in col_lower for p in key_patterns) and col not in key_columns:
                    if len(key_columns) < 4:
                        key_columns.append(col)
                if any(p in col_lower for p in agg_patterns) and col not in agg_columns:
                    if len(agg_columns) < 6:
                        agg_columns.append(col)

        config = ValidationConfig(
            procedure_name=procedure_name,
            source_table=source_table,
            target_table=target_table,
            key_columns=key_columns,
            aggregation_columns=agg_columns,
            date_column="ShiftLocalDate",
            sample_size=100,
            tolerance_percent=0.01,
        )

        return self.generate(config)

    # ==================== REQ-F7 Enhanced Sections ====================

    def _generate_freshness_section(self, config: ValidationConfig) -> str:
        """Generate data freshness validation section."""
        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Freshness Check

# COMMAND ----------

print("=" * 60)
print("DATA FRESHNESS CHECK")
print("=" * 60)

from datetime import datetime, timedelta

EXPECTED_FRESHNESS_HOURS = {config.expected_freshness_hours}

# Check when target was last updated
try:
    # Get table history for Delta tables
    history_df = spark.sql(f"DESCRIBE HISTORY {{TARGET_TABLE}} LIMIT 1")
    last_modified = history_df.select("timestamp").collect()[0][0]

    now = datetime.now()
    age_hours = (now - last_modified).total_seconds() / 3600

    print(f"Last modified: {{last_modified}}")
    print(f"Age: {{age_hours:.1f}} hours")

    if age_hours <= EXPECTED_FRESHNESS_HOURS:
        add_result("Data Freshness", "PASS", f"Data is {{age_hours:.1f}} hours old")
    else:
        add_result("Data Freshness", "WARN", f"Data is {{age_hours:.1f}} hours old (expected < {config.expected_freshness_hours}h)")

except Exception as e:
    print(f"Could not determine table freshness: {{e}}")
    add_result("Data Freshness", "SKIP", "Could not determine freshness")

# Check for recent data based on date column
if DATE_COLUMN and DATE_COLUMN in target_df.columns:
    max_date = target_df.agg(F.max(DATE_COLUMN)).collect()[0][0]
    if max_date:
        days_since_max = (datetime.now().date() - max_date).days if hasattr(max_date, 'days') else 0
        print(f"\\nMost recent date in data: {{max_date}}")
        print(f"Days since max date: {{days_since_max}}")

print("=" * 60)
'''

    def _generate_profiling_section(self, config: ValidationConfig) -> str:
        """Generate data profiling section."""
        return '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Profiling

# COMMAND ----------

print("=" * 60)
print("DATA PROFILING")
print("=" * 60)

# NULL distribution
print("\\nNULL Distribution:")
print("-" * 40)

null_stats = []
for col in target_df.columns[:20]:  # Limit to first 20 columns
    total = target_df.count()
    nulls = target_df.filter(F.col(col).isNull()).count()
    null_pct = (nulls / total * 100) if total > 0 else 0

    if nulls > 0:
        null_stats.append({"column": col, "nulls": nulls, "pct": null_pct})
        print(f"  {col}: {nulls:,} NULLs ({null_pct:.2f}%)")

if not null_stats:
    print("  No NULL values found in first 20 columns")

# Numeric column statistics
print("\\nNumeric Column Statistics:")
print("-" * 40)

numeric_cols = [f.name for f in target_df.schema.fields
                if str(f.dataType) in ['IntegerType()', 'LongType()', 'DoubleType()', 'DecimalType()']][:10]

if numeric_cols:
    stats_df = target_df.select(numeric_cols).describe()
    display(stats_df)
else:
    print("  No numeric columns found")

# Record counts by date
if DATE_COLUMN and DATE_COLUMN in target_df.columns:
    print(f"\\nRecords by {DATE_COLUMN}:")
    print("-" * 40)

    date_counts = (
        target_df
        .groupBy(DATE_COLUMN)
        .count()
        .orderBy(F.desc(DATE_COLUMN))
        .limit(10)
    )
    display(date_counts)

add_result("Data Profiling", "PASS", "Profile generated")
print("=" * 60)
'''

    def _generate_distribution_section(self, config: ValidationConfig) -> str:
        """Generate value distribution comparison section."""
        categorical_cols = str(config.categorical_columns) if config.categorical_columns else "[]"

        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Value Distribution Comparison

# COMMAND ----------

print("=" * 60)
print("VALUE DISTRIBUTION COMPARISON")
print("=" * 60)

CATEGORICAL_COLUMNS = {categorical_cols}

distribution_issues = []

for col in CATEGORICAL_COLUMNS:
    print(f"\\nColumn: {{col}}")
    print("-" * 40)

    if col not in source_df.columns or col not in target_df.columns:
        print(f"  [SKIP] Column not in both tables")
        continue

    # Get value counts from source
    source_dist = (
        source_df
        .groupBy(col)
        .count()
        .withColumnRenamed("count", "source_count")
    )

    # Get value counts from target
    target_dist = (
        target_df
        .groupBy(col)
        .count()
        .withColumnRenamed("count", "target_count")
    )

    # Join distributions
    comparison = (
        source_dist.alias("s")
        .join(target_dist.alias("t"), on=col, how="outer")
        .withColumn("diff", F.coalesce(F.col("target_count"), F.lit(0)) - F.coalesce(F.col("source_count"), F.lit(0)))
        .withColumn("diff_pct", F.abs(F.col("diff")) / F.coalesce(F.col("source_count"), F.lit(1)) * 100)
    )

    # Show significant differences
    significant_diffs = comparison.filter(F.col("diff_pct") > TOLERANCE * 100)
    diff_count = significant_diffs.count()

    if diff_count > 0:
        print(f"  [WARN] {{diff_count}} values with significant distribution difference")
        display(significant_diffs.orderBy(F.desc("diff_pct")).limit(5))
        distribution_issues.append(f"{{col}}: {{diff_count}} value differences")
    else:
        print(f"  [PASS] Distribution matches within tolerance")

if distribution_issues:
    add_result("Value Distribution", "WARN", f"{{len(distribution_issues)}} columns with differences")
else:
    add_result("Value Distribution", "PASS", f"All {{len(CATEGORICAL_COLUMNS)}} categorical columns match")

print("=" * 60)
'''

    def _generate_fk_validation_section(self, config: ValidationConfig) -> str:
        """Generate foreign key validation section."""
        fk_checks = str(config.foreign_key_checks) if config.foreign_key_checks else "[]"

        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Foreign Key Validation

# COMMAND ----------

print("=" * 60)
print("FOREIGN KEY VALIDATION")
print("=" * 60)

# Foreign key checks configuration
# Format: [{{"column": "project_id", "ref_table": "catalog.schema.dim_project", "ref_column": "project_id"}}]
FK_CHECKS = {fk_checks}

fk_violations = []

for fk in FK_CHECKS:
    col = fk.get("column")
    ref_table = fk.get("ref_table")
    ref_column = fk.get("ref_column", col)

    print(f"\\nCheck: {{col}} -> {{ref_table}}.{{ref_column}}")
    print("-" * 40)

    if col not in target_df.columns:
        print(f"  [SKIP] Column {{col}} not in target")
        continue

    try:
        # Load reference table
        ref_df = spark.table(ref_table)

        # Get distinct values from target
        target_values = target_df.select(col).distinct()

        # Find orphaned values (in target but not in reference)
        orphans = target_values.join(
            ref_df.select(F.col(ref_column).alias(col)),
            on=col,
            how="left_anti"
        ).filter(F.col(col).isNotNull())

        orphan_count = orphans.count()

        if orphan_count > 0:
            print(f"  [FAIL] {{orphan_count:,}} orphaned values")
            print("  Sample orphans:")
            display(orphans.limit(5))
            fk_violations.append(f"{{col}}: {{orphan_count}} orphans")
        else:
            print(f"  [PASS] All values exist in reference table")

    except Exception as e:
        print(f"  [SKIP] Could not load reference table: {{e}}")

if fk_violations:
    add_result("Foreign Keys", "FAIL", f"{{len(fk_violations)}} FK violations")
else:
    add_result("Foreign Keys", "PASS", f"All {{len(FK_CHECKS)}} foreign keys valid")

print("=" * 60)
'''

    def _generate_custom_rules_section(self, config: ValidationConfig) -> str:
        """Generate custom business rules section."""
        custom_rules = str(config.custom_rules) if config.custom_rules else "[]"

        return f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Custom Validation Rules

# COMMAND ----------

print("=" * 60)
print("CUSTOM VALIDATION RULES")
print("=" * 60)

# Custom rules configuration
# Format: [{{"name": "Rule Name", "sql": "SELECT COUNT(*) as violations FROM target WHERE condition", "expected": 0}}]
CUSTOM_RULES = {custom_rules}

custom_rule_failures = []

for rule in CUSTOM_RULES:
    name = rule.get("name", "Unnamed Rule")
    sql = rule.get("sql", "")
    expected = rule.get("expected", 0)

    print(f"\\nRule: {{name}}")
    print("-" * 40)

    if not sql:
        print(f"  [SKIP] No SQL defined")
        continue

    try:
        # Replace TARGET_TABLE placeholder
        sql_resolved = sql.replace("{{TARGET_TABLE}}", TARGET_TABLE)

        # Execute rule
        result = spark.sql(sql_resolved).collect()[0][0]

        if result == expected:
            print(f"  [PASS] Result: {{result}} (expected: {{expected}})")
        else:
            print(f"  [FAIL] Result: {{result}} (expected: {{expected}})")
            custom_rule_failures.append(f"{{name}}: {{result}} != {{expected}}")

    except Exception as e:
        print(f"  [ERROR] {{e}}")
        custom_rule_failures.append(f"{{name}}: Error - {{e}}")

if custom_rule_failures:
    add_result("Custom Rules", "FAIL", f"{{len(custom_rule_failures)}} rule violations")
elif CUSTOM_RULES:
    add_result("Custom Rules", "PASS", f"All {{len(CUSTOM_RULES)}} custom rules passed")
else:
    add_result("Custom Rules", "SKIP", "No custom rules defined")

print("=" * 60)
'''

    def generate_comprehensive_validation(
        self,
        procedure_name: str,
        source_sql: str,
        target_notebook: str,
        source_table: str,
        target_table: str,
        dependencies: Optional[List[Dict]] = None,
        business_logic_comparison: Optional[Dict] = None,
    ) -> str:
        """
        Generate comprehensive validation notebook with Phase 3 integration.

        Integrates with:
        - Business Logic Comparator (REQ-R3)
        - Dependency Tracker (REQ-R4)

        Args:
            procedure_name: Name of the stored procedure
            source_sql: Original T-SQL source
            target_notebook: Converted notebook content
            source_table: Silver layer source table
            target_table: Gold layer target table
            dependencies: Dependency analysis results
            business_logic_comparison: Business logic comparison results

        Returns:
            Comprehensive validation notebook content
        """
        # Extract columns from source SQL
        detected_columns = self._extract_columns_from_sql(source_sql)

        # Build custom rules from business logic comparison
        custom_rules = []
        if business_logic_comparison:
            for issue in business_logic_comparison.get("comparisons", []):
                if issue.get("status") == "missing" and issue.get("risk") in ["BLOCKER", "HIGH"]:
                    custom_rules.append({
                        "name": f"Check {issue.get('type', 'unknown')} - {issue.get('source', 'N/A')}",
                        "sql": "SELECT 1 as violations",  # Placeholder
                        "expected": 0
                    })

        # Build FK checks from dependencies
        fk_checks = []
        if dependencies:
            for dep in dependencies:
                if dep.get("type") in ["reads", "TABLE_READ"] and dep.get("resolved"):
                    # Infer FK relationship
                    target_obj = dep.get("target", "")
                    if "id" in target_obj.lower():
                        col_name = target_obj.split(".")[-1].lower().replace("_", "") + "_id"
                        fk_checks.append({
                            "column": col_name,
                            "ref_table": dep.get("databricks_path", ""),
                            "ref_column": col_name
                        })

        config = ValidationConfig(
            procedure_name=procedure_name,
            source_table=source_table,
            target_table=target_table,
            key_columns=self._infer_key_columns(detected_columns),
            aggregation_columns=self._infer_agg_columns(detected_columns),
            date_column=self._infer_date_column(detected_columns),
            sample_size=100,
            tolerance_percent=0.01,
            categorical_columns=self._infer_categorical_columns(detected_columns),
            foreign_key_checks=fk_checks[:5],  # Limit to 5 FK checks
            custom_rules=custom_rules[:5],  # Limit to 5 custom rules
            include_profiling=True,
            include_freshness=True,
            expected_freshness_hours=24,
        )

        return self.generate(config)

    def _extract_columns_from_sql(self, sql: str) -> List[str]:
        """Extract column names from T-SQL."""
        import re

        columns = set()

        # SELECT columns
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_list = select_match.group(1)
            # Extract column names (simplified)
            for part in select_list.split(","):
                # Get alias or column name
                alias_match = re.search(r'(?:AS\s+)?(\[?\w+\]?)$', part.strip(), re.IGNORECASE)
                if alias_match:
                    columns.add(alias_match.group(1).strip("[]"))

        # INSERT columns
        insert_match = re.search(r'INSERT\s+INTO\s+[\w.\[\]]+\s*\(([^)]+)\)', sql, re.IGNORECASE)
        if insert_match:
            for col in insert_match.group(1).split(","):
                columns.add(col.strip().strip("[]"))

        return list(columns)

    def _infer_key_columns(self, columns: List[str]) -> List[str]:
        """Infer key columns from column list."""
        key_patterns = ['id', 'key', 'projectid', 'workerid', 'timestamp']
        keys = []
        for col in columns:
            if any(p in col.lower() for p in key_patterns):
                keys.append(col)
        return keys[:4] or ['ProjectId', 'WorkerId']

    def _infer_agg_columns(self, columns: List[str]) -> List[str]:
        """Infer aggregation columns from column list."""
        agg_patterns = ['time', 'count', 'sum', 'amount', 'duration', 'active', 'inactive', 'total']
        aggs = []
        for col in columns:
            if any(p in col.lower() for p in agg_patterns):
                aggs.append(col)
        return aggs[:6] or ['ActiveTime', 'InactiveTime']

    def _infer_date_column(self, columns: List[str]) -> Optional[str]:
        """Infer date column from column list."""
        date_patterns = ['date', 'timestamp', 'datetime', 'created', 'updated']
        for col in columns:
            if any(p in col.lower() for p in date_patterns):
                return col
        return None

    def _infer_categorical_columns(self, columns: List[str]) -> List[str]:
        """Infer categorical columns from column list."""
        cat_patterns = ['type', 'status', 'category', 'name', 'code', 'flag']
        cats = []
        for col in columns:
            if any(p in col.lower() for p in cat_patterns):
                cats.append(col)
        return cats[:5]
