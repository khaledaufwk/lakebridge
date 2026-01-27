"""
Tests for Phase 4 Review Skill Components.

Tests for:
- REQ-R5: SP-Specific Review Checklist Generator
- REQ-R6: Data Type Validator
- REQ-R7: Enhanced Report Format
"""


def test_checklist_generator_basic():
    """Test basic checklist generation."""
    from checklist_generator import SPChecklistGenerator, SPChecklistConfig

    generator = SPChecklistGenerator()

    config = SPChecklistConfig(
        procedure_name="stg.spCalculateMetrics",
        source_line_count=250,
        target_line_count=180,
        patterns_found=[{"name": "CURSOR", "converted": False}],
        patterns_converted=[{"name": "TEMP_TABLE", "converted": True}],
        dependencies=[
            {"target": "dbo.Worker", "type": "TABLE_READ", "resolved": True},
            {"target": "dbo.Zone", "type": "TABLE_READ", "resolved": False},
        ],
        conversion_score=0.75,
        complexity_score=6,
    )

    checklist = generator.generate_checklist(config)

    # Verify sections present
    assert "## Stored Procedure Review" in checklist
    assert "stg.spCalculateMetrics" in checklist
    assert "### 1. Source Analysis" in checklist
    assert "### 2. Pattern Conversion Status" in checklist
    assert "### 3. Business Logic Validation" in checklist
    assert "### 4. Dependency Resolution" in checklist
    assert "### 5. Data Quality Rules" in checklist
    assert "### 6. Performance Considerations" in checklist
    assert "### 7. Testing Requirements" in checklist
    assert "### Review Verdict" in checklist

    # Verify content
    assert "250" in checklist  # Source lines
    assert "CURSOR" in checklist
    assert "TEMP_TABLE" in checklist
    assert "dbo.Worker" in checklist
    assert "dbo.Zone" in checklist

    print("PASS: test_checklist_generator_basic")


def test_checklist_generator_from_analysis():
    """Test checklist generation from analysis results."""
    from checklist_generator import SPChecklistGenerator

    generator = SPChecklistGenerator()

    source_sql = """
    DECLARE @cursor CURSOR FOR SELECT * FROM dbo.Worker
    SELECT * INTO #temp FROM dbo.Project
    MERGE INTO dbo.FactOutput USING #temp ON ...
    """

    target_notebook = """
    # Databricks notebook source
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    workers_df = spark.table("silver.silver_worker")
    projects_df = spark.table("silver.silver_project")
    """

    checklist = generator.generate_from_analysis(
        procedure_name="test_proc",
        source_sql=source_sql,
        target_notebook=target_notebook,
        conversion_result={
            "patterns_converted": ["MERGE"],
            "patterns_manual": ["CURSOR", "TEMP_TABLE"],
            "conversion_score": 0.6,
        },
        dependency_analysis={
            "dependencies": [
                {"target": "dbo.Worker", "type": "TABLE_READ", "resolved": True},
            ]
        },
    )

    assert "test_proc" in checklist
    assert "CURSOR" in checklist
    assert "MERGE" in checklist

    print("PASS: test_checklist_generator_from_analysis")


def test_quick_checklists():
    """Test quick checklist generation."""
    from checklist_generator import QuickChecklist

    # CURSOR checklist
    cursor_checklist = QuickChecklist.for_cursor_conversion("my_proc")
    assert "CURSOR Conversion" in cursor_checklist
    assert "Window Function" in cursor_checklist
    assert "partition columns" in cursor_checklist.lower()

    # MERGE checklist
    merge_checklist = QuickChecklist.for_merge_conversion("my_proc")
    assert "MERGE Conversion" in merge_checklist
    assert "Delta MERGE" in merge_checklist
    assert "WHEN MATCHED" in merge_checklist

    # Spatial checklist
    spatial_checklist = QuickChecklist.for_spatial_conversion("my_proc")
    assert "Spatial Conversion" in spatial_checklist
    assert "H3" in spatial_checklist
    assert "Haversine" in spatial_checklist

    # Temp table checklist
    temp_checklist = QuickChecklist.for_temp_table_conversion("my_proc")
    assert "Temp Table Conversion" in temp_checklist
    assert "createOrReplaceTempView" in temp_checklist

    print("PASS: test_quick_checklists")


def test_datatype_validator_extract_tsql():
    """Test T-SQL type extraction."""
    from datatype_validator import DataTypeValidator

    validator = DataTypeValidator()

    source_sql = """
    CREATE TABLE dbo.Worker (
        WorkerId INT NOT NULL,
        WorkerName NVARCHAR(100),
        Salary DECIMAL(18,4),
        IsActive BIT,
        CreatedAt DATETIME,
        LocationPoint GEOGRAPHY
    )
    """

    columns = validator.extract_column_types_from_tsql(source_sql)

    assert len(columns) >= 5

    # Find specific columns
    worker_id = next((c for c in columns if c.name.lower() == "workerid"), None)
    assert worker_id is not None
    assert worker_id.base_type == "INT"
    assert worker_id.is_nullable is False

    worker_name = next((c for c in columns if c.name.lower() == "workername"), None)
    assert worker_name is not None
    assert worker_name.base_type == "NVARCHAR"
    assert worker_name.max_length == 100

    salary = next((c for c in columns if c.name.lower() == "salary"), None)
    assert salary is not None
    assert salary.base_type == "DECIMAL"
    assert salary.precision == 18
    assert salary.scale == 4

    print("PASS: test_datatype_validator_extract_tsql")


def test_datatype_validator_validate():
    """Test type validation."""
    from datatype_validator import DataTypeValidator, ColumnTypeInfo, ValidationSeverity

    validator = DataTypeValidator()

    # Test precision loss warning
    money_col = ColumnTypeInfo(
        name="amount",
        base_type="MONEY",
        raw_type="MONEY"
    )

    issues = validator.validate_type_mapping(money_col)

    assert len(issues) >= 1
    assert any(i.severity == ValidationSeverity.WARNING for i in issues)

    # Test special handling info
    geo_col = ColumnTypeInfo(
        name="location",
        base_type="GEOGRAPHY",
        raw_type="GEOGRAPHY"
    )

    issues = validator.validate_type_mapping(geo_col)

    assert len(issues) >= 1
    assert any(i.severity == ValidationSeverity.INFO for i in issues)
    assert any("special handling" in i.message.lower() for i in issues)

    print("PASS: test_datatype_validator_validate")


def test_datatype_validator_report():
    """Test validation report generation."""
    from datatype_validator import DataTypeValidator

    validator = DataTypeValidator()

    source_sql = """
    CREATE TABLE test (
        id INT NOT NULL,
        amount MONEY,
        location GEOGRAPHY
    )
    """

    target_notebook = """
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("amount", DecimalType(19, 4)),
        StructField("location", StringType())
    ])
    """

    result = validator.validate_all_types(source_sql, target_notebook)

    assert "source_columns" in result
    assert "issues" in result
    assert "verdict" in result

    report = validator.generate_report(result)

    assert "## Data Type Validation Report" in report
    assert "Verdict" in report

    print("PASS: test_datatype_validator_report")


def test_datatype_validator_mapping_table():
    """Test type mapping table generation."""
    from datatype_validator import DataTypeValidator

    validator = DataTypeValidator()

    table = validator.get_type_mapping_table()

    assert "## T-SQL to Spark Type Mapping Reference" in table
    assert "NVARCHAR" in table
    assert "STRING" in table
    assert "DATETIME" in table
    assert "TIMESTAMP" in table

    print("PASS: test_datatype_validator_mapping_table")


def test_enhanced_report_basic():
    """Test basic enhanced report generation."""
    from enhanced_report import EnhancedReviewReport, SPAnalysisResult

    sp_analysis = SPAnalysisResult(
        procedure_name="test_proc",
        source_lines=250,
        target_lines=180,
        conversion_score=0.85,
        complexity_score=6,
    )

    report = EnhancedReviewReport(
        summary="Test review of stored procedure conversion",
        verdict="PASS",
        issues=[],
        sp_analysis=sp_analysis,
    )

    markdown = report.to_markdown()

    assert "# SP Migration Review Report" in markdown
    assert "test_proc" in markdown
    assert "85%" in markdown  # Conversion score
    assert "6/10" in markdown  # Complexity
    assert "PASS" in markdown

    print("PASS: test_enhanced_report_basic")


def test_enhanced_report_with_patterns():
    """Test enhanced report with pattern status."""
    from enhanced_report import (
        EnhancedReviewReport,
        SPAnalysisResult,
        PatternConversionStatus,
        DependencyStatus,
    )

    sp_analysis = SPAnalysisResult(
        procedure_name="test_proc",
        conversion_score=0.7,
    )

    sp_analysis.patterns = [
        PatternConversionStatus("CURSOR", 2, 2, True, "Window functions"),
        PatternConversionStatus("TEMP_TABLE", 3, 3, True, "createOrReplaceTempView"),
        PatternConversionStatus("MERGE", 1, 0, False, None),
    ]

    sp_analysis.dependencies = [
        DependencyStatus("dbo.Worker", "TABLE_READ", True, "silver.silver_worker"),
        DependencyStatus("dbo.Zone", "TABLE_READ", False, None),
    ]

    report = EnhancedReviewReport(
        summary="Review with patterns and dependencies",
        verdict="NEEDS_WORK",
        issues=[],
        sp_analysis=sp_analysis,
    )

    markdown = report.to_markdown()

    assert "Pattern Conversion Status" in markdown
    assert "CURSOR" in markdown
    assert "CONVERTED" in markdown
    assert "MISSING" in markdown
    assert "Dependencies" in markdown
    assert "RESOLVED" in markdown
    assert "dbo.Zone" in markdown

    print("PASS: test_enhanced_report_with_patterns")


def test_report_builder():
    """Test report builder."""
    from enhanced_report import ReportBuilder

    builder = ReportBuilder("stg.spCalculateMetrics")

    report = (
        builder
        .set_summary("Comprehensive review of metric calculation procedure")
        .set_source_info("sql/stg.spCalculateMetrics.sql", 350)
        .set_target_info("notebooks/calc_metrics.py", 250)
        .set_conversion_score(0.9)
        .set_complexity_score(7)
        .add_pattern("CURSOR", 2, 2, True, "lag/lead")
        .add_pattern("TEMP_TABLE", 3, 3, True, "temp views")
        .add_dependency("dbo.Worker", "TABLE_READ", True, "silver.silver_worker")
        .add_dependency("dbo.Project", "TABLE_READ", True, "silver.silver_project")
        .build()
    )

    assert report.verdict == "PASS"
    assert report.sp_analysis.procedure_name == "stg.spCalculateMetrics"
    assert report.sp_analysis.conversion_score == 0.9
    assert len(report.sp_analysis.patterns) == 2
    assert len(report.sp_analysis.dependencies) == 2

    markdown = report.to_markdown()
    assert "stg.spCalculateMetrics" in markdown
    assert "90%" in markdown

    print("PASS: test_report_builder")


def test_report_builder_with_blockers():
    """Test report builder correctly identifies blockers."""
    from enhanced_report import ReportBuilder

    builder = ReportBuilder("test_proc")

    report = (
        builder
        .set_summary("Test with unresolved dependencies")
        .set_conversion_score(0.5)
        .add_pattern("CURSOR", 2, 0, False)  # Unconverted
        .add_dependency("dbo.MissingTable", "TABLE_READ", False)  # Unresolved
        .build()
    )

    assert report.verdict == "FAIL"

    markdown = report.to_markdown()
    assert "FAIL" in markdown
    assert "Unresolved Dependencies" in markdown or "BLOCKERS" in markdown.upper()

    print("PASS: test_report_builder_with_blockers")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Running Phase 4 Review Skill Tests")
    print("=" * 60 + "\n")

    tests = [
        test_checklist_generator_basic,
        test_checklist_generator_from_analysis,
        test_quick_checklists,
        test_datatype_validator_extract_tsql,
        test_datatype_validator_validate,
        test_datatype_validator_report,
        test_datatype_validator_mapping_table,
        test_enhanced_report_basic,
        test_enhanced_report_with_patterns,
        test_report_builder,
        test_report_builder_with_blockers,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
