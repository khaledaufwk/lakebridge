"""
Tests for Phase 3 Review Skill Components.

Tests for:
- REQ-R3: Business Logic Comparator
- REQ-R4: Dependency Tracker
"""


def test_business_logic_extractor_tsql():
    """Test extraction of business logic from T-SQL."""
    from analyzer_business_logic import BusinessLogicExtractor

    extractor = BusinessLogicExtractor()

    source_sql = """
    SELECT
        w.WorkerId,
        w.WorkerName,
        p.ProjectId,
        SUM(f.ActiveTime) AS TotalActiveTime,
        COUNT(*) AS RecordCount
    FROM dbo.Worker w
    INNER JOIN dbo.Project p ON w.ProjectId = p.ProjectId
    LEFT JOIN fact.FactWorkersHistory f ON w.WorkerId = f.WorkerId
    WHERE w.IsActive = 1
        AND f.LocalDate >= '2024-01-01'
    GROUP BY w.WorkerId, w.WorkerName, p.ProjectId
    ORDER BY TotalActiveTime DESC
    """

    logic = extractor.extract_from_tsql(source_sql)

    # Check input tables detected
    input_tables = [f"{t.schema}.{t.table}" for t in logic.input_tables]
    assert "dbo.Worker" in input_tables, f"Expected dbo.Worker in {input_tables}"
    assert "dbo.Project" in input_tables, f"Expected dbo.Project in {input_tables}"

    # Check aggregations detected
    agg_funcs = [a.function for a in logic.aggregations]
    assert "SUM" in agg_funcs, "Expected SUM aggregation"
    assert "COUNT" in agg_funcs, "Expected COUNT aggregation"

    # Check joins detected
    assert len(logic.joins) >= 1, "Expected at least 1 join"

    # Check group by
    assert len(logic.group_by) >= 2, f"Expected at least 2 group by columns, got {logic.group_by}"

    print("PASS: test_business_logic_extractor_tsql")


def test_business_logic_extractor_spark():
    """Test extraction of business logic from Spark code."""
    from analyzer_business_logic import BusinessLogicExtractor

    extractor = BusinessLogicExtractor()

    notebook_code = """
    from pyspark.sql import functions as F

    # Load tables
    workers_df = spark.table("wakecap_prod.silver.silver_worker")
    projects_df = spark.table("wakecap_prod.silver.silver_project")
    history_df = spark.table("wakecap_prod.gold.gold_fact_workers_history")

    # Join tables
    result_df = (
        workers_df.alias("w")
        .join(projects_df.alias("p"), on="ProjectId", how="inner")
        .join(history_df.alias("h"), on="WorkerId", how="left")
        .filter(F.col("IsActive") == 1)
        .groupBy("WorkerId", "WorkerName", "ProjectId")
        .agg(
            F.sum("ActiveTime").alias("TotalActiveTime"),
            F.count("*").alias("RecordCount")
        )
        .orderBy(F.desc("TotalActiveTime"))
    )

    # Write result
    result_df.write.mode("overwrite").saveAsTable("wakecap_prod.gold.gold_summary")
    """

    logic = extractor.extract_from_notebook(notebook_code)

    # Check input tables detected
    input_tables = [f"{t.schema}.{t.table}" for t in logic.input_tables]
    assert any("silver_worker" in t for t in input_tables), f"Expected silver_worker in {input_tables}"

    # Check output tables detected
    output_tables = [f"{t.schema}.{t.table}" for t in logic.output_tables]
    assert any("gold_summary" in t for t in output_tables), f"Expected gold_summary in {output_tables}"

    # Check aggregations
    agg_funcs = [a.function for a in logic.aggregations]
    assert "SUM" in agg_funcs, "Expected SUM aggregation"
    assert "COUNT" in agg_funcs, "Expected COUNT aggregation"

    print("PASS: test_business_logic_extractor_spark")


def test_business_logic_comparator():
    """Test comparison of source and target business logic."""
    from analyzer_business_logic import BusinessLogicComparator

    comparator = BusinessLogicComparator(table_mapping={
        "dbo.worker": "wakecap_prod.silver.silver_worker",
        "dbo.project": "wakecap_prod.silver.silver_project",
    })

    source_sql = """
    SELECT w.WorkerId, SUM(ActiveTime) AS TotalTime
    FROM dbo.Worker w
    INNER JOIN dbo.Project p ON w.ProjectId = p.ProjectId
    WHERE w.IsActive = 1
    GROUP BY w.WorkerId
    """

    target_code = """
    from pyspark.sql import functions as F

    workers_df = spark.table("wakecap_prod.silver.silver_worker")
    projects_df = spark.table("wakecap_prod.silver.silver_project")

    result_df = (
        workers_df.alias("w")
        .join(projects_df.alias("p"), on="ProjectId", how="inner")
        .filter(F.col("IsActive") == 1)
        .groupBy("WorkerId")
        .agg(F.sum("ActiveTime").alias("TotalTime"))
    )
    """

    result = comparator.compare(source_sql, target_code, "test_procedure")

    assert "procedure_name" in result
    assert "verdict" in result
    assert result["verdict"] in ["PASS", "NEEDS_REVIEW", "FAIL"]

    print(f"PASS: test_business_logic_comparator (verdict: {result['verdict']})")


def test_dependency_tracker_extract():
    """Test dependency extraction from T-SQL."""
    from analyzer_dependencies import DependencyTracker, DependencyType

    tracker = DependencyTracker(catalog="wakecap_prod")

    source_sql = """
    CREATE PROCEDURE stg.spCalculateMetrics
    AS
    BEGIN
        -- Read from multiple tables
        SELECT *
        FROM dbo.Worker w
        INNER JOIN dbo.Project p ON w.ProjectId = p.ProjectId
        LEFT JOIN dim.DimZone z ON w.ZoneId = z.ZoneId

        -- Call another procedure
        EXEC dbo.spUpdateStats

        -- Use a function
        SELECT dbo.fnCalcDuration(StartTime, EndTime) AS Duration
        FROM fact.FactActivity

        -- Write to target table
        INSERT INTO gold.FactMetrics (ProjectId, Value)
        SELECT ProjectId, SUM(Value)
        FROM #TempResults
        GROUP BY ProjectId
    END
    """

    dependencies = tracker.extract_dependencies(source_sql, "stg.spCalculateMetrics")

    # Check table reads
    read_deps = [d for d in dependencies if d.dependency_type == DependencyType.TABLE_READ]
    read_tables = [d.target_object.lower() for d in read_deps]
    assert any("worker" in t for t in read_tables), f"Expected Worker table read in {read_tables}"
    assert any("project" in t for t in read_tables), f"Expected Project table read in {read_tables}"

    # Check table writes
    write_deps = [d for d in dependencies if d.dependency_type == DependencyType.TABLE_WRITE]
    write_tables = [d.target_object.lower() for d in write_deps]
    assert any("factmetrics" in t for t in write_tables), f"Expected FactMetrics table write in {write_tables}"

    # Check procedure calls
    proc_deps = [d for d in dependencies if d.dependency_type == DependencyType.PROCEDURE_CALL]
    assert len(proc_deps) >= 1, "Expected at least 1 procedure call dependency"

    # Check function calls
    func_deps = [d for d in dependencies if d.dependency_type == DependencyType.FUNCTION_CALL]
    assert len(func_deps) >= 1, "Expected at least 1 function call dependency"

    print(f"PASS: test_dependency_tracker_extract ({len(dependencies)} dependencies)")


def test_dependency_tracker_resolve():
    """Test dependency resolution."""
    from analyzer_dependencies import DependencyTracker

    tracker = DependencyTracker(
        catalog="wakecap_prod",
        table_mapping={
            "dbo.worker": "wakecap_prod.silver.silver_worker",
            "dbo.project": "wakecap_prod.silver.silver_project",
        }
    )

    source_sql = """
    SELECT * FROM dbo.Worker w
    INNER JOIN dbo.Project p ON w.ProjectId = p.ProjectId
    """

    dependencies = tracker.extract_dependencies(source_sql, "test_proc")

    # Simulate existing tables
    existing_tables = {
        "wakecap_prod.silver.silver_worker",
        "wakecap_prod.silver.silver_project",
    }

    resolved = tracker.resolve_dependencies(dependencies, existing_tables)

    # Check resolution
    resolved_count = len([d for d in resolved if d.is_resolved])
    assert resolved_count >= 1, "Expected at least 1 resolved dependency"

    print(f"PASS: test_dependency_tracker_resolve ({resolved_count} resolved)")


def test_dependency_tracker_report():
    """Test dependency report generation."""
    from analyzer_dependencies import DependencyTracker

    tracker = DependencyTracker(catalog="wakecap_prod")

    source_sql = """
    SELECT * FROM dbo.Worker w
    INNER JOIN dbo.Project p ON w.ProjectId = p.ProjectId
    """

    dependencies = tracker.extract_dependencies(source_sql, "stg.spTestProc")
    report = tracker.generate_dependency_report("stg.spTestProc", dependencies)

    assert "## Dependency Report" in report
    assert "stg.spTestProc" in report
    assert "Total Dependencies" in report

    print("PASS: test_dependency_tracker_report")


def test_dependency_analyzer():
    """Test high-level dependency analyzer."""
    from analyzer_dependencies import DependencyAnalyzer

    analyzer = DependencyAnalyzer(
        catalog="wakecap_prod",
        table_mapping={
            "dbo.worker": "wakecap_prod.silver.silver_worker",
        }
    )

    source_sql = """
    SELECT * FROM dbo.Worker
    INSERT INTO gold.FactSummary SELECT * FROM dbo.Worker
    """

    result = analyzer.analyze(
        source_sql,
        "test_proc",
        existing_tables={"wakecap_prod.silver.silver_worker"}
    )

    assert "procedure_name" in result
    assert "total_dependencies" in result
    assert "readiness_percentage" in result
    assert "blockers" in result

    print(f"PASS: test_dependency_analyzer (readiness: {result['readiness_percentage']:.1f}%)")


def test_conversion_order():
    """Test topological sort for conversion order."""
    from analyzer_dependencies import DependencyTracker

    tracker = DependencyTracker(catalog="wakecap_prod")

    # Create dependency chain: proc_a -> proc_b -> proc_c
    proc_a = "EXEC dbo.spProcB"
    proc_b = "EXEC dbo.spProcC"
    proc_c = "SELECT 1"

    tracker.extract_dependencies(proc_a, "dbo.spProcA")
    tracker.extract_dependencies(proc_b, "dbo.spProcB")
    tracker.extract_dependencies(proc_c, "dbo.spProcC")

    order = tracker.get_conversion_order([
        "dbo.spProcA",
        "dbo.spProcB",
        "dbo.spProcC"
    ])

    # C should come before B, B before A
    c_idx = order.index("dbo.spProcC")
    b_idx = order.index("dbo.spProcB")
    a_idx = order.index("dbo.spProcA")

    assert c_idx < b_idx, f"Expected C before B: {order}"
    assert b_idx < a_idx, f"Expected B before A: {order}"

    print(f"PASS: test_conversion_order ({order})")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Running Phase 3 Review Skill Tests")
    print("=" * 60 + "\n")

    tests = [
        test_business_logic_extractor_tsql,
        test_business_logic_extractor_spark,
        test_business_logic_comparator,
        test_dependency_tracker_extract,
        test_dependency_tracker_resolve,
        test_dependency_tracker_report,
        test_dependency_analyzer,
        test_conversion_order,
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
