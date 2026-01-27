"""
Tests for Phase 3 Fix Skill Components.

Tests for:
- REQ-F6: Dependency Resolver
- REQ-F7: Enhanced Validation Generator
"""


def test_dependency_resolver_init():
    """Test DependencyResolver initialization."""
    from dependency_resolver import DependencyResolver

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        default_schema="silver",
        dry_run=True
    )

    assert resolver.catalog == "wakecap_prod"
    assert resolver.default_schema == "silver"
    assert resolver.dry_run is True

    print("PASS: test_dependency_resolver_init")


def test_dependency_resolver_mapping():
    """Test table name mapping."""
    from dependency_resolver import DependencyResolver

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        table_mapping={
            "dbo.Worker": "wakecap_prod.silver.silver_worker_custom",
        }
    )

    # Test explicit mapping
    mapped = resolver._map_to_databricks("dbo.Worker")
    assert mapped == "wakecap_prod.silver.silver_worker_custom", f"Expected custom mapping, got {mapped}"

    # Test default mapping
    mapped = resolver._map_to_databricks("dbo.Project")
    assert "silver" in mapped, f"Expected silver schema, got {mapped}"
    assert "project" in mapped.lower(), f"Expected project in name, got {mapped}"

    # Test fact table mapping
    mapped = resolver._map_to_databricks("dbo.FactWorkersShifts")
    assert "gold" in mapped, f"Expected gold schema for fact table, got {mapped}"

    print("PASS: test_dependency_resolver_mapping")


def test_dependency_resolver_resolve_all():
    """Test resolving all dependencies."""
    from dependency_resolver import DependencyResolver, ActionStatus

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        dry_run=True
    )

    # Set existing tables
    resolver.set_existing_tables({
        "wakecap_prod.silver.silver_worker",
        "wakecap_prod.silver.silver_project",
    })

    dependencies = [
        {"target_object": "dbo.Worker", "dependency_type": "reads"},
        {"target_object": "dbo.Project", "dependency_type": "reads"},
        {"target_object": "dbo.Zone", "dependency_type": "reads"},
        {"target_object": "gold.FactOutput", "dependency_type": "writes"},
        {"target_object": "dbo.fnCalcTime", "dependency_type": "uses_function"},
    ]

    actions = resolver.resolve_all(dependencies, auto_create=False)

    assert len(actions) == len(dependencies)

    # Check statuses
    statuses = [a.action for a in actions]
    assert ActionStatus.EXISTS in statuses, "Expected at least one EXISTS status"
    assert ActionStatus.MANUAL_REQUIRED in statuses, "Expected at least one MANUAL_REQUIRED status"

    print(f"PASS: test_dependency_resolver_resolve_all ({len(actions)} actions)")


def test_dependency_resolver_create_placeholder():
    """Test placeholder table creation."""
    from dependency_resolver import DependencyResolver, ActionStatus

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        dry_run=True  # Don't actually create
    )

    action = resolver.create_placeholder_table(
        source_table="dbo.Worker",
        target_table="wakecap_prod.silver.silver_worker"
    )

    assert action.action == ActionStatus.SKIPPED  # Dry run skips
    assert action.sql_executed is not None
    assert "CREATE TABLE" in action.sql_executed
    assert "wakecap_prod.silver.silver_worker" in action.sql_executed
    assert "USING DELTA" in action.sql_executed

    print("PASS: test_dependency_resolver_create_placeholder")


def test_dependency_resolver_udf_stub():
    """Test UDF stub generation."""
    from dependency_resolver import DependencyResolver

    resolver = DependencyResolver(catalog="wakecap_prod")

    action = resolver.create_udf_stub(
        source_func="dbo.fnCalcDuration",
        udf_name="wakecap_prod.default.calc_duration"
    )

    assert action.sql_executed is not None
    assert "def calc" in action.sql_executed.lower()
    assert "@udf" in action.sql_executed

    print("PASS: test_dependency_resolver_udf_stub")


def test_dependency_resolver_report():
    """Test resolution report generation."""
    from dependency_resolver import DependencyResolver

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        dry_run=True
    )

    resolver.set_existing_tables({
        "wakecap_prod.silver.silver_worker",
    })

    dependencies = [
        {"target_object": "dbo.Worker", "dependency_type": "reads"},
        {"target_object": "dbo.Project", "dependency_type": "reads"},
    ]

    actions = resolver.resolve_all(dependencies, auto_create=False)
    report = resolver.generate_resolution_report(actions)

    assert "## Dependency Resolution Report" in report
    assert "Total Dependencies" in report
    assert "Table Dependencies" in report

    print("PASS: test_dependency_resolver_report")


def test_dependency_resolver_setup_notebook():
    """Test setup notebook generation."""
    from dependency_resolver import DependencyResolver, ActionStatus

    resolver = DependencyResolver(
        catalog="wakecap_prod",
        dry_run=True
    )

    dependencies = [
        {"target_object": "dbo.Worker", "dependency_type": "reads"},
        {"target_object": "dbo.fnCalcTime", "dependency_type": "uses_function"},
    ]

    actions = resolver.resolve_all(dependencies, auto_create=True)
    notebook = resolver.generate_setup_notebook(actions)

    assert "# Databricks notebook source" in notebook
    assert "Dependency Setup" in notebook
    assert "wakecap_prod" in notebook

    print("PASS: test_dependency_resolver_setup_notebook")


def test_type_conversion():
    """Test T-SQL to Spark type conversion."""
    from dependency_resolver import DependencyResolver

    resolver = DependencyResolver(catalog="wakecap_prod")

    # Test various type conversions
    test_cases = [
        ("NVARCHAR(100)", "STRING"),
        ("INT", "INT"),
        ("BIGINT", "BIGINT"),
        ("BIT", "BOOLEAN"),
        ("DECIMAL(18,4)", "DECIMAL(18,4)"),
        ("DATETIME", "TIMESTAMP"),
        ("FLOAT", "DOUBLE"),
        ("UNIQUEIDENTIFIER", "STRING"),
    ]

    for tsql_type, expected_spark in test_cases:
        result = resolver.convert_tsql_type(tsql_type)
        assert result == expected_spark, f"Expected {expected_spark} for {tsql_type}, got {result}"

    print("PASS: test_type_conversion")


def test_validation_generator_enhanced_config():
    """Test enhanced ValidationConfig with Phase 3 options."""
    from validation_generator import ValidationConfig

    config = ValidationConfig(
        procedure_name="test_proc",
        source_table="silver.source",
        target_table="gold.target",
        key_columns=["id"],
        aggregation_columns=["value"],
        date_column="created_at",
        categorical_columns=["status", "type"],
        foreign_key_checks=[
            {"column": "project_id", "ref_table": "silver.dim_project", "ref_column": "project_id"}
        ],
        custom_rules=[
            {"name": "No negative values", "sql": "SELECT COUNT(*) FROM {TARGET_TABLE} WHERE value < 0", "expected": 0}
        ],
        include_profiling=True,
        include_freshness=True,
        expected_freshness_hours=24,
    )

    assert config.categorical_columns == ["status", "type"]
    assert len(config.foreign_key_checks) == 1
    assert len(config.custom_rules) == 1
    assert config.include_profiling is True
    assert config.expected_freshness_hours == 24

    print("PASS: test_validation_generator_enhanced_config")


def test_validation_generator_enhanced_sections():
    """Test that enhanced notebook includes all Phase 3 sections."""
    from validation_generator import ValidationNotebookGenerator, ValidationConfig

    generator = ValidationNotebookGenerator()

    config = ValidationConfig(
        procedure_name="test_proc",
        source_table="silver.source",
        target_table="gold.target",
        key_columns=["id"],
        aggregation_columns=["value"],
        date_column="created_at",
        categorical_columns=["status"],
        foreign_key_checks=[
            {"column": "project_id", "ref_table": "silver.dim_project", "ref_column": "project_id"}
        ],
        custom_rules=[
            {"name": "Test Rule", "sql": "SELECT 0", "expected": 0}
        ],
        include_profiling=True,
        include_freshness=True,
    )

    notebook = generator.generate(config)

    # Check all sections present
    assert "Data Freshness Check" in notebook, "Missing freshness section"
    assert "Data Profiling" in notebook, "Missing profiling section"
    assert "Value Distribution" in notebook, "Missing distribution section"
    assert "Foreign Key Validation" in notebook, "Missing FK section"
    assert "Custom Validation Rules" in notebook, "Missing custom rules section"

    print("PASS: test_validation_generator_enhanced_sections")


def test_validation_generator_comprehensive():
    """Test comprehensive validation notebook generation."""
    from validation_generator import ValidationNotebookGenerator

    generator = ValidationNotebookGenerator()

    source_sql = """
    SELECT ProjectId, WorkerId, SUM(ActiveTime) AS TotalTime
    FROM dbo.FactWorkersHistory
    GROUP BY ProjectId, WorkerId
    """

    notebook = generator.generate_comprehensive_validation(
        procedure_name="test_proc",
        source_sql=source_sql,
        target_notebook="# converted code",
        source_table="silver.fact_workers_history",
        target_table="gold.gold_fact_summary",
        dependencies=[
            {"target": "dbo.Worker", "type": "TABLE_READ", "resolved": True, "databricks_path": "silver.silver_worker"}
        ],
        business_logic_comparison={
            "comparisons": [
                {"type": "aggregation", "source": "SUM(ActiveTime)", "status": "match", "risk": "LOW"}
            ]
        }
    )

    assert "# Databricks notebook source" in notebook
    assert "test_proc" in notebook
    assert "gold.gold_fact_summary" in notebook

    print("PASS: test_validation_generator_comprehensive")


def test_validation_generator_column_inference():
    """Test automatic column inference from SQL."""
    from validation_generator import ValidationNotebookGenerator

    generator = ValidationNotebookGenerator()

    # Test key column inference
    key_cols = generator._infer_key_columns(["ProjectId", "WorkerId", "Name", "Value"])
    assert "ProjectId" in key_cols or any("id" in c.lower() for c in key_cols)

    # Test agg column inference
    agg_cols = generator._infer_agg_columns(["ProjectId", "ActiveTime", "Duration", "Name"])
    assert "ActiveTime" in agg_cols or "Duration" in agg_cols

    # Test date column inference
    date_col = generator._infer_date_column(["ProjectId", "CreatedAt", "Name", "LocalDate"])
    assert date_col in ["CreatedAt", "LocalDate"]

    print("PASS: test_validation_generator_column_inference")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Running Phase 3 Fix Skill Tests")
    print("=" * 60 + "\n")

    tests = [
        test_dependency_resolver_init,
        test_dependency_resolver_mapping,
        test_dependency_resolver_resolve_all,
        test_dependency_resolver_create_placeholder,
        test_dependency_resolver_udf_stub,
        test_dependency_resolver_report,
        test_dependency_resolver_setup_notebook,
        test_type_conversion,
        test_validation_generator_enhanced_config,
        test_validation_generator_enhanced_sections,
        test_validation_generator_comprehensive,
        test_validation_generator_column_inference,
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
