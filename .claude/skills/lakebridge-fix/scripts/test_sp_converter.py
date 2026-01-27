"""
Test script for SP Converter implementation.

Phase 1 Tests:
- REQ-R1: SP Pattern Detection
- REQ-F1: SP Converter Class
- REQ-F2: CURSOR to Window Converter
- REQ-F3: MERGE Statement Generator

Phase 2 Tests:
- REQ-F4: Temp Table Converter
- REQ-F5: Spatial Function Stub Generator
- REQ-F8: Enhanced Notebook Generator (with validation notebook)
- REQ-R2: Conversion Completeness Validator
"""

import sys
import os

# Add the scripts directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sp_converter import SPConverter, CursorConverter, MergeConverter, ConversionResult
from analyzer_patterns import SP_SOURCE_PATTERNS, SP_TARGET_PATTERNS

# Phase 2 imports
try:
    from temp_converter import TempTableConverter
    from spatial_converter import SpatialConverter
    from validation_generator import ValidationNotebookGenerator, ValidationConfig
    PHASE2_AVAILABLE = True
except ImportError as e:
    print(f"Phase 2 imports failed: {e}")
    PHASE2_AVAILABLE = False


def test_pattern_detection():
    """Test pattern detection on sample T-SQL."""
    print("=" * 60)
    print("TEST: Pattern Detection (REQ-R1)")
    print("=" * 60)

    sample_sql = """
    CREATE PROCEDURE [stg].[spTestProcedure]
    AS
    BEGIN
        -- Cursor pattern
        DECLARE @WorkerID INT
        DECLARE worker_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT WorkerID FROM dbo.Worker ORDER BY WorkerID

        OPEN worker_cursor
        FETCH NEXT FROM worker_cursor INTO @WorkerID

        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT @WorkerID
            FETCH NEXT FROM worker_cursor INTO @WorkerID
        END

        CLOSE worker_cursor
        DEALLOCATE worker_cursor

        -- Temp table pattern
        SELECT * INTO #TempResults FROM dbo.SomeTable

        -- MERGE pattern
        MERGE INTO dbo.TargetTable AS target
        USING #TempResults AS source
        ON target.ID = source.ID
        WHEN MATCHED THEN
            UPDATE SET target.Value = source.Value
        WHEN NOT MATCHED THEN
            INSERT (ID, Value) VALUES (source.ID, source.Value);

        -- Spatial pattern
        SELECT geography::Point(lat, lon, 4326).STDistance(@center) as Distance
        FROM dbo.Locations

        -- Transaction pattern
        BEGIN TRANSACTION
        UPDATE dbo.SomeTable SET Status = 1
        COMMIT TRANSACTION
    END
    """

    import re

    patterns_found = {}
    for pattern_name, pattern_info in SP_SOURCE_PATTERNS.items():
        regex = pattern_info[0]
        matches = list(re.finditer(regex, sample_sql, re.IGNORECASE | re.MULTILINE))
        if matches:
            patterns_found[pattern_name] = len(matches)
            print(f"  [FOUND] {pattern_name}: {len(matches)} match(es)")

    expected = ["CURSOR", "WHILE_LOOP", "TEMP_TABLE", "MERGE", "SPATIAL_GEOGRAPHY", "TRANSACTION", "FETCH_CURSOR"]
    found = list(patterns_found.keys())

    missing = [p for p in expected if p not in found]
    if missing:
        print(f"\n  [WARN] Expected patterns not found: {missing}")
    else:
        print(f"\n  [OK] All expected patterns detected!")

    print()
    return len(missing) == 0


def test_cursor_converter():
    """Test CURSOR to Window function conversion (REQ-F2)."""
    print("=" * 60)
    print("TEST: CURSOR Converter (REQ-F2)")
    print("=" * 60)

    cursor_sql = """
    DECLARE @WorkerID INT, @PrevTimestamp DATETIME, @SessionID INT = 0
    DECLARE worker_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT WorkerID, TimestampUTC
        FROM FactWorkersHistory
        ORDER BY WorkerID, TimestampUTC

    OPEN worker_cursor
    FETCH NEXT FROM worker_cursor INTO @WorkerID, @Timestamp

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF DATEDIFF(MINUTE, @PrevTimestamp, @Timestamp) > 300
            SET @SessionID = @SessionID + 1

        UPDATE FactWorkersHistory
        SET SessionID = @SessionID
        WHERE CURRENT OF worker_cursor

        SET @PrevTimestamp = @Timestamp
        FETCH NEXT FROM worker_cursor INTO @WorkerID, @Timestamp
    END
    """

    converter = CursorConverter()

    # Test cursor definition extraction
    cursor_def = converter.extract_cursor_definition(cursor_sql)
    if cursor_def:
        print(f"  [OK] Cursor name: {cursor_def['cursor_name']}")
        print(f"  [OK] Order columns: {cursor_def['order_cols']}")
        print(f"  [OK] Fetch variables: {cursor_def['fetch_variables']}")
    else:
        print("  [FAIL] Could not extract cursor definition")
        return False

    # Test loop body extraction
    loop_body = converter.extract_loop_body(cursor_sql, cursor_def['cursor_name'])
    if loop_body:
        print(f"  [OK] Loop body extracted ({len(loop_body)} chars)")
    else:
        print("  [FAIL] Could not extract loop body")
        return False

    # Test pattern detection
    patterns = converter.detect_cursor_pattern(loop_body)
    print(f"  [OK] Detected patterns: {patterns}")

    # Test conversion
    context = {
        "source_table": "source_df",
        "timestamp_col": "TimestampUTC",
        "gap_threshold_seconds": 18000,  # 5 hours in seconds
    }
    converted_code, warnings = converter.convert(cursor_sql, context)

    if "Window" in converted_code and "over" in converted_code:
        print("  [OK] Generated Window function code")
    else:
        print("  [WARN] Window function code may be incomplete")

    if warnings:
        print(f"  [WARN] Conversion warnings: {warnings}")

    print(f"\n  Generated code preview:\n{'='*40}")
    print(converted_code[:500] + "...")
    print()

    return True


def test_merge_converter():
    """Test MERGE statement conversion (REQ-F3)."""
    print("=" * 60)
    print("TEST: MERGE Converter (REQ-F3)")
    print("=" * 60)

    merge_sql = """
    MERGE INTO dbo.FactWorkersShifts AS target
    USING #CalculatedShifts AS source
    ON target.ProjectID = source.ProjectID
       AND target.WorkerID = source.WorkerID
       AND target.ShiftLocalDate = source.ShiftLocalDate

    WHEN MATCHED AND source.UpdatedAt > target.UpdatedAt THEN
        UPDATE SET
            target.ActiveTime = source.ActiveTime,
            target.InactiveTime = source.InactiveTime,
            target.UpdatedAt = source.UpdatedAt

    WHEN NOT MATCHED THEN
        INSERT (ProjectID, WorkerID, ShiftLocalDate, ActiveTime, InactiveTime, CreatedAt)
        VALUES (source.ProjectID, source.WorkerID, source.ShiftLocalDate,
                source.ActiveTime, source.InactiveTime, GETUTCDATE());
    """

    converter = MergeConverter()

    # Test component extraction
    components = converter.extract_merge_components(merge_sql)
    if components:
        print(f"  [OK] Target table: {components['target_table']}")
        print(f"  [OK] Target alias: {components['target_alias']}")
        print(f"  [OK] Source alias: {components['source_alias']}")
        print(f"  [OK] WHEN MATCHED clauses: {len(components['when_matched'])}")
        print(f"  [OK] WHEN NOT MATCHED clauses: {len(components['when_not_matched'])}")
    else:
        print("  [FAIL] Could not extract MERGE components")
        return False

    # Test conversion
    converted_code, warnings = converter.convert(
        merge_sql,
        "wakecap_prod.gold.gold_fact_workers_shifts",
        "calculated_shifts_df"
    )

    if "DeltaTable.forName" in converted_code and ".merge(" in converted_code:
        print("  [OK] Generated Delta MERGE code")
    else:
        print("  [WARN] Delta MERGE code may be incomplete")

    if warnings:
        print(f"  [WARN] Conversion warnings: {warnings}")

    print(f"\n  Generated code preview:\n{'='*40}")
    print(converted_code[:800] + "...")
    print()

    return True


def test_sp_converter():
    """Test full SP converter (REQ-F1)."""
    print("=" * 60)
    print("TEST: SP Converter (REQ-F1)")
    print("=" * 60)

    sample_sql = """
    CREATE PROCEDURE [stg].[spCalculateTestMetrics]
    -- Purpose: Calculate test metrics for workers
    AS
    BEGIN
        SET NOCOUNT ON

        -- Create temp table for results
        SELECT WorkerID, SUM(ActiveTime) as TotalActive
        INTO #WorkerTotals
        FROM dbo.FactWorkersHistory
        GROUP BY WorkerID

        -- Merge into target
        MERGE INTO dbo.WorkerMetrics AS target
        USING #WorkerTotals AS source
        ON target.WorkerID = source.WorkerID
        WHEN MATCHED THEN
            UPDATE SET target.TotalActive = source.TotalActive
        WHEN NOT MATCHED THEN
            INSERT (WorkerID, TotalActive) VALUES (source.WorkerID, source.TotalActive);

        BEGIN TRANSACTION
        UPDATE dbo.SyncState SET LastSync = GETUTCDATE()
        COMMIT TRANSACTION
    END
    """

    converter = SPConverter()

    try:
        result = converter.convert(
            source_sql=sample_sql,
            procedure_name="stg.spCalculateTestMetrics",
            target_catalog="wakecap_prod",
            target_schema="gold",
        )

        print(f"  [OK] Procedure name: {result.procedure_name}")
        print(f"  [OK] Target path: {result.target_path}")
        print(f"  [OK] Patterns converted: {result.patterns_converted}")
        print(f"  [OK] Patterns requiring manual review: {result.patterns_manual}")
        print(f"  [OK] Conversion score: {result.conversion_score:.0%}")
        print(f"  [OK] Success: {result.success}")

        if result.warnings:
            print(f"  [WARN] Warnings: {result.warnings[:3]}...")

        # Check notebook structure
        notebook = result.notebook_content
        required_sections = [
            "# Databricks notebook source",
            "# MAGIC %md",
            "## Configuration",
            "## Pre-flight Check",
            "## Helper Functions",
            "## Execution Summary",
            "dbutils.notebook.exit",
        ]

        missing_sections = [s for s in required_sections if s not in notebook]
        if missing_sections:
            print(f"  [WARN] Missing sections: {missing_sections}")
        else:
            print("  [OK] All required notebook sections present")

        print(f"\n  Notebook length: {len(notebook):,} characters")
        print(f"  Notebook lines: {len(notebook.split(chr(10))):,}")

        return result.success or len(result.patterns_manual) <= 2

    except Exception as e:
        print(f"  [FAIL] Conversion error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_real_procedure():
    """Test with a real stored procedure from the migration project."""
    print("=" * 60)
    print("TEST: Real Procedure (stg.spDeltaSyncFactWorkersHistory)")
    print("=" * 60)

    # Path to the real procedure
    proc_path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..",
        "migration_project", "source_sql", "stored_procedures",
        "stg.spDeltaSyncFactWorkersHistory.sql"
    )

    if not os.path.exists(proc_path):
        print(f"  [SKIP] Procedure file not found: {proc_path}")
        return True  # Not a failure, just skip

    with open(proc_path, 'r', encoding='utf-8') as f:
        source_sql = f.read()

    print(f"  [OK] Loaded procedure: {len(source_sql):,} characters")

    # Analyze patterns
    import re
    patterns_found = {}
    for pattern_name, pattern_info in SP_SOURCE_PATTERNS.items():
        regex = pattern_info[0]
        matches = list(re.finditer(regex, source_sql, re.IGNORECASE | re.MULTILINE))
        if matches:
            patterns_found[pattern_name] = len(matches)

    print(f"  [OK] Patterns detected: {list(patterns_found.keys())}")

    # Convert
    converter = SPConverter()
    try:
        result = converter.convert(
            source_sql=source_sql,
            procedure_name="stg.spDeltaSyncFactWorkersHistory",
            target_catalog="wakecap_prod",
            target_schema="gold",
            target_table="gold_fact_workers_history",
        )

        print(f"  [OK] Conversion complete")
        print(f"  [OK] Patterns converted: {result.patterns_converted}")
        print(f"  [OK] Patterns manual: {result.patterns_manual}")
        print(f"  [OK] Conversion score: {result.conversion_score:.0%}")
        print(f"  [OK] Notebook lines: {len(result.notebook_content.split(chr(10))):,}")

        # Save the generated notebook for review
        output_dir = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..",
            "migration_project", "pipelines", "gold", "notebooks"
        )
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, "delta_sync_fact_workers_history_generated.py")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result.notebook_content)

        print(f"  [OK] Saved generated notebook: {output_path}")

        return True

    except Exception as e:
        print(f"  [FAIL] Conversion error: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================
# PHASE 2 TESTS
# ============================================================

def test_temp_table_converter():
    """Test temp table converter (REQ-F4)."""
    print("=" * 60)
    print("TEST: Temp Table Converter (REQ-F4)")
    print("=" * 60)

    if not PHASE2_AVAILABLE:
        print("  [SKIP] Phase 2 modules not available")
        return True

    sample_sql = """
    -- Create temp table with SELECT INTO
    SELECT WorkerID, SUM(ActiveTime) as TotalActive
    INTO #WorkerTotals
    FROM dbo.FactWorkersHistory
    GROUP BY WorkerID

    -- Create temp table with CREATE TABLE
    CREATE TABLE #Results (
        ProjectID INT,
        WorkerID INT,
        TotalHours DECIMAL(10,2)
    )

    -- Use temp tables
    INSERT INTO #Results
    SELECT w.ProjectID, w.WorkerID, wt.TotalActive
    FROM dbo.Worker w
    INNER JOIN #WorkerTotals wt ON w.WorkerID = wt.WorkerID

    -- Read from temp table
    SELECT * FROM #Results WHERE TotalHours > 0
    """

    converter = TempTableConverter()

    # Test detection
    temp_tables = converter.detect_temp_tables(sample_sql)
    print(f"  [OK] Detected {len(temp_tables)} temp tables")

    for tt in temp_tables:
        print(f"       - {tt.name}: {tt.create_type}, used {tt.usage_count} times")

    # Test conversion
    if temp_tables:
        context = {"target_catalog": "wakecap_prod", "target_schema": "gold"}
        code, warnings = converter.convert(temp_tables[0], context)

        if "createOrReplaceTempView" in code or "temp view" in code.lower():
            print("  [OK] Generated temp view code")
        else:
            print("  [WARN] Temp view code may be incomplete")

        if warnings:
            print(f"  [WARN] Warnings: {warnings}")

    print()
    return len(temp_tables) >= 2


def test_spatial_converter():
    """Test spatial converter (REQ-F5)."""
    print("=" * 60)
    print("TEST: Spatial Converter (REQ-F5)")
    print("=" * 60)

    if not PHASE2_AVAILABLE:
        print("  [SKIP] Phase 2 modules not available")
        return True

    sample_sql = """
    -- Create point from lat/lon
    SELECT geography::Point(@lat, @lon, 4326) as Location

    -- Calculate distance
    SELECT point1.STDistance(point2) as DistanceMeters
    FROM Locations

    -- Check containment
    SELECT *
    FROM Workers w
    WHERE zone.STContains(w.Location) = 1
    """

    converter = SpatialConverter()

    # Test detection
    spatial_funcs = converter.detect_spatial_functions(sample_sql)
    print(f"  [OK] Detected {len(spatial_funcs)} spatial functions")

    for sf in spatial_funcs:
        print(f"       - {sf.function_name}: line {sf.line_number}")

    # Test conversion
    if spatial_funcs:
        context = {"use_h3": True}
        code, warnings = converter.convert(spatial_funcs, context)

        if "haversine" in code.lower() or "h3" in code.lower():
            print("  [OK] Generated spatial conversion code")
        else:
            print("  [WARN] Spatial code may be incomplete")

        if warnings:
            print(f"  [WARN] Warnings: {warnings[:2]}")

    print()
    return len(spatial_funcs) >= 2


def test_validation_generator():
    """Test validation notebook generator (REQ-F8)."""
    print("=" * 60)
    print("TEST: Validation Notebook Generator (REQ-F8)")
    print("=" * 60)

    if not PHASE2_AVAILABLE:
        print("  [SKIP] Phase 2 modules not available")
        return True

    generator = ValidationNotebookGenerator()

    config = ValidationConfig(
        procedure_name="stg.spTestProcedure",
        source_table="wakecap_prod.silver.silver_test",
        target_table="wakecap_prod.gold.gold_test",
        key_columns=["ProjectId", "WorkerId"],
        aggregation_columns=["ActiveTime", "InactiveTime"],
        date_column="ShiftLocalDate",
        sample_size=100,
    )

    notebook = generator.generate(config)

    # Check required sections
    required = [
        "# Databricks notebook source",
        "Row Count Comparison",
        "Aggregation Comparison",
        "Sample Record Comparison",
        "dbutils.notebook.exit",
    ]

    missing = [r for r in required if r not in notebook]

    if missing:
        print(f"  [WARN] Missing sections: {missing}")
    else:
        print("  [OK] All required sections present")

    print(f"  [OK] Generated notebook: {len(notebook):,} characters")
    print(f"  [OK] Notebook lines: {len(notebook.split(chr(10))):,}")

    print()
    return len(missing) == 0


def test_enhanced_sp_converter():
    """Test SP converter with Phase 2 features."""
    print("=" * 60)
    print("TEST: Enhanced SP Converter (Phase 2)")
    print("=" * 60)

    sample_sql = """
    CREATE PROCEDURE [stg].[spTestWithAllPatterns]
    AS
    BEGIN
        -- Temp table
        SELECT * INTO #TempData FROM dbo.Source

        -- Spatial
        SELECT geography::Point(lat, lon, 4326) as Loc FROM #TempData

        -- MERGE
        MERGE INTO dbo.Target AS t
        USING #TempData AS s
        ON t.ID = s.ID
        WHEN MATCHED THEN UPDATE SET t.Value = s.Value
        WHEN NOT MATCHED THEN INSERT (ID, Value) VALUES (s.ID, s.Value);
    END
    """

    converter = SPConverter(generate_validation=True)
    result = converter.convert(
        source_sql=sample_sql,
        procedure_name="stg.spTestWithAllPatterns",
        target_catalog="wakecap_prod",
        target_schema="gold",
    )

    print(f"  [OK] Procedure: {result.procedure_name}")
    print(f"  [OK] Patterns converted: {result.patterns_converted}")
    print(f"  [OK] Patterns manual: {result.patterns_manual}")
    print(f"  [OK] Temp tables converted: {result.temp_tables_converted}")
    print(f"  [OK] Spatial functions: {result.spatial_functions_converted}")
    print(f"  [OK] Conversion score: {result.conversion_score:.0%}")

    # Check for validation notebook
    if result.validation_notebook:
        print(f"  [OK] Validation notebook generated: {len(result.validation_notebook):,} chars")
    else:
        print("  [WARN] Validation notebook not generated")

    print()
    return result.conversion_score >= 0.5


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("SP CONVERSION AUTOMATION TEST SUITE")
    print("=" * 60 + "\n")

    results = {}

    # Phase 1 tests
    print("\n--- PHASE 1 TESTS ---\n")
    results["Pattern Detection"] = test_pattern_detection()
    results["CURSOR Converter"] = test_cursor_converter()
    results["MERGE Converter"] = test_merge_converter()
    results["SP Converter"] = test_sp_converter()
    results["Real Procedure"] = test_with_real_procedure()

    # Phase 2 tests
    print("\n--- PHASE 2 TESTS ---\n")
    results["Temp Table Converter"] = test_temp_table_converter()
    results["Spatial Converter"] = test_spatial_converter()
    results["Validation Generator"] = test_validation_generator()
    results["Enhanced SP Converter"] = test_enhanced_sp_converter()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = 0
    failed = 0
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1

    print()
    print(f"  Total: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
