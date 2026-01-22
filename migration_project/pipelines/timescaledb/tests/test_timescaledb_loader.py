"""
Unit Tests for TimescaleDB Loader
=================================
Tests for the parametrized TimescaleDB loader components.

Run with: pytest migration_project/pipelines/timescaledb/tests/test_timescaledb_loader.py -v
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

# Add source directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from timescaledb_loader import (
    TableConfig,
    WatermarkType,
    LoadStatus,
    LoadResult,
    TimescaleDBCredentials,
    WatermarkManager,
    TimescaleDBLoader
)


class TestTableConfig:
    """Tests for TableConfig dataclass."""

    def test_from_dict_basic(self):
        """Test TableConfig creation from dictionary with required fields."""
        data = {
            "source_schema": "public",
            "source_table": "Worker",
            "primary_key_columns": ["WorkerID"],
            "watermark_column": "WatermarkUTC",
            "watermark_type": "timestamp"
        }

        config = TableConfig.from_dict(data)

        assert config.source_schema == "public"
        assert config.source_table == "Worker"
        assert config.primary_key_columns == ["WorkerID"]
        assert config.watermark_column == "WatermarkUTC"
        assert config.watermark_type == WatermarkType.TIMESTAMP

    def test_from_dict_with_defaults(self):
        """Test TableConfig creation with default values."""
        data = {
            "source_schema": "public",
            "source_table": "ShiftType",
            "primary_key_columns": ["ShiftTypeID"],
        }

        defaults = {
            "watermark_column": "modified_at",
            "watermark_type": "timestamp",
            "fetch_size": 50000,
            "is_full_load": True
        }

        config = TableConfig.from_dict(data, defaults)

        assert config.watermark_column == "modified_at"
        assert config.fetch_size == 50000
        assert config.is_full_load == True

    def test_from_dict_overrides_defaults(self):
        """Test that explicit values override defaults."""
        data = {
            "source_schema": "public",
            "source_table": "FactObservations",
            "primary_key_columns": ["ObservationID"],
            "fetch_size": 100000  # Override default
        }

        defaults = {
            "fetch_size": 10000,
            "watermark_column": "created_at"
        }

        config = TableConfig.from_dict(data, defaults)

        assert config.fetch_size == 100000  # Should use explicit value

    def test_full_source_name_property(self):
        """Test full_source_name property."""
        config = TableConfig(
            source_schema="public",
            source_table="Worker",
            primary_key_columns=["WorkerID"],
            watermark_column="WatermarkUTC"
        )

        assert config.full_source_name == "public.Worker"

    def test_target_table_name_property(self):
        """Test target_table_name property generates correct name."""
        config = TableConfig(
            source_schema="public",
            source_table="FactWorkersHistory",
            primary_key_columns=["WorkerHistoryID"],
            watermark_column="WatermarkUTC"
        )

        assert config.target_table_name == "public_factworkershistory"

    def test_primary_key_str_property(self):
        """Test primary_key_str with multiple keys."""
        config = TableConfig(
            source_schema="public",
            source_table="SomeTable",
            primary_key_columns=["ID1", "ID2", "ID3"],
            watermark_column="modified_at"
        )

        assert config.primary_key_str == "ID1,ID2,ID3"

    def test_watermark_type_bigint(self):
        """Test TableConfig with bigint watermark type."""
        data = {
            "source_schema": "public",
            "source_table": "SomeTable",
            "primary_key_columns": ["ID"],
            "watermark_column": "RowVersion",
            "watermark_type": "bigint"
        }

        config = TableConfig.from_dict(data)

        assert config.watermark_type == WatermarkType.BIGINT


class TestWatermarkType:
    """Tests for WatermarkType enum."""

    def test_timestamp_value(self):
        assert WatermarkType.TIMESTAMP.value == "timestamp"

    def test_bigint_value(self):
        assert WatermarkType.BIGINT.value == "bigint"

    def test_date_value(self):
        assert WatermarkType.DATE.value == "date"

    def test_integer_value(self):
        assert WatermarkType.INTEGER.value == "integer"


class TestLoadStatus:
    """Tests for LoadStatus enum."""

    def test_success_value(self):
        assert LoadStatus.SUCCESS.value == "success"

    def test_failed_value(self):
        assert LoadStatus.FAILED.value == "failed"

    def test_running_value(self):
        assert LoadStatus.RUNNING.value == "running"

    def test_skipped_value(self):
        assert LoadStatus.SKIPPED.value == "skipped"


class TestLoadResult:
    """Tests for LoadResult dataclass."""

    def test_duration_seconds_calculation(self):
        """Test duration calculation from start and end times."""
        config = TableConfig(
            source_schema="public",
            source_table="Test",
            primary_key_columns=["ID"],
            watermark_column="modified_at"
        )

        start = datetime(2026, 1, 20, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 1, 20, 10, 1, 30, tzinfo=timezone.utc)  # 90 seconds later

        result = LoadResult(
            table_config=config,
            status=LoadStatus.SUCCESS,
            rows_loaded=1000,
            start_time=start,
            end_time=end
        )

        assert result.duration_seconds == 90.0

    def test_duration_seconds_none_when_missing_times(self):
        """Test duration is None when times are not set."""
        config = TableConfig(
            source_schema="public",
            source_table="Test",
            primary_key_columns=["ID"],
            watermark_column="modified_at"
        )

        result = LoadResult(
            table_config=config,
            status=LoadStatus.SUCCESS,
            rows_loaded=1000
        )

        assert result.duration_seconds is None


class TestTimescaleDBCredentials:
    """Tests for TimescaleDBCredentials class."""

    def test_jdbc_url_generation(self):
        """Test JDBC URL is generated correctly."""
        creds = TimescaleDBCredentials(
            host="timescale.example.com",
            port=5432,
            database="wakecap",
            user="testuser",
            password="testpass",
            sslmode="require"
        )

        expected = "jdbc:postgresql://timescale.example.com:5432/wakecap?sslmode=require"
        assert creds.jdbc_url == expected

    def test_connection_properties(self):
        """Test connection properties dictionary."""
        creds = TimescaleDBCredentials(
            host="timescale.example.com",
            port=5432,
            database="wakecap",
            user="testuser",
            password="testpass"
        )

        props = creds.connection_properties

        assert props["user"] == "testuser"
        assert props["password"] == "testpass"
        assert props["driver"] == "org.postgresql.Driver"

    def test_from_yaml(self, tmp_path):
        """Test loading credentials from YAML file."""
        yaml_content = """
timescaledb:
  host: ts-host.example.com
  port: 5432
  database: mydb
  user: myuser
  password: mypassword
  sslmode: require
"""
        yaml_file = tmp_path / "credentials.yml"
        yaml_file.write_text(yaml_content)

        creds = TimescaleDBCredentials.from_yaml(str(yaml_file))

        assert creds.host == "ts-host.example.com"
        assert creds.port == 5432
        assert creds.database == "mydb"
        assert creds.user == "myuser"
        assert creds.password == "mypassword"

    def test_from_yaml_custom_key(self, tmp_path):
        """Test loading credentials from YAML with custom key."""
        yaml_content = """
custom_db:
  host: custom-host.example.com
  port: 5433
  database: customdb
  user: customuser
  password: custompass
"""
        yaml_file = tmp_path / "credentials.yml"
        yaml_file.write_text(yaml_content)

        creds = TimescaleDBCredentials.from_yaml(str(yaml_file), key="custom_db")

        assert creds.host == "custom-host.example.com"
        assert creds.port == 5433


class TestTableRegistry:
    """Tests for loading table registry from YAML."""

    def test_load_registry(self, tmp_path):
        """Test loading table registry from YAML file."""
        yaml_content = """
registry_version: "1.0"
source_system: "timescaledb"
target_catalog: "wakecap_prod"
target_schema: "raw_timescaledb"

defaults:
  watermark_column: "created_at"
  watermark_type: "timestamp"
  fetch_size: 10000

tables:
  - source_schema: "public"
    source_table: "Worker"
    primary_key_columns:
      - "WorkerID"
    watermark_column: "WatermarkUTC"
    category: "dimensions"

  - source_schema: "public"
    source_table: "FactObservations"
    primary_key_columns:
      - "ObservationID"
    watermark_column: "WatermarkUTC"
    category: "facts"
    fetch_size: 50000
"""
        yaml_file = tmp_path / "tables.yml"
        yaml_file.write_text(yaml_content)

        configs = TimescaleDBLoader.load_registry(str(yaml_file))

        assert len(configs) == 2
        assert configs[0].source_table == "Worker"
        assert configs[0].category == "dimensions"
        assert configs[1].source_table == "FactObservations"
        assert configs[1].fetch_size == 50000


class TestIncrementalQuery:
    """Tests for incremental query building."""

    @pytest.fixture
    def mock_loader(self):
        """Create a mock loader for testing."""
        mock_spark = MagicMock()
        mock_creds = TimescaleDBCredentials(
            host="test",
            port=5432,
            database="test",
            user="test",
            password="test"
        )

        loader = TimescaleDBLoader.__new__(TimescaleDBLoader)
        loader.spark = mock_spark
        loader.credentials = mock_creds
        loader.target_catalog = "test_catalog"
        loader.target_schema = "test_schema"
        loader.pipeline_id = None
        loader.pipeline_run_id = None

        return loader

    def test_build_full_load_query(self, mock_loader):
        """Test query building for full load (no watermark)."""
        config = TableConfig(
            source_schema="public",
            source_table="Worker",
            primary_key_columns=["WorkerID"],
            watermark_column="WatermarkUTC",
            watermark_type=WatermarkType.TIMESTAMP,
            is_full_load=True
        )

        query = mock_loader._build_incremental_query(config, last_watermark=None)

        assert 'SELECT * FROM "public"."Worker"' in query
        assert "WHERE" not in query or "WatermarkUTC" not in query.split("WHERE")[1] if "WHERE" in query else True

    def test_build_incremental_query_timestamp(self, mock_loader):
        """Test query building for incremental load with timestamp watermark."""
        config = TableConfig(
            source_schema="public",
            source_table="Worker",
            primary_key_columns=["WorkerID"],
            watermark_column="WatermarkUTC",
            watermark_type=WatermarkType.TIMESTAMP
        )

        last_watermark = datetime(2026, 1, 20, 10, 0, 0)
        query = mock_loader._build_incremental_query(config, last_watermark)

        assert "WatermarkUTC" in query
        assert ">" in query
        assert "2026-01-20" in query


class TestCategoryFiltering:
    """Tests for category-based table filtering."""

    def test_filter_by_category(self):
        """Test filtering tables by category."""
        configs = [
            TableConfig(
                source_schema="public",
                source_table="Worker",
                primary_key_columns=["WorkerID"],
                watermark_column="WatermarkUTC",
                category="dimensions"
            ),
            TableConfig(
                source_schema="public",
                source_table="FactObservations",
                primary_key_columns=["ObservationID"],
                watermark_column="WatermarkUTC",
                category="facts"
            ),
            TableConfig(
                source_schema="public",
                source_table="CrewAssignments",
                primary_key_columns=["CrewAssignmentID"],
                watermark_column="WatermarkUTC",
                category="assignments"
            )
        ]

        # Filter dimensions
        dimensions = [c for c in configs if c.category == "dimensions"]
        assert len(dimensions) == 1
        assert dimensions[0].source_table == "Worker"

        # Filter facts
        facts = [c for c in configs if c.category == "facts"]
        assert len(facts) == 1
        assert facts[0].source_table == "FactObservations"


class TestGeometryHandling:
    """Tests for geometry column handling."""

    def test_custom_column_mapping(self):
        """Test custom column mapping for geometry columns."""
        config = TableConfig(
            source_schema="public",
            source_table="Floor",
            primary_key_columns=["FloorID"],
            watermark_column="WatermarkUTC",
            custom_column_mapping={
                "Geometry": "ST_AsText(Geometry)"
            }
        )

        assert config.custom_column_mapping is not None
        assert "Geometry" in config.custom_column_mapping
        assert config.custom_column_mapping["Geometry"] == "ST_AsText(Geometry)"


class TestHypertableConfig:
    """Tests for TimescaleDB hypertable configuration."""

    def test_hypertable_config(self):
        """Test hypertable configuration fields."""
        config = TableConfig(
            source_schema="public",
            source_table="FactObservations",
            primary_key_columns=["ObservationID"],
            watermark_column="WatermarkUTC",
            partition_column="ObservationTime",
            is_hypertable=True,
            chunk_time_interval="1 day",
            fetch_size=50000
        )

        assert config.is_hypertable == True
        assert config.chunk_time_interval == "1 day"
        assert config.partition_column == "ObservationTime"
        assert config.fetch_size == 50000


# Integration test placeholder (requires Spark session)
class TestIntegration:
    """Integration tests (require Spark session - run in Databricks)."""

    @pytest.mark.skip(reason="Requires Spark session - run in Databricks")
    def test_full_load_cycle(self):
        """Test full load cycle for a single table."""
        pass

    @pytest.mark.skip(reason="Requires Spark session - run in Databricks")
    def test_incremental_load_cycle(self):
        """Test incremental load after initial load."""
        pass

    @pytest.mark.skip(reason="Requires Spark session - run in Databricks")
    def test_watermark_state_persistence(self):
        """Test watermark state is persisted correctly."""
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
