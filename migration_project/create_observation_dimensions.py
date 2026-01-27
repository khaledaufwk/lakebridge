"""
Create observation dimension tables in Databricks Silver layer.

Loads dimension tables from SQL Server WakeCapDW:
- ObservationDiscriminator (6 rows)
- ObservationSource (6 rows)
- ObservationType (194 rows)
- ObservationSeverity (4 rows)
- ObservationStatus (3 rows)
- ObservationClinicViolationStatus (2 rows)
- Company -> Organization (dimension table)
"""

import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
import pyodbc
from databricks.sdk import WorkspaceClient


def load_credentials():
    """Load credentials from credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if creds_path.exists():
        with open(creds_path) as f:
            return yaml.safe_load(f)
    return {}


def get_sqlserver_connection(creds):
    """Create SQL Server connection."""
    mssql = creds.get("mssql", {})
    conn_str = f"""
        DRIVER={{{mssql['driver']}}};
        SERVER={mssql['server']};
        DATABASE={mssql['database']};
        UID={mssql['user']};
        PWD={mssql['password']};
        Encrypt=yes;
        TrustServerCertificate=no;
    """
    return pyodbc.connect(conn_str)


def main():
    print("=" * 70)
    print("Create Observation Dimension Tables in Databricks Silver Layer")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    # Load credentials
    creds = load_credentials()
    db_creds = creds.get("databricks", {})

    # Initialize clients
    w = WorkspaceClient(host=db_creds["host"], token=db_creds["token"])
    print(f"\nConnected to Databricks: {db_creds['host']}")

    conn = get_sqlserver_connection(creds)
    print(f"Connected to SQL Server: {creds['mssql']['server']}")

    # Configuration
    TARGET_CATALOG = "wakecap_prod"
    TARGET_SCHEMA = "silver"

    # Dimension tables to load
    dimensions = [
        {
            "source_table": "dbo.ObservationDiscriminator",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_discriminator",
            "query": """
                SELECT
                    ObservationDiscriminatorID,
                    ObservationDiscriminator,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationDiscriminator
            """,
        },
        {
            "source_table": "dbo.ObservationSource",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_source",
            "query": """
                SELECT
                    ObservationSourceID,
                    ObservationSource,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationSource
            """,
        },
        {
            "source_table": "dbo.ObservationType",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_type",
            "query": """
                SELECT
                    ObservationTypeID,
                    ObservationType,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationType
            """,
        },
        {
            "source_table": "dbo.ObservationSeverity",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_severity",
            "query": """
                SELECT
                    ObservationSeverityID,
                    ObservationSeverity,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationSeverity
            """,
        },
        {
            "source_table": "dbo.ObservationStatus",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_status",
            "query": """
                SELECT
                    ObservationStatusID,
                    ObservationStatus,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationStatus
            """,
        },
        {
            "source_table": "dbo.ObservationClinicViolationStatus",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_observation_clinic_violation_status",
            "query": """
                SELECT
                    ObservationClinicViolationStatusID,
                    ObservationClinicViolationStatus,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.ObservationClinicViolationStatus
            """,
        },
        {
            "source_table": "dbo.Company",
            "target_table": f"{TARGET_CATALOG}.{TARGET_SCHEMA}.silver_organization",
            "query": """
                SELECT
                    CompanyID,
                    ExtCompanyID,
                    CompanyName,
                    CompanyTypeID,
                    ProjectID,
                    IsActive,
                    ExtSourceID,
                    GETUTCDATE() as _loaded_at
                FROM dbo.Company
            """,
        },
    ]

    # Get SQL warehouse for statement execution
    warehouses = list(w.warehouses.list())
    warehouse_id = None
    for wh in warehouses:
        if wh.state and wh.state.value in ["RUNNING", "STARTING"]:
            warehouse_id = wh.id
            break
    if not warehouse_id and warehouses:
        warehouse_id = warehouses[0].id

    if not warehouse_id:
        print("\n[ERROR] No SQL warehouse available")
        sys.exit(1)

    print(f"Using SQL warehouse: {warehouse_id}")

    # Ensure silver schema exists
    print(f"\nEnsuring schema exists: {TARGET_CATALOG}.{TARGET_SCHEMA}")
    try:
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}",
            wait_timeout="30s"
        )
    except Exception as e:
        print(f"  Warning: {e}")

    # Process each dimension
    print("\n" + "-" * 70)
    print("Loading dimension tables...")
    print("-" * 70)

    cursor = conn.cursor()

    for dim in dimensions:
        source = dim["source_table"]
        target = dim["target_table"]
        query = dim["query"]

        print(f"\n{source} -> {target}")

        try:
            # Load from SQL Server
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            row_count = len(rows)
            print(f"  Loaded {row_count} rows from SQL Server")

            if row_count == 0:
                print(f"  [SKIP] No data to load")
                continue

            # Drop existing table
            drop_sql = f"DROP TABLE IF EXISTS {target}"
            try:
                w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=drop_sql,
                    wait_timeout="30s"
                )
            except Exception as e:
                print(f"  Warning dropping table: {e}")

            # Determine column types from first row
            col_defs = []
            for i, col in enumerate(columns):
                sample_val = rows[0][i] if rows else None
                if isinstance(sample_val, int):
                    col_defs.append(f"`{col}` INT")
                elif isinstance(sample_val, float):
                    col_defs.append(f"`{col}` DOUBLE")
                elif isinstance(sample_val, bool):
                    col_defs.append(f"`{col}` BOOLEAN")
                elif hasattr(sample_val, 'isoformat'):
                    col_defs.append(f"`{col}` TIMESTAMP")
                else:
                    col_defs.append(f"`{col}` STRING")

            create_sql = f"""
                CREATE TABLE {target} (
                    {', '.join(col_defs)}
                ) USING DELTA
            """

            try:
                w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=create_sql,
                    wait_timeout="30s"
                )
                print(f"  Created table: {target}")
            except Exception as e:
                print(f"  Error creating table: {e}")
                continue

            # Insert data in batches
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]

                values_list = []
                for row in batch:
                    vals = []
                    for val in row:
                        if val is None:
                            vals.append("NULL")
                        elif isinstance(val, str):
                            escaped = val.replace("'", "''")
                            vals.append(f"'{escaped}'")
                        elif isinstance(val, (int, float)):
                            vals.append(str(val))
                        elif isinstance(val, bool):
                            vals.append("TRUE" if val else "FALSE")
                        elif hasattr(val, 'isoformat'):
                            vals.append(f"'{val.isoformat()}'")
                        else:
                            escaped = str(val).replace("'", "''")
                            vals.append(f"'{escaped}'")
                    values_list.append(f"({', '.join(vals)})")

                insert_sql = f"""
                    INSERT INTO {target} ({', '.join([f'`{c}`' for c in columns])})
                    VALUES {', '.join(values_list)}
                """

                try:
                    w.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=insert_sql,
                        wait_timeout="60s"
                    )
                except Exception as e:
                    print(f"  Error inserting batch: {e}")

            print(f"  [OK] Inserted {row_count} rows")

        except Exception as e:
            print(f"  [ERROR] {e}")

    conn.close()

    # Verify results
    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)

    for dim in dimensions:
        target = dim["target_table"]
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT COUNT(*) FROM {target}",
                wait_timeout="30s"
            )
            if result.result and result.result.data_array:
                count = result.result.data_array[0][0]
                print(f"  [OK] {target}: {count} rows")
            else:
                print(f"  [WARN] {target}: Could not verify")
        except Exception as e:
            print(f"  [ERROR] {target}: {str(e)[:50]}")

    print("\n" + "=" * 70)
    print("Dimension tables created!")
    print("=" * 70)


if __name__ == "__main__":
    main()
