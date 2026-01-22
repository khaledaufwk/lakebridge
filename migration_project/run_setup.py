#!/usr/bin/env python3
"""
Quick setup script using existing credentials.
"""

import sys
import urllib.request
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient

# Configuration
SECRET_SCOPE = "wakecap_migration"
PIPELINE_ID = "f2c1c736-d12c-4274-b9da-a7f4119afa68"
JDBC_DRIVER_URL = "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar"
JDBC_DRIVER_FILENAME = "mssql-jdbc-12.4.2.jre11.jar"


def main():
    print("=" * 60)
    print("WakeCapDW Migration - Setup")
    print("=" * 60)

    # Load credentials
    print("\n[1] Loading credentials...")
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    print(f"    Host: {creds['databricks']['host']}")
    print(f"    SQL Server: {creds['mssql']['server']}")

    # Connect to Databricks
    print("\n[2] Connecting to Databricks...")
    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )
    user = w.current_user.me()
    print(f"    Connected as: {user.user_name}")

    # Download JDBC driver
    print("\n[3] Downloading JDBC driver...")
    temp_dir = Path(tempfile.gettempdir())
    local_path = temp_dir / JDBC_DRIVER_FILENAME

    if not local_path.exists():
        print(f"    Downloading from Maven Central...")
        urllib.request.urlretrieve(JDBC_DRIVER_URL, local_path)

    size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"    Downloaded: {local_path} ({size_mb:.2f} MB)")

    # Upload to DBFS
    print("\n[4] Uploading JDBC driver to DBFS...")
    dbfs_path = f"/FileStore/jars/{JDBC_DRIVER_FILENAME}"

    try:
        status = w.dbfs.get_status(dbfs_path)
        print(f"    Already exists: dbfs:{dbfs_path} ({status.file_size/(1024*1024):.2f} MB)")
    except:
        print(f"    Uploading to dbfs:{dbfs_path}...")
        # Use upload method for large files
        with open(local_path, 'rb') as f:
            w.dbfs.upload(dbfs_path, f, overwrite=True)
        print(f"    Uploaded successfully!")

    # Setup secret scope
    print("\n[5] Configuring secret scope...")
    mssql = creds['mssql']

    try:
        w.secrets.create_scope(scope=SECRET_SCOPE)
        print(f"    Created scope: {SECRET_SCOPE}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"    Scope exists: {SECRET_SCOPE}")
        else:
            print(f"    Warning: {e}")

    # Build JDBC URL
    jdbc_url = (
        f"jdbc:sqlserver://{mssql['server']}:{mssql.get('port', 1433)};"
        f"database={mssql['database']};"
        f"encrypt={'true' if mssql.get('encrypt', True) else 'false'};"
        f"trustServerCertificate={'true' if mssql.get('trustServerCertificate', False) else 'false'};"
        f"loginTimeout={mssql.get('loginTimeout', 30)}"
    )

    secrets = [
        ("sqlserver_jdbc_url", jdbc_url),
        ("sqlserver_user", mssql['user']),
        ("sqlserver_password", mssql['password']),
        ("sqlserver_server", mssql['server']),
        ("sqlserver_database", mssql['database']),
    ]

    for key, value in secrets:
        try:
            w.secrets.put_secret(scope=SECRET_SCOPE, key=key, string_value=value)
            print(f"    Stored: {key}")
        except Exception as e:
            print(f"    {key}: {e}")

    # List secrets
    print("\n    Secrets in scope:")
    for s in w.secrets.list_secrets(scope=SECRET_SCOPE):
        print(f"      - {s.key}")

    # Check pipeline
    print("\n[6] Checking pipeline...")
    try:
        pipeline = w.pipelines.get(pipeline_id=PIPELINE_ID)
        print(f"    Name: {pipeline.name}")
        print(f"    State: {pipeline.state}")
        print(f"    Catalog: {pipeline.catalog}")
        print(f"    Target: {pipeline.target}")
    except Exception as e:
        print(f"    Error: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print(f"""
JDBC Driver: dbfs:{dbfs_path}
Secret Scope: {SECRET_SCOPE}
Pipeline: {creds['databricks']['host']}/pipelines/{PIPELINE_ID}

NEXT STEPS:
1. Configure Azure SQL firewall to allow Databricks IPs
2. Start the pipeline from Databricks UI
3. Monitor for connection errors
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
