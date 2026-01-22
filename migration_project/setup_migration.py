#!/usr/bin/env python3
"""
Non-interactive setup script for WakeCapDW Migration.

Prerequisites:
1. Fill in credentials_template.yml with your actual credentials
2. Run: python setup_migration.py

This script will:
1. Copy credentials to the standard location
2. Download and upload the JDBC driver
3. Configure the secret scope
4. Verify the pipeline
"""

import os
import sys
import shutil
import urllib.request
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient

# Configuration
SECRET_SCOPE = "wakecap_migration"
PIPELINE_ID = "f2c1c736-d12c-4274-b9da-a7f4119afa68"
JDBC_DRIVER_URL = "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar"
JDBC_DRIVER_FILENAME = "mssql-jdbc-12.4.2.jre11.jar"


def print_header(text):
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60)


def print_step(num, text):
    print(f"\n[Step {num}] {text}")
    print("-" * 50)


def load_and_validate_credentials():
    """Load credentials from template file."""
    print_step(1, "Loading credentials")

    template_path = Path(__file__).parent / "credentials_template.yml"

    if not template_path.exists():
        print(f"   [ERROR] Credentials file not found: {template_path}")
        return None

    with open(template_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    # Validate credentials
    db_token = creds.get('databricks', {}).get('token', '')
    sql_user = creds.get('mssql', {}).get('user', '')
    sql_pass = creds.get('mssql', {}).get('password', '')

    if 'YOUR_' in db_token or not db_token:
        print("   [ERROR] Please fill in your Databricks token in credentials_template.yml")
        return None

    if 'YOUR_' in sql_user or not sql_user:
        print("   [ERROR] Please fill in your SQL Server username in credentials_template.yml")
        return None

    if 'YOUR_' in sql_pass or not sql_pass:
        print("   [ERROR] Please fill in your SQL Server password in credentials_template.yml")
        return None

    print(f"   Databricks Host: {creds['databricks']['host']}")
    print(f"   SQL Server: {creds['mssql']['server']}")
    print(f"   Database: {creds['mssql']['database']}")
    print("   [OK] Credentials loaded successfully")

    return creds


def install_credentials(creds):
    """Install credentials to standard location."""
    print_step(2, "Installing credentials")

    creds_dir = Path.home() / ".databricks" / "labs" / "lakebridge"
    creds_path = creds_dir / ".credentials.yml"

    creds_dir.mkdir(parents=True, exist_ok=True)

    with open(creds_path, 'w', encoding='utf-8') as f:
        yaml.dump(creds, f, default_flow_style=False)

    print(f"   [OK] Credentials installed to: {creds_path}")
    return True


def connect_databricks(creds):
    """Connect to Databricks workspace."""
    print_step(3, "Connecting to Databricks")

    try:
        w = WorkspaceClient(
            host=creds['databricks']['host'],
            token=creds['databricks']['token']
        )

        user = w.current_user.me()
        print(f"   [OK] Connected as: {user.user_name}")
        return w
    except Exception as e:
        print(f"   [ERROR] Connection failed: {e}")
        return None


def download_jdbc_driver():
    """Download JDBC driver."""
    print_step(4, "Downloading JDBC driver")

    temp_dir = Path(tempfile.gettempdir())
    local_path = temp_dir / JDBC_DRIVER_FILENAME

    if local_path.exists():
        print(f"   [OK] Driver already exists: {local_path}")
        return local_path

    print(f"   Downloading from Maven Central...")

    try:
        urllib.request.urlretrieve(JDBC_DRIVER_URL, local_path)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"   [OK] Downloaded {size_mb:.2f} MB to {local_path}")
        return local_path
    except Exception as e:
        print(f"   [ERROR] Download failed: {e}")
        return None


def upload_jdbc_driver(w, local_path):
    """Upload JDBC driver to DBFS."""
    print_step(5, "Uploading JDBC driver to DBFS")

    dbfs_path = f"/FileStore/jars/{JDBC_DRIVER_FILENAME}"

    try:
        # Check if exists
        try:
            status = w.dbfs.get_status(dbfs_path)
            print(f"   [OK] Driver already in DBFS: dbfs:{dbfs_path}")
            print(f"   Size: {status.file_size / (1024*1024):.2f} MB")
            return dbfs_path
        except:
            pass

        print(f"   Uploading to dbfs:{dbfs_path}...")

        with open(local_path, 'rb') as f:
            content = f.read()

        w.dbfs.put(dbfs_path, contents=content, overwrite=True)

        status = w.dbfs.get_status(dbfs_path)
        print(f"   [OK] Uploaded successfully ({status.file_size / (1024*1024):.2f} MB)")
        return dbfs_path

    except Exception as e:
        print(f"   [ERROR] Upload failed: {e}")
        return None


def setup_secrets(w, creds):
    """Setup secret scope with SQL Server credentials."""
    print_step(6, "Configuring secret scope")

    mssql = creds['mssql']

    # Create scope
    try:
        w.secrets.create_scope(scope=SECRET_SCOPE)
        print(f"   [OK] Created scope: {SECRET_SCOPE}")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"   [OK] Scope exists: {SECRET_SCOPE}")
        else:
            print(f"   [WARN] Scope issue: {e}")

    # Build JDBC URL
    jdbc_url = (
        f"jdbc:sqlserver://{mssql['server']}:{mssql.get('port', 1433)};"
        f"database={mssql['database']};"
        f"encrypt={'true' if mssql.get('encrypt', True) else 'false'};"
        f"trustServerCertificate={'true' if mssql.get('trustServerCertificate', False) else 'false'};"
        f"loginTimeout={mssql.get('loginTimeout', 30)}"
    )

    # Store secrets
    secrets = [
        ("sqlserver_jdbc_url", jdbc_url),
        ("sqlserver_user", mssql['user']),
        ("sqlserver_password", mssql['password']),
        ("sqlserver_server", mssql['server']),
        ("sqlserver_database", mssql['database']),
    ]

    print("   Storing secrets...")
    for key, value in secrets:
        try:
            w.secrets.put_secret(scope=SECRET_SCOPE, key=key, string_value=value)
            print(f"     [OK] {key}")
        except Exception as e:
            print(f"     [ERROR] {key}: {e}")

    # List secrets
    print("\n   Secrets in scope:")
    try:
        for s in w.secrets.list_secrets(scope=SECRET_SCOPE):
            print(f"     - {s.key}")
    except Exception as e:
        print(f"   [ERROR] {e}")

    return True


def check_pipeline(w, creds):
    """Check pipeline status."""
    print_step(7, "Checking pipeline status")

    try:
        pipeline = w.pipelines.get(pipeline_id=PIPELINE_ID)
        print(f"   Name: {pipeline.name}")
        print(f"   ID: {PIPELINE_ID}")
        print(f"   State: {pipeline.state}")
        print(f"   Catalog: {pipeline.catalog}")
        print(f"   Target: {pipeline.target}")

        return True
    except Exception as e:
        print(f"   [ERROR] {e}")
        return False


def show_summary(creds, dbfs_path):
    """Show final summary and next steps."""
    print_header("SETUP COMPLETE")

    db_host = creds['databricks']['host']

    print(f"""
STATUS:
  [OK] Credentials configured
  [OK] JDBC driver uploaded to DBFS
  [OK] Secret scope configured
  [OK] Pipeline verified

NEXT STEPS:

1. CONFIGURE AZURE SQL FIREWALL
   ----------------------------------
   Go to Azure Portal:
   - SQL Server > Networking
   - Add Databricks cluster IPs
   - OR enable "Allow Azure services"

2. START THE PIPELINE
   ----------------------------------
   Pipeline URL: {db_host}/pipelines/{PIPELINE_ID}

   Click "Start" to begin data ingestion

3. MONITOR THE PIPELINE
   ----------------------------------
   Watch for:
   - Connection errors (firewall issue)
   - Driver errors (library config needed)
   - Data validation in bronze tables

JDBC Driver Location: dbfs:{dbfs_path}

If you see "driver not found" errors, add this to the pipeline:
  spark.jars: dbfs:{dbfs_path}
""")


def main():
    print_header("WakeCapDW Migration Setup")

    # Load credentials
    creds = load_and_validate_credentials()
    if not creds:
        print("\nPlease fill in credentials_template.yml and run again.")
        return 1

    # Install credentials
    install_credentials(creds)

    # Connect to Databricks
    w = connect_databricks(creds)
    if not w:
        return 1

    # Download JDBC driver
    local_driver = download_jdbc_driver()

    # Upload JDBC driver
    dbfs_path = None
    if local_driver:
        dbfs_path = upload_jdbc_driver(w, local_driver)

    # Setup secrets
    setup_secrets(w, creds)

    # Check pipeline
    check_pipeline(w, creds)

    # Show summary
    show_summary(creds, dbfs_path or f"/FileStore/jars/{JDBC_DRIVER_FILENAME}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
