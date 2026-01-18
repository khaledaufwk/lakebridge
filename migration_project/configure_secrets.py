#!/usr/bin/env python3
"""
Configure Databricks secrets for SQL Server connection.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission


def load_credentials():
    """Load credentials from the lakebridge credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    print("=" * 60)
    print("DATABRICKS SECRETS CONFIGURATION")
    print("=" * 60)
    
    # Load credentials
    creds = load_credentials()
    db_config = creds['databricks']
    mssql_config = creds['mssql']
    
    # Connect to Databricks
    print("\n1. Connecting to Databricks...")
    w = WorkspaceClient(
        host=db_config['host'],
        token=db_config['token']
    )
    
    user = w.current_user.me()
    print(f"   Connected as: {user.user_name}")
    
    # Define secret scope
    scope_name = "wakecap_migration"
    
    # Create secret scope
    print(f"\n2. Creating secret scope '{scope_name}'...")
    try:
        w.secrets.create_scope(scope=scope_name)
        print(f"   [OK] Secret scope '{scope_name}' created")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print(f"   [OK] Secret scope '{scope_name}' already exists")
        else:
            print(f"   [WARN] Could not create scope: {e}")
    
    # Build JDBC URL
    jdbc_url = (
        f"jdbc:sqlserver://{mssql_config['server']}:{mssql_config.get('port', 1433)};"
        f"database={mssql_config['database']};"
        f"encrypt={'true' if mssql_config.get('encrypt', True) else 'false'};"
        f"trustServerCertificate={'true' if mssql_config.get('trustServerCertificate', False) else 'false'};"
        f"loginTimeout={mssql_config.get('loginTimeout', 30)}"
    )
    
    # Store secrets
    print("\n3. Storing secrets...")
    
    secrets_to_store = [
        ("sqlserver_jdbc_url", jdbc_url),
        ("sqlserver_user", mssql_config['user']),
        ("sqlserver_password", mssql_config['password']),
        ("sqlserver_server", mssql_config['server']),
        ("sqlserver_database", mssql_config['database']),
    ]
    
    for key, value in secrets_to_store:
        try:
            w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
            print(f"   [OK] Stored secret: {key}")
        except Exception as e:
            print(f"   [ERROR] Failed to store {key}: {e}")
    
    # Verify secrets
    print("\n4. Verifying secrets...")
    try:
        secret_list = list(w.secrets.list_secrets(scope=scope_name))
        print(f"   Secrets in scope '{scope_name}':")
        for secret in secret_list:
            print(f"     - {secret.key}")
    except Exception as e:
        print(f"   [ERROR] Could not list secrets: {e}")
    
    print("\n" + "=" * 60)
    print("SECRETS CONFIGURATION COMPLETE")
    print("=" * 60)
    
    print(f"""
To use these secrets in your DLT notebook:

    jdbc_url = dbutils.secrets.get("{scope_name}", "sqlserver_jdbc_url")
    user = dbutils.secrets.get("{scope_name}", "sqlserver_user")
    password = dbutils.secrets.get("{scope_name}", "sqlserver_password")

JDBC URL format:
    {jdbc_url[:80]}...
""")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
