#!/usr/bin/env python3
"""
Test script to verify connections to SQL Server and Databricks.
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.lakebridge.connections.database_manager import DatabaseManager


def load_credentials():
    """Load credentials from the lakebridge credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    if not creds_path.exists():
        raise FileNotFoundError(f"Credentials file not found at {creds_path}")
    
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_sql_server_connection(creds: dict) -> bool:
    """Test SQL Server connection."""
    print("\n" + "=" * 60)
    print("TESTING SQL SERVER CONNECTION")
    print("=" * 60)
    
    if "mssql" not in creds:
        print("[FAIL] No MSSQL credentials found in config")
        return False
    
    mssql_config = creds["mssql"]
    print(f"Server: {mssql_config.get('server')}")
    print(f"Database: {mssql_config.get('database')}")
    print(f"User: {mssql_config.get('user')}")
    print(f"Auth Type: {mssql_config.get('auth_type')}")
    
    try:
        db_manager = DatabaseManager("mssql", mssql_config)
        
        # Test connection with a simple query
        if db_manager.check_connection():
            print("[OK] SQL Server connection SUCCESSFUL!")
            
            # Run a test query to show it's working
            result = db_manager.fetch("SELECT @@VERSION AS version")
            print(f"\nSQL Server Version:")
            print(f"  {result.rows[0][0][:100]}...")
            
            # Get table count
            result = db_manager.fetch("""
                SELECT COUNT(*) as table_count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            print(f"\nTotal tables in database: {result.rows[0][0]}")
            return True
        else:
            print("[FAIL] SQL Server connection FAILED")
            return False
            
    except Exception as e:
        print(f"[FAIL] SQL Server connection FAILED: {e}")
        return False


def test_databricks_connection(creds: dict) -> bool:
    """Test Databricks connection."""
    print("\n" + "=" * 60)
    print("TESTING DATABRICKS CONNECTION")
    print("=" * 60)
    
    if "databricks" not in creds:
        print("[FAIL] No Databricks credentials found in config")
        return False
    
    db_config = creds["databricks"]
    host = db_config.get("host")
    token = db_config.get("token")
    
    print(f"Host: {host}")
    print(f"Catalog: {db_config.get('catalog')}")
    print(f"Schema: {db_config.get('schema')}")
    
    try:
        # Create workspace client
        w = WorkspaceClient(
            host=host,
            token=token
        )
        
        # Test connection by getting current user
        user = w.current_user.me()
        print(f"[OK] Databricks connection SUCCESSFUL!")
        print(f"\nConnected as: {user.user_name}")
        
        # Get workspace info
        try:
            clusters = list(w.clusters.list())
            print(f"Total clusters visible: {len(clusters)}")
        except Exception:
            pass
        
        # Try to list catalogs
        try:
            catalogs = list(w.catalogs.list())
            print(f"Total catalogs: {len(catalogs)}")
            if catalogs:
                print(f"Available catalogs: {', '.join(c.name for c in catalogs[:5])}")
        except Exception as e:
            print(f"(Could not list catalogs: {e})")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] Databricks connection FAILED: {e}")
        return False


def main():
    print("=" * 60)
    print("LAKEBRIDGE CONNECTION TEST")
    print("=" * 60)
    
    try:
        creds = load_credentials()
        print("[OK] Credentials file loaded successfully")
    except Exception as e:
        print(f"[FAIL] Failed to load credentials: {e}")
        return 1
    
    sql_success = test_sql_server_connection(creds)
    db_success = test_databricks_connection(creds)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"SQL Server:  {'[OK] Connected' if sql_success else '[FAIL] Failed'}")
    print(f"Databricks:  {'[OK] Connected' if db_success else '[FAIL] Failed'}")
    
    return 0 if (sql_success and db_success) else 1


if __name__ == "__main__":
    sys.exit(main())
