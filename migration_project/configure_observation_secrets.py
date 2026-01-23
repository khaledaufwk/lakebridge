#!/usr/bin/env python3
"""
Configure Databricks secrets for the wakecap_observation TimescaleDB connection.

This script creates the 'wakecap-observation' secret scope and stores the
credentials needed to connect to the wakecap_observation database.

Usage:
    python configure_observation_secrets.py
"""

import sys
import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient


def load_credentials():
    """Load credentials from the credentials template file."""
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    print("=" * 60)
    print("DATABRICKS SECRETS CONFIGURATION - OBSERVATION DATABASE")
    print("=" * 60)

    # Load credentials
    creds = load_credentials()
    db_config = creds['databricks']
    obs_config = creds['timescaledb_observation']

    # Connect to Databricks
    print("\n1. Connecting to Databricks...")
    w = WorkspaceClient(
        host=db_config['host'],
        token=db_config['token']
    )

    user = w.current_user.me()
    print(f"   Connected as: {user.user_name}")

    # Define secret scope
    scope_name = "wakecap-observation"

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

    # Store secrets
    print("\n3. Storing secrets...")

    secrets_to_store = [
        ("timescaledb-host", obs_config['host']),
        ("timescaledb-port", str(obs_config['port'])),
        ("timescaledb-database", obs_config['database']),
        ("timescaledb-user", obs_config['user']),
        ("timescaledb-password", obs_config['password']),
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

    from timescaledb_loader_v2 import TimescaleDBCredentials
    credentials = TimescaleDBCredentials.from_databricks_secrets("{scope_name}", dbutils=dbutils)

Connection details:
    Host: {obs_config['host']}
    Port: {obs_config['port']}
    Database: {obs_config['database']}
    User: {obs_config['user']}
""")

    return 0


if __name__ == "__main__":
    sys.exit(main())
