#!/usr/bin/env python3
"""Check cluster configuration."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.sdk import WorkspaceClient


def main():
    creds_path = Path(__file__).parent / "credentials_template.yml"
    with open(creds_path, encoding='utf-8') as f:
        creds = yaml.safe_load(f)

    w = WorkspaceClient(
        host=creds['databricks']['host'],
        token=creds['databricks']['token']
    )

    # Find the cluster
    for cluster in w.clusters.list():
        if cluster.cluster_name and "Migrate" in cluster.cluster_name:
            print(f"Cluster: {cluster.cluster_name}")
            print(f"  ID: {cluster.cluster_id}")
            print(f"  State: {cluster.state}")
            print(f"  Spark Version: {cluster.spark_version}")
            print(f"  Node Type: {cluster.node_type_id}")
            print(f"  Num Workers: {cluster.num_workers}")
            if cluster.data_security_mode:
                print(f"  Data Security Mode: {cluster.data_security_mode}")
            if cluster.single_user_name:
                print(f"  Single User: {cluster.single_user_name}")

            # Get full details
            details = w.clusters.get(cluster.cluster_id)

            if details.spark_conf:
                print(f"  Spark Config:")
                for k, v in details.spark_conf.items():
                    print(f"    {k}: {v}")

            if details.cluster_log_conf:
                print(f"  Log Config: {details.cluster_log_conf}")

            # Check events
            print(f"\n  Recent Events:")
            try:
                events = w.clusters.events(cluster_id=cluster.cluster_id, limit=5)
                for event in events.events:
                    print(f"    {event.timestamp}: {event.type} - {event.details}")
            except Exception as e:
                print(f"    Could not get events: {e}")


if __name__ == "__main__":
    main()
