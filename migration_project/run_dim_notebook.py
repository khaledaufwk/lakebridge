"""Run the dimension creation notebook."""
import sys
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import SubmitTask, NotebookTask, Source

# Load credentials
creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
with open(creds_path) as f:
    creds = yaml.safe_load(f).get("databricks", {})

w = WorkspaceClient(host=creds["host"], token=creds["token"])

# Run dimension creation notebook
print("Running dimension creation notebook...")
run = w.jobs.submit(
    run_name="create_observation_dimensions",
    tasks=[
        SubmitTask(
            task_key="create_dims",
            existing_cluster_id="0118-134705-lklfkwvh",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/migration_project/pipelines/gold/notebooks/create_observation_dimensions",
                source=Source.WORKSPACE
            )
        )
    ]
)
print(f"Run ID: {run.run_id}")

# Wait for completion
for i in range(60):
    r = w.jobs.get_run(run_id=run.run_id)
    state = r.state.life_cycle_state.value if r.state.life_cycle_state else "UNKNOWN"
    result = r.state.result_state.value if r.state.result_state else "PENDING"
    print(f"[{i*5}s] {state} / {result}")

    if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        if result == "SUCCESS":
            print("\n=== SUCCESS ===")
        else:
            print(f"\n=== FAILED: {result} ===")
            if r.state.state_message:
                print(r.state.state_message)
        break
    time.sleep(5)
