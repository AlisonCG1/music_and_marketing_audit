from dagster import job, op, resource
from dagster_dbt import DbtCliResource
import subprocess

# Paths
DBT_PROJECT_DIR = "/Users/alisoncordoba/captone/music_and_marketing_audit/music_transform"
DBT_PROFILES_DIR = "/Users/alisoncordoba/.dbt"  # Expanded tilde for clarity

# Define dbt resource
@resource
def dbt_resource():
    return DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )

# Define ops for running and testing
@op(required_resource_keys={"dbt"})
def dbt_run(context):
    # Run dbt run command
    result = context.resources.dbt.cli(["run"])
    # Don't return the full process object; just log completion
    context.log.info("dbt run completed successfully")
    return "success"  # Return simple, serializable value

@op(required_resource_keys={"dbt"})
def dbt_test(context, run_status):
    if run_status != "success":
        context.log.error("Skipping tests because dbt run failed")
        return
    context.resources.dbt.cli(["test"])
    context.log.info("dbt test completed successfully")

# Define the job
@job(resource_defs={"dbt": dbt_resource})
def dbt_job():
    dbt_test(dbt_run())
