from dagster import job
from music_transform import dbt_run_op, dbt_test_op

# Paths to your dbt project
DBT_PROJECT_DIR = "path/to/your/dbt_project"  # root folder containing dbt_project.yml
DBT_PROFILES_DIR = "~/.dbt"  # or wherever your profiles.yml is

@job
def dbt_job():
    # Run all models (bronze, silver, gold)
    run_results = dbt_run_op.configured(
        {
            "project_dir": DBT_PROJECT_DIR,
            "profiles_dir": DBT_PROFILES_DIR,
        }
    )()
    
    # Test all models after run
    dbt_test_op.configured(
        {
            "project_dir": DBT_PROJECT_DIR,
            "profiles_dir": DBT_PROFILES_DIR,
        }
    )(run_results)
