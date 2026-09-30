from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DBT_PROJECT_DIR = Path("/dbt/jobscrapper")
DBT_EXECUTABLE_PATH = Path("/home/airflow/.local/bin/dbt")

profile_config = ProfileConfig(
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
    profile_name="trino",
    target_name="gold",
)

with DAG(
    dag_id="test_dbt_trino_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": 0,
    },
    tags=["dbt", "trino", "test"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    
    t_load_villages = SparkSubmitOperator(
        task_id="upload_seed_vilages",
        application="/jobs/spark/upload_seed_vilages.py", # Script pyspark untuk meload CSV
        packages="org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        conf={"spark.cores.max": "2"},
        conn_id="spark_default",
    )

    dbt_transform = DbtTaskGroup(
        group_id="jobscrapper",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT_EXECUTABLE_PATH),
        ),
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> t_load_villages >> dbt_transform >> end