from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
)


DBT_PROJECT_DIR = Path("/dbt/jobscrapper")
DBT_EXECUTABLE_PATH = Path("/home/airflow/.local/bin/dbt")

ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)

SPARK_CONN_ID = "spark_default"


default_args = {
    "owner": "data_team",
    "retries": 0,
}


profile_config = ProfileConfig(
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
    profile_name="trino",
    target_name="gold",
)

execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE_PATH),
)


with DAG(
    dag_id="spark_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["spark", "iceberg", "dbt", "warehouse"],
) as dag:


    @task_group(group_id="extracting")
    def extracting():


        load_seed_villages = SparkSubmitOperator(
            task_id="upload_seed_villages",
            application="/jobs/spark/upload_seed_vilages.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
            },
        )


        extract_salary = SparkSubmitOperator(
            task_id="extract_salary",
            application="/jobs/spark/extract_salary.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
        )


        extract_region = SparkSubmitOperator(
            task_id="extract_region",
            application="/jobs/spark/extract_region.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
            },
        )


        extract_skills = SparkSubmitOperator(
            task_id="extract_skills",
            application="/jobs/spark/extract_skills.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
            },
        )

        extract_majors = SparkSubmitOperator(
            task_id="extract_majors",
            application="/jobs/spark/extract_majors.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
                "spark.executor.memoryOverhead": "1g",
            },
        )

        extract_language = SparkSubmitOperator(
            task_id="extract_language",
            application="/jobs/spark/extract_language.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
                "spark.executor.memoryOverhead": "1g",
            },
        )

        job_seniority = SparkSubmitOperator(
            task_id="extract_job_seniority",
            application="/jobs/spark/extract_seniority.py",
            packages=ICEBERG_PACKAGES,
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.cores.max": "4",
                "spark.executor.memoryOverhead": "1g",
            },
        )
        

        load_seed_villages >> extract_region
        
        return [
            extract_majors,
            extract_region,
            extract_language,
            job_seniority,
            extract_skills,
            extract_salary,
        ]


    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            DBT_PROJECT_DIR,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={
            "retries": 4,
            "retry_delay": timedelta(minutes=1),
        },
    )


    end = EmptyOperator(
        task_id="end_pipeline",
    )


    extracting() >> dbt >> end