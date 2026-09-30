from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_arg = {
    'retries': 0,
    'owner': 'data_team'
}


with DAG(
    dag_id="ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    default_args=default_arg,
    catchup=False,
) as dag:
    t1 = SparkSubmitOperator(
            task_id="extract_majors",
            application="/jobs/spark/listen.py",
            packages=(
                "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.11.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
            ),
            conn_id="spark_default",
        )
    
    t1