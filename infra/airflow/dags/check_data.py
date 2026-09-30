from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="check_data",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="debug",
        application="/jobs/spark/debug.py",
        packages="org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        conf={
            "spark.cores.max": "2"
        },
        verbose=True,
        conn_id="spark_default",
        dag=dag
    )