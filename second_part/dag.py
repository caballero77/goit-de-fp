from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    "project_solution_maslianko_andrii",
    default_args={
        "owner": "airflow",
        "start_date": "2025-09-14",
    },
    schedule=None,
    tags=["maslianko_andrii"],
)


landing_to_bronze_task = SparkSubmitOperator(
    task_id="landing_to_bronze",
    application="/opt/airflow/dags/spark_scripts/landing_to_bronze.py",
    conn_id="spark_default",
    dag=dag,
)

bronze_to_silver_task = SparkSubmitOperator(
    task_id="bronze_to_silver",
    application="/opt/airflow/dags/spark_scripts/bronze_to_silver.py",
    conn_id="spark_default",
    dag=dag,
)

silver_to_gold_task = SparkSubmitOperator(
    task_id="silver_to_gold",
    application="/opt/airflow/dags/spark_scripts/silver_to_gold.py",
    conn_id="spark_default",
    dag=dag,
)

landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task