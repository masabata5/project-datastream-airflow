from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_task():
    print("ETL Pipeline Running")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=test_task
    )