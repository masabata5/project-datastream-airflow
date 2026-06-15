from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd

def etl_task():
    # Extract
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)

    data = response.json()

    # Transform
    df = pd.DataFrame(data)

    df = df.drop_duplicates()

    # Load
    df.to_csv("/tmp/users.csv", index=False)

    print("ETL completed successfully")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id="run_etl",
        python_callable=etl_task
    )