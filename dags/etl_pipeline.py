from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd


# Extract
def extract_data():
    print("Starting data extraction")

    url = "https://jsonplaceholder.typicode.com/users"

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame(data)

    df.to_csv("/tmp/raw_users.csv", index=False)

    print("Data extraction completed")


# Transform
def transform_data():
    print("Starting data transformation")

    df = pd.read_csv("/tmp/raw_users.csv")

    df = df.drop_duplicates()

    df = df[["id", "name", "email", "phone"]]

    df.columns = [
        "user_id",
        "full_name",
        "email",
        "phone"
    ]

    df.to_csv("/tmp/clean_users.csv", index=False)

    print("Data transformation completed")


# Load
def load_data():
    print("Starting data load")

    df = pd.read_csv("/tmp/clean_users.csv")

    df.to_csv("/tmp/users.csv", index=False)

    print("Data load completed")


with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract >> transform >> load