from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd

from google.cloud import storage

# Extract
# Extract
def extract_data():
    print("Starting data extraction")

    df = pd.read_csv('/opt/airflow/data/corporate_ai_adoption_dataset.csv')

    df.to_csv("/tmp/raw_users.csv", index=False)

    print("Data extraction completed")


# Transform
def transform_data():
    print("Starting data transformation")

    df = pd.read_csv("/tmp/raw_users.csv")

    df = df.drop_duplicates()

    df = df[[
        "company_id", "industry", "country", "year",
        "ai_adoption_level", "ai_investment_usd", "automation_rate",
        "cost_savings", "revenue_impact", "productivity_gain",
        "employee_ai_training_hours", "ai_maturity_score", "deployment_count"
    ]]

    df.to_csv("/tmp/clean_users.csv", index=False)

    print("Data transformation completed")


# Load
def load_data():

    print("Starting data load")

    try:

        client = storage.Client(
            project="project-repo-498812"
        )

        bucket = client.bucket(
            "project-repo-498812-rawdata"
        )

        blob = bucket.blob(
            "users/users.csv"
        )

        blob.upload_from_filename(
            "/tmp/clean_users.csv"
        )

        print(
            "File uploaded successfully to GCS"
        )

    except Exception as e:

        print(
            f"Upload failed: {e}"
        )

        raise

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