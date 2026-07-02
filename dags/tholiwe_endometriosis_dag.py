from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def process_csv():
    # Path to your CSV file
    file_path = "/opt/airflow/data/tholiwe_endometriosis.csv"

    # Read CSV
    df = pd.read_csv(file_path)

    # Print information to Airflow logs
    print("CSV loaded successfully")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")
    print(df.head())

with DAG(
    dag_id="tholiwe_endometriosis_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    process_task = PythonOperator(
        task_id="process_csv",
        python_callable=process_csv,
    )

    process_task