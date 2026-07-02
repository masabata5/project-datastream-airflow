from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Function to process the CSV
def process_heart_data():
    csv_file = "/home/airflow/data/heart.csv"  # Update with your file path

    # Read CSV
    df = pd.read_csv(csv_file)

    # Display basic information
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(df.head())

    # Check for missing values
    print("Missing Values:")
    print(df.isnull().sum())

    # Remove duplicates
    df = df.drop_duplicates()

    # Save cleaned data
    output_file = "/home/airflow/data/heart_cleaned.csv"
    df.to_csv(output_file, index=False)

    print(f"Cleaned file saved to: {output_file}")

# Define DAG
with DAG(
    dag_id="heart_csv_processing",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    process_csv = PythonOperator(
        task_id="process_heart_csv",
        python_callable=process_heart_data,
    )

    process_csv