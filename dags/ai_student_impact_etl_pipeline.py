import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Set global Google Cloud Project
os.environ["GOOGLE_CLOUD_PROJECT"] = "project-repo-498812"

# ==========================================
# 1. KAGGLE FETCH PIPELINE
# ==========================================
def download_and_read_ai_impact_data():
    import kagglehub
    import pandas as pd
    from kagglehub import KaggleDatasetAdapter
    
    print("--- STARTING AUTOMATED KAGGLE FETCH ---")
    
    download_dir = "/opt/airflow/data/"
    os.makedirs(download_dir, exist_ok=True)
    
    # UPDATE: Replace with the actual 'username/dataset-slug' from Kaggle
    kaggle_dataset = "your_kaggle_username/ai-student-impact-dataset"
    
    path = kagglehub.dataset_download(kaggle_dataset)
    print(f"Kaggle files successfully downloaded to cache path: {path}")
    
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        kaggle_dataset,
        "ai_student_impact_dataset.csv" # UPDATE: Looking for the new CSV filename
    )
    
    print("\n================== FIRST 5 RECORDS ==================")
    print(df.head())
    print("=====================================================\n")

with DAG(
    dag_id="ai_student_impact_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    download_ai_data_task = PythonOperator(
        task_id="download_and_parse_kaggle_data",
        python_callable=download_and_read_ai_impact_data
    )


# ==========================================
# 2. AUTOMATED GCS TO BIGQUERY PIPELINE
# ==========================================
with DAG(
    dag_id="gcs_to_bigquery_automation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as bq_dag:

    drop_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="drop_gcs_to_bigquery",
        gcp_conn_id="google_cloud_default",
        bucket="project-repo-498812-rawdata",
        source_objects=["*.csv"],  # Monitors the bucket root for your CSV drop
        destination_project_dataset_table="project-repo-498812.raw_data_dataset.target_table", # Update destination table here if needed
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        autodetect=True, # Automatically adapts to the new AI dataset columns
        skip_leading_rows=1,
    )