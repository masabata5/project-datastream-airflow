import os
os.environ["GOOGLE_CLOUD_PROJECT"] = "project-repo-498812"
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def download_and_read_fifa_data():
    import kagglehub
    import pandas as pd
    from kagglehub import KaggleDatasetAdapter
    
    print("--- STARTING AUTOMATED KAGGLE FETCH ---")
    
    download_dir = "/opt/airflow/data/"
    os.makedirs(download_dir, exist_ok=True)
    
    path = kagglehub.dataset_download("rauffauzanrambe/fifa-world-cup-2026-player-performance-dataset")
    print(f"Kaggle files successfully downloaded to cache path: {path}")
    
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "rauffauzanrambe/fifa-world-cup-2026-player-performance-dataset",
        "fifa_world_cup_2026_player_performance.csv"
    )
    
    print("\n================== FIRST 5 RECORDS ==================")
    print(df.head())
    print("=====================================================\n")

with DAG(
    dag_id="fifa_player_performance_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    download_fifa_task = PythonOperator(
        task_id="download_and_parse_kaggle_data",
        python_callable=download_and_read_fifa_data
    )

# ==========================================
# AUTOMATED GCS TO BIGQUERY PIPELINE
# ==========================================
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id="gcs_to_bigquery_automation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as bq_dag:

    drop_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="drop_gcs_to_bigquery",
        gcp_conn_id="google_cloud_default",
        bucket="your_gcs_bucket_name",  # <-- Check your bucket name here
        source_objects=["data/*.csv"],
        destination_project_dataset_table="your_project.your_dataset.your_table", # <-- Check your table here
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

# ==========================================
# AUTOMATED GCS TO BIGQUERY PIPELINE
# ==========================================
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

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
        source_objects=["*.csv"],  # Looks for any CSV dropped in the bucket root
        destination_project_dataset_table="project-repo-498812.raw_data_dataset.target_table", # Update dataset.table if needed
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        skip_leading_rows=1, # Skips header row automatically
    )

# ==========================================
# AUTOMATED GCS TO BIGQUERY PIPELINE
# ==========================================
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

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
        source_objects=["*.csv"],
        destination_project_dataset_table="project-repo-498812.raw_data_dataset.target_table",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        skip_leading_rows=1,
    )
