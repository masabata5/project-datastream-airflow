from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def upload_csv_to_gcs():
    csv_path = Path(__file__).parent / "data" / "early_wakeup_health_dataset.csv"

    bucket_name = "project-repo-498812-rawdata"

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob("early_wakeup_health_dataset.csv")

    blob.upload_from_filename(csv_path)

    print("CSV uploaded successfully!")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    run_etl = PythonOperator(
    task_id="upload_csv_to_gcs",
    python_callable=upload_csv_to_gcs
)
    
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        gcp_conn_id="google_cloud_default",
        bucket="project-repo-498812-rawdata",
        source_format="CSV",
        source_objects=["early_wakeup_health_dataset.csv"],
        destination_project_dataset_table="project-repo-498812.raw_data_dataset.nonny",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND",
)
    run_etl >> load_to_bigquery

