import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set global Google Cloud Project
os.environ["GOOGLE_CLOUD_PROJECT"] = "project-repo-498812"

if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

# ==========================================
# COMBINED IN-MEMORY GCS TO BIGQUERY PIPELINE
# ==========================================
def run_gcs_to_bigquery_etl():
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import pandas as pd
    import io
    
    print("--- STARTING BYPASS GCS TO BIGQUERY PIPELINE ---")
    
    # 1. Stream from GCS
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_bytes = hook.download(
        bucket_name="project-repo-498812-rawdata", 
        object_name="ai_student_impact_dataset.csv"
    )
    
    df = pd.read_csv(io.BytesIO(file_bytes))
    print(f"Data successfully read into memory. Row count: {len(df)}")
    
    # 2. Write straight to BigQuery via Pandas GBQ (Bypasses regular API job creation limits)
    print("Writing dataframe straight into BigQuery dataset table...")
    df.to_gbq(
        destination_table="raw_data_dataset.ai_student_impact_table",
        project_id="project-repo-498812",
        if_exists="replace",
        progress_bar=False
    )
    print("Successfully pushed raw GCS stream data straight into BigQuery target table!")

# ==========================================
# AIRFLOW WORKFLOW DEFINITION
# ==========================================
with DAG(
    dag_id="ai_student_impact_gcs_to_bq_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    execute_etl = PythonOperator(
        task_id="run_in_memory_gcs_to_bq_etl",
        python_callable=run_gcs_to_bigquery_etl
    )