from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import pandas as pd
from google.cloud import storage

# --- 1. EXTRACT TASK ---
def extract_teen_data():
    print("Starting data extraction for Teen Mental Health dataset")
    
    # Inside Docker, the dags folder is mounted at /opt/airflow/dags/
    input_file_path = "/opt/airflow/dags/Teen_Mental_Health_Dataset.csv"
    
    # Read the dataset
    df = pd.read_csv(input_file_path)
    
    # Save a raw copy to the container's temporary folder
    df.to_csv("/tmp/raw_teen_mental_health.csv", index=False)
    print("Data extraction completed successfully")


# --- 2. TRANSFORM TASK ---
def transform_teen_data():
    print("Starting data transformation and aggregation")
    
    df = pd.read_csv("/tmp/raw_teen_mental_health.csv")
    df = df.drop_duplicates()
    
    # Create an aggregated summary: average hours, stress, and scores per platform and gender
    summary_df = df.groupby(["platform_usage", "gender"]).agg({
        "daily_social_media_hours": "mean",
        "sleep_hours": "mean",
        "stress_level": "mean",
        "anxiety_level": "mean",
        "academic_performance": "mean"
    }).reset_index()
    
    # Round metrics to 2 decimal places for a clean report
    summary_df = summary_df.round(2)
    
    # Save the transformed summary report
    summary_df.to_csv("/tmp/teen_platform_summary.csv", index=False)
    print("Data transformation completed successfully")

import os  # Make sure this is at the top of your function if not at the top of the file

# --- 3. LOAD TASK ---
def load_teen_data():
    print("Starting data load to Google Cloud Storage using implicit credentials")
    
    # Forcefully clear the environment variables causing the file lookup block
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    
    try:
        # Pass project parameter directly to stop Google from scanning files for it
        client = storage.Client(project="project-repo-498812")
        bucket = client.bucket("project-repo-498812-rawdata")
        
        # Saving it under a dedicated folder path in your bucket
        blob = bucket.blob("teen_mental_health/platform_summary.csv")
        blob.upload_from_filename("/tmp/teen_platform_summary.csv")
        
        print("Summary report successfully uploaded to GCS!")
        
    except Exception as e:
        print(f"GCS Upload failed: {e}")
        raise

# --- DAG CONFIGURATION ---
with DAG(
    dag_id="teen_mental_health_pipeline",  # Unique ID for the new dashboard row
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",                    # Schedules it weekly (or change to @daily)
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_teen_data",
        python_callable=extract_teen_data
    )

    transform_task = PythonOperator(
        task_id="transform_teen_data",
        python_callable=transform_teen_data
    )

    load_task = PythonOperator(
        task_id="load_teen_data",
        python_callable=load_teen_data
    )

    # Set dependency chain
    extract_task >> transform_task >> load_task
