from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import os

# Constants
API_URL = "https://api.openf1.org/v1/drivers"
BUCKET_NAME = "rushi-us-east1"
S3_KEY = "f1_drivers/drivers_data.json"

# Function to extract data from API
def extract_data(**kwargs):
    response = requests.get(API_URL)
    response.raise_for_status()
    
    data = response.json()
    
    # Save data to a local file
    file_path = "/tmp/drivers_data.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

# Function to upload data to S3
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='extract_data', key='file_path')
    
    hook = S3Hook(aws_conn_id='aws_default')
    
    hook.load_file(
        filename=file_path,
        key=S3_KEY,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    os.remove(file_path)  # Clean up local file

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_f1_drivers_to_s3",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["F1", "API", "S3"]
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True
    )

    extract >> upload
