from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

from datetime import datetime

from taxidataset import format_to_parquet, upload_to_gcs


PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
# link https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

DATASET_FILE = 'taxi+_zone_lookup.csv'
DATASET_URL = f'https://s3.amazonaws.com/nyc-tlc/misc/{DATASET_FILE}'
PATH_TO_LOCAL_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
PARQUET_FILE = DATASET_FILE.replace('.csv', '.parquet')


zones_workflow = DAG(
    dag_id='IngestionZonesToLake',
    schedule_interval='@once',
    start_date=datetime(2021, 2, 5)
)

with zones_workflow:

    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'curl -sS {DATASET_URL} > {PATH_TO_LOCAL_HOME}/{DATASET_FILE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id='format_to_parquet_task',
        python_callable=format_to_parquet,
        op_kwargs={
            'src_file': f'{PATH_TO_LOCAL_HOME}/{DATASET_FILE}',
        },
    )

    local_to_gcs_task = PythonOperator(
            task_id='local_to_gcs_task',
            python_callable=upload_to_gcs,
            op_kwargs={
                'bucket': BUCKET,
                'object_name': f'raw/{PARQUET_FILE}',
                'local_file': f'{PATH_TO_LOCAL_HOME}/{PARQUET_FILE}',
            },
        )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task
