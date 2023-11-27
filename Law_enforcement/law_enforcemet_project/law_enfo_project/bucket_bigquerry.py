from airflow import DAG
from datetime import datetime, timedelta
from web.operators.grouppro import WebToGCSHKOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    'gcp_conn_id': 'google_cloud_conn',
    'dbt_cloud_conn_id': 'dbt_cloud_conn',
    "account_id": '221428',
    
}

# Create your DAG
with DAG('law_enforcement_data_fetch_dag', default_args=default_args, schedule_interval="0 6 * * *") as dag:
    start = DummyOperator(task_id='start')

    # Fetch and store data in GCS
    fetch_and_store_data = WebToGCSHKOperator(
        task_id='download_to_gcs',
        gcs_bucket_name='alt_new_bucket',
        gcp_conn_id= 'google_cloud_conn',
        email=["tunwaju@gmail.com"],
        gcs_object_name='law_enforcement/law_Enforcement_Dispatched_Calls_for_Service.csv',
        api_endpoint='https://data.sfgov.org/resource/gnap-fj3t.json',
        api_headers={
            "X-App-Token": 'H5iB0k6uYvlqi0k6yobmF0xmY',
            "X-App-Secret": 'rAP6bbNMln-aNQmnd94NlSTCiklYhOhfO3B3',
        },
        api_params={
            "$limit": 2000,
        },
    )

    # Push data from GCS to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id='upload_to_bigquery',
        source_objects=['law_enforcement/law_Enforcement_Dispatched_Calls_for_Service.csv'],
        destination_project_dataset_table='poetic-now-399015.Law_Enforcement_Dispatched_Calls_for_Service.Law_Enforcemen_table',
        schema_fields=[],  # Define schema fields if needed
        skip_leading_rows=1,
        source_format='CSV',
        field_delimiter=',',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',  # Change to your desired write disposition
        autodetect=True, 
        email=["tunwaju@gmail.com"],
        bucket ="alt_new_bucket",
    )

    trigger_dbt_cloud_run_job = DbtCloudRunJobOperator(
        task_id="dbt_run_job",
        job_id=464317,
        check_interval=10,
        timeout=300,
        email=["tunwaju@gmail.com"],
        # dbt_conn_id="dbt_cloud_conn"

    )


    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> fetch_and_store_data >> upload_to_bigquery >> trigger_dbt_cloud_run_job >> end
