from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # noqa: E501
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator  # noqa: E501

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

from datetime import date

GCS_BUCKET_SOURCE = 'test-bucket-airflow'
GCS_BUCKET_DESTINATION = 'test-destination-bucket-airflow'
FILE_FORMAT = 'csv'
DATASET_TABLE = 'final-project-datapath-etl.test_data.rental_xcoms'
DATASET_TABLE_TARGET = 'final-project-datapath-etl.test_data.summary_rentals_xcoms'


dag_args = {
    'dag_id': '2_1_gcs_files_to_bigquery_hooks',
    'schedule': '@daily',
    'start_date': days_ago(7),
    'catchup': False,
    'description': 'DAG with Hooks',
    'default_args': {
        'owner': 'Andres M',
        'retries': 1,
    }
}


# ----
# HOOKS
# ----
def list_objects(bucket: str = None):
    return GCSHook().list(bucket)

# ----
# XCOMS
# ----
def move_objects(source_bucket: str = None, destination_bucket: str = None, prefix=None, **kwargs):
    # ti - task instance
    storage_objects = kwargs['ti'].xcom_pull(task_ids='list_files')
    for obj in storage_objects:
        dest_ob = f'{prefix}/{obj}' if prefix else obj
        # move from source to destination
        GCSHook().copy(source_bucket, obj, destination_bucket, dest_ob)
        # delete files from source
        GCSHook().delete(source_bucket, obj)


with DAG(**dag_args) as dag:
    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_objects,
        op_args=[GCS_BUCKET_SOURCE]
    )

    load_data = GCSToBigQueryOperator(
        task_id='load_data',
        bucket=GCS_BUCKET_SOURCE,
        source_objects=['*'],  # all objects
        source_format='CSV',
        skip_leading_rows=1,  # first row has header, ignore it
        # Big query fields
        destination_project_dataset_table=DATASET_TABLE,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default'
    )

    # second task - execute big query query
    query = f"""
    SELECT
        DATE_TRUNC(rental_date, WEEK) AS week,
        COUNT(*) AS total
    FROM `{DATASET_TABLE}`
    WHERE return_date IS NOT NULL
    GROUP BY 1
    """

    create_summary_table = BigQueryExecuteQueryOperator(
        task_id='create_summary_table',
        sql=query,
        destination_dataset_table=DATASET_TABLE_TARGET,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-east1',
        gcp_conn_id='google_cloud_default',
    )

    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_objects,
        op_kwargs={
            'source_bucket': GCS_BUCKET_SOURCE,
            'destination_bucket': GCS_BUCKET_DESTINATION,
            'prefix': date.today().strftime('%Y-%m-%d')
        }
    )

# DEPENDENCIES
list_files >> load_data >> create_summary_table >> move_files
