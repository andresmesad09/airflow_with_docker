from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # noqa: E501
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator  # noqa: E501


GCS_BUCKET = 'raw-data-datapath'
GCS_OBJECT_FOLDER = 'rental'
FILE_FORMAT = 'csv'
DATASET_TABLE = 'final-project-datapath-etl.test_data.rental'
DATASET_TABLE_TARGET = 'final-project-datapath-etl.test_data.summary_rentals'


dag_args = {
    'dag_id': '2_gcs_files_to_bigquery',
    'schedule': '@daily',
    'start_date': days_ago(7),
    'catchup': False,
    'description': 'Second DAG - datapath',
    'default_args': {
        'owner': 'Andres M',
        'retries': 1,
    }
}

with DAG(**dag_args) as dag:
    # first task - load data from GCS to BigQuery
    load_data = GCSToBigQueryOperator(
        task_id='load_data',
        bucket=f"{GCS_BUCKET}/{GCS_OBJECT_FOLDER}",
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

# DEPENDENCIES
load_data >> create_summary_table
