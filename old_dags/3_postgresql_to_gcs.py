from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator  # noqa: E501

GCS_BUCKET = 'raw-data-datapath'
GCS_OBJECT_PATH = 'rental'
SOURCE_TABLE_NAME = 'rental'
POSTGRES_CONNECTION_ID = 'dvd_rental_connection'
FILE_FORMAT = 'csv'

dag_args = {
    'dag_id': '3_postgresql_to_gcs',
    'schedule': '@daily',
    'start_date': days_ago(7),
    'catchup': False,
    'description': 'Third DAG - datapath',
    'default_args': {
        'owner': 'Andres M',
        'retries': 3,
    }
}

with DAG(**dag_args) as dag:
    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id='postgres_to_gcs',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql=f'SELECT * FROM {SOURCE_TABLE_NAME};',
        bucket=GCS_BUCKET,
        filename=f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME}.{FILE_FORMAT}',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
    )

postgres_to_gcs_task
