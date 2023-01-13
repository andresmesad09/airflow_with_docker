# ----
# Airflow
# ----
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# ----
# Providers
# ----
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ----
# Python
# ----
import uuid
from datetime import datetime

GCS_BUCKET = 'raw-data-datapath'
POSTGRES_CONNECTION_ID = 'dvd_rental_connection'
OWNER = Variable.get('owner')
INTERVAL = Variable.get('interval')
FILE_FORMAT = 'csv'
PROJECT_ID = 'final-project-datapath-etl'
DATASET_RAW = 'final-project-datapath-etl.final_dtlk_raw'
SPARK_BUCKET = 'spark-final-project-bucket'
CLUSTER_RAW = 'sparkcluster-datapath-raw'
CLUSTER_QUALITY = 'sparkcluster-datapath-quality'
REGION = 'us-east1'
GOOGLE_CONN = 'google_cloud_default'
SOURCE_TABLES = [
    'actor',
    'address',
    'category',
    'city',
    'country',
    'customer',
    'film',
    'film_actor',
    'film_category',
    'inventory',
    'language',
    'payment',
    'rental',
    'staff',
    'store'
]

dag_args = {
    'dag_id': '7_final_project_etl_v4',
    'schedule': INTERVAL,
    'start_date': days_ago(1),
    'catchup': False,
    'description': 'Final project DAG',
    'default_args': {
        'owner': OWNER,
        'retries': 1,
    }
}

def generate_uuid():
    return str(uuid.uuid4()).split('-')[-1]


uuid_run = generate_uuid()


with DAG(**dag_args) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "[Date: $TODAY]" with uuid: $UUID',
        env={'TODAY': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')), 'UUID': uuid_run}
    )

    with TaskGroup(group_id='move_tables_to_raw_bucket') as move_tables_to_raw_bucket:
        for table in SOURCE_TABLES:
            PostgresToGCSOperator(
                task_id=f'move_{table}_to_raw_bucket',
                postgres_conn_id=POSTGRES_CONNECTION_ID,
                sql=f'SELECT * FROM {table};',  # TODO: Execute specific query for each table.
                bucket=GCS_BUCKET,
                filename=f'{table}/{table}.{FILE_FORMAT}',
                export_format='csv',
                gzip=False,
                use_server_side_cursor=False,
            )

    with TaskGroup(group_id='create_tables_in_dtlk_raw') as create_tables_in_dtlk_raw:
        for table in SOURCE_TABLES:
            GCSToBigQueryOperator(
                task_id=f'load_{table}_to_raw_dtlk',
                bucket=f"{GCS_BUCKET}/{table}",
                source_objects=['*'],  # all objects in that bucket
                source_format='CSV',
                skip_leading_rows=1,  # first row has header, ignore it
                # Big query fields
                destination_project_dataset_table=f'{DATASET_RAW}.{table}',
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                gcp_conn_id=GOOGLE_CONN
            )

    with TaskGroup(group_id='raw_to_quality_layer') as raw_to_quality_layer:
        create_cluster_raw = DataprocCreateClusterOperator(
            task_id='create_cluster_raw',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_RAW,
            num_workers=2,
            # we need a bucket to store files from the cluster
            storage_bucket=SPARK_BUCKET,
            region=REGION,
        )

        pyspark_job_raw_to_qty = {
            'reference': {
                'project_id': PROJECT_ID,
                'job_id': f"raw_to_qty_{uuid_run}"
            },
            'placement': {
                'cluster_name': CLUSTER_RAW
            },
            'labels': {
                'airflow-version': 'v2-4-1'
            },
            'pyspark_job': {
                # this is given by Google
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                # Python script that uses PySpark
                'main_python_file_uri': 'gs://spark-final-project-bucket/raw_to_quality.py'
            }
        }

        tables_from_raw_to_quality = DataprocSubmitJobOperator(
            task_id='tables_from_raw_to_quality',
            project_id=PROJECT_ID,
            region=REGION,
            job=pyspark_job_raw_to_qty,
            gcp_conn_id=GOOGLE_CONN
        )

        delete_cluster_raw = DataprocDeleteClusterOperator(
            task_id='delete_cluster_raw',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_RAW,
            region=REGION,
            trigger_rule='all_done'
        )

        create_cluster_raw >> tables_from_raw_to_quality >> delete_cluster_raw

    with TaskGroup(group_id='quality_to_access_layer') as quality_to_access_layer:
        create_cluster_quality = DataprocCreateClusterOperator(
            task_id='create_cluster_quality',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_QUALITY,
            num_workers=2,
            # we need a bucket to store files from the cluster
            storage_bucket=SPARK_BUCKET,
            region=REGION,
        )

        pyspark_job_qty_to_acess = {
            'reference': {
                'project_id': PROJECT_ID,
                'job_id': f"qty_to_access_{uuid_run}"
            },
            'placement': {
                'cluster_name': CLUSTER_QUALITY
            },
            'labels': {
                'airflow-version': 'v2-4-1'
            },
            'pyspark_job': {
                # this is given by Google
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                # Python script that uses PySpark
                'main_python_file_uri': 'gs://spark-final-project-bucket/quality_to_access.py'
            }
        }

        tables_from_quality_to_access = DataprocSubmitJobOperator(
            task_id='tables_from_quality_to_access',
            project_id=PROJECT_ID,
            region=REGION,
            job=pyspark_job_qty_to_acess,
            gcp_conn_id=GOOGLE_CONN
        )

        delete_cluster_qty = DataprocDeleteClusterOperator(
            task_id='delete_cluster_qty',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_QUALITY,
            region=REGION,
            trigger_rule='all_done'
        )

        create_cluster_quality >> tables_from_quality_to_access >> delete_cluster_qty


bash_task >> move_tables_to_raw_bucket >> create_tables_in_dtlk_raw >> raw_to_quality_layer >> quality_to_access_layer
