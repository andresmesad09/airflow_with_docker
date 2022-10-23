
import uuid
# requirements
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteBatchOperator
from airflow.utils.task_group import TaskGroup
import pendulum
# --------------------------------------------------

# => job variables
uuid_code = str(uuid.uuid4())
today = pendulum.today('America/Lima')
gcp_project_id = Variable.get(key='gcp_project_id', default_var=None, deserialize_json=False)

# => dag arguments
dag_args = {
    'dag_id': 'replicate_dataproc_gcp',
    'schedule': '0 0 * * 1',
    'catchup': False,
    'user_defined_macros': {
        'gcp_project_id': gcp_project_id,
        'gcp_dataproc_region': 'us-central1',
        'gcp_jarfile_path': 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',
        'gcp_dataproc_sacc': 'datapath-test-account-service@datapath-lab.iam.gserviceaccount.com',
        'gcp_dataproc_network': 'taller-airflow-net',
        'gcp_dataproc_subnetwork': 'taller-airflow-subnet',
        'exec_date': today
    },
    'default_args': {
        'owner': 'Andres M',
        'start_date': today.add(days=-1)
    }
}
pyspark_paths = [
    {'uri': 'gs://taller-airflow-amesa/pyspark/src/script_1.py', 'args': ['Hello', 'World']},
    {'uri': 'gs://taller-airflow-amesa/pyspark/src/script_2.py', 'args': ['{{ ti.xcom_pull("operation_uuid") }}']}
]

with DAG(**dag_args) as dag:

    operation_uuid = PythonOperator(
        task_id='operation_uuid',
        python_callable=lambda: uuid_code
    )

    with TaskGroup(group_id='dataproc_batch_jobs') as dataproc_batch_jobs:
        for i, job_dict in enumerate(pyspark_paths):
            num = i + 1
            dataproc_create_batch = DataprocCreateBatchOperator(
                task_id=f'dataproc_create_batch_{num}',
                batch_id='{{ ti.xcom_pull("operation_uuid") }}-%s' % (num),
                batch={
                    'pyspark_batch': {
                        'main_python_file_uri': job_dict['uri'],
                        'args': job_dict['args'],
                        'jar_file_uris': ['{{ gcp_jarfile_path }}'],
                    },
                    'runtime_config': {
                        'properties': {
                            'spark.executor.instances': '2',
                            'spark.driver.cores': '4',
                            'spark.executor.cores': '4'
                        }
                    },
                    'environment_config': {
                        'execution_config': {
                            'service_account': '{{ gcp_dataproc_sacc }}',
                            'network_uri': '{{ gcp_dataproc_network }}',
                            'subnetwork_uri': '{{ gcp_dataproc_subnetwork }}'
                        }
                    },
                    'labels': {
                        'airflow': 'true',
                        'dev': 'true',
                        'replica': 'true'
                    }
                },
                retry=None,  # type: ignore
                project_id='{{ gcp_project_id }}',
                region='{{ gcp_dataproc_region }}',
                gcp_conn_id='google_cloud_default'
            )
            retrieve_job_id = PythonOperator(
                task_id=f'retrieve_job_id_{num}',
                python_callable=lambda item: item['name'].split('/')[-1],
                op_kwargs={
                    'item': dataproc_create_batch.output
                }
            )
            # dataproc_drop_batch = DataprocDeleteBatchOperator(
            #     task_id=f'dataproc_drop_batch_{num}',
            #     batch_id=retrieve_job_id.output,
            #     region='{{ gcp_dataproc_region }}',
            #     project_id='{{ gcp_project_id }}',
            #     gcp_conn_id='google_cloud_default'
            # )
        dataproc_create_batch >> retrieve_job_id  # type: ignore # >> dataproc_drop_batch

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "---> Bash task en Airflow $CODIGO. Fecha de hoy: $TODAY..."',
        env={
            'TODAY': '{{ exec_date }}',
            'CODIGO': '{{ ti.xcom_pull("operation_uuid") }}'
        },
        trigger_rule='all_success'
    )

# => dependencies
operation_uuid >> dataproc_batch_jobs >> bash_task  # type: ignore
