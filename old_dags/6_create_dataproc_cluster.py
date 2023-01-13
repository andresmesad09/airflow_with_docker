from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator  # noqa: F401
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.python import BranchPythonOperator
from random import uniform
from airflow.utils.task_group import TaskGroup

dag_args = {
    'dag_id': '6_create_dataproc_cluster',
    'schedule': '@daily',
    'start_date': days_ago(1),
    'catchup': False,
    'description': 'DAG with DataProc',
    'default_args': {
        'owner': 'Andres M',
        'retries': 1,
    },
}

# Branch Python Function
pyspark_files = ('avg_quant', 'avg_tincome', 'avg_uprice')
def generate_task_number(min_number=None, max_number=None):
    # if round(uniform(min_number, max_number)) % 2 == 0:
    if True:
        return 'odd_task'
    else:
        return [f'even_task.{x}' for x in pyspark_files]


with DAG(**dag_args) as dag:
    # ----
    # Create Cluster
    # ----
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='final-project-datapath-etl',
        cluster_name='sparkcluster-datapath-course',
        num_workers=2,
        # we need a bucket to store files from the cluster
        storage_bucket='spark-bucket-datapath-project',
        region='us-east1',
    )

    # ----
    # Branching
    # ----
    identify_num = BranchPythonOperator(
        task_id='identify_num',
        python_callable=generate_task_number,
        op_args=[1, 100],
    )

    # ----
    # odd_task
    # ----
    pyspark_job = {
        'reference': {
            'project_id': 'final-project-datapath-etl',
            'job_id': 'ODDTASK_dfa22fbf'
        },
        'placement': {
            'cluster_name': 'sparkcluster-datapath-course'
        },
        'labels': {
            'airflow-version': 'v2-4-1'
        },
        'pyspark_job': {
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
            'main_python_file_uri': 'gs://spark-bucket-datapath-project/odd_task/vars_stdp.py'
        }
    }

    odd_task = DataprocSubmitJobOperator(
        task_id='odd_task',
        project_id='final-project-datapath-etl',
        region='us-east1',
        job=pyspark_job,
        gcp_conn_id='google_cloud_default'
    )

    # ----
    # even_task
    # ----
    with TaskGroup(group_id='even_task') as even_task:
        pyspark_files = ['avg_quant', 'avg_tincome', 'avg_uprice']

        for subtask in pyspark_files:
            pyspark_subjob = {
                'reference': {
                    'project_id': 'final-project-datapath-etl',
                    'job_id': 'EVENTASK_dfa22fbf_{}'.format(subtask)
                },
                'placement': {
                    'cluster_name': 'sparkcluster-datapath-course'
                },
                'labels': {
                    'airflow-version': 'v2-4-1'
                },
                'pyspark_job': {
                    'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                    'main_python_file_uri': f'gs://spark-bucket-datapath-project/even_task/{subtask}.py'
                }
            }

            DataprocSubmitJobOperator(
                task_id=subtask,
                project_id='final-project-datapath-etl',
                region='us-east1',
                job=pyspark_subjob,
                gcp_conn_id='google_cloud_default'
            )

    # ----
    # DELETE CLUSTER
    # ----
    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id='delete_cluster',
    #     project_id='final-project-datapath-etl',
    #     cluster_name='sparkcluster-datapath-course',
    #     region='us-east1',
    #     trigger_rule='all_done'
    # )


# DEPENDENCIAS
create_cluster >> identify_num >> [odd_task, even_task]
