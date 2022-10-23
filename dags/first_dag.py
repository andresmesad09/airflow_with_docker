from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Andres M',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag',
    description='This is our first dag',
    start_date=datetime(2022, 10, 17),
    schedule='@daily',
    default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello World! This is the first task'
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey! I'm running later"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey! I'm running later. At the same time of task2"
    )

    task1 >> [task2, task3]  # type: ignore
