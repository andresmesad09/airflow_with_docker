from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
from random import random


dag_args = {
    'dag_id': '1_my_first_dag',
    'schedule': '@daily',
    'start_date': datetime(2022, 12, 25),
    'catchup': False,
    'description': 'First DAG - datapath',
    'default_args': {
        'owner': 'Andres M',
        'retries': 3,
    }
}

with DAG(**dag_args) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "---> Fecha: $TODAY"',
        env={'TODAY': str(datetime.today())}
    )

    def print_random_number(number=None, other=None):
        for i in range(number):
            print(f'Random number {i+1}: {random()}')

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_random_number,
        op_kwargs={'number': 10}
    )

    ls_task = BashOperator(
        task_id="list_files",
        bash_command='ls',
    )

bash_task >> python_task >> ls_task
