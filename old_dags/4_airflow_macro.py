from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import random

dag_args = {
    'dag_id': '4_airflow_with_macros',
    'schedule': '@daily',
    'start_date': days_ago(7),
    'catchup': False,
    'description': 'DAG with Macros',
    'default_args': {
        'owner': 'Andres M',
        'retries': 1,
    },
    # to use these variables in the operators
    'user_defined_macros': {
        'message': 'Mi primer DAG',
        'date': datetime.today().strftime('%Y-%m-%d')
    }
}

with DAG(**dag_args) as dag:
    bash_task = BashOperator(
        task_id="bash_task",
        # ref: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#airflow-variables-in-templates
        bash_command='echo "---> {{ message }} Fecha: $TODAY y ts es: {{ ts_nodash_with_tz }}"',
        env={'TODAY': '{{ date }}'}
    )

    def print_random_number(number=None):
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