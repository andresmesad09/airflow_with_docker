from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import random

# to interact with variables
from airflow.models import Variable

# ----
# Variables
# ----
OWNER = Variable.get('owner')
MESSAGE = Variable.get('message')
INTERVAL = Variable.get('interval')


dag_args = {
    'dag_id': '5_airflow_variables',
    'schedule': INTERVAL,
    'start_date': days_ago(7),
    'catchup': False,
    'description': 'DAG with Variables',
    'default_args': {
        'owner': OWNER,
        'retries': 1,
    },
    # to use these variables in the operators
    'user_defined_macros': {
        'message': MESSAGE,
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

bash_task >> python_task