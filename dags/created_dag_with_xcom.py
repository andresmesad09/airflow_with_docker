from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    name = first_name + ' ' + last_name
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f'Hello, {name.title()}. You are {age} years old')


def get_name(ti):
    ti.xcom_push(key='first_name', value='Andres')
    ti.xcom_push(key='last_name', value='Mesa')


def get_age(ti):
    ti.xcom_push(key='age', value=30)


dag_args = {
    'dag_id': 'dag_with_python_operator_and_xcom',
    'schedule': '*/10 * * * *',  # every ten minutes
    'start_date': datetime(2022, 10, 17),
    'catchup': False,
    'description': 'DAG with Python Operator and XCom',
    'default_args': {
        'owner': 'Andres M',
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:

    task1 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
    )
    [task1, task3] >> task2  # type: ignore
