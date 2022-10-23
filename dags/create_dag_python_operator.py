from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def greet(name: str, age: int) -> None:
    print(f'Hello, {name.title()}. You are {age} years old')


dag_args = {
    'dag_id': 'dag_with_python_operator',
    'schedule': '*/10 * * * *',  # every ten minutes
    'start_date': datetime(2022, 10, 17),
    'catchup': False,
    'description': 'DAG with Python Operator',
    'default_args': {
        'owner': 'Andres M',
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={
            'name': 'Andres',
            'age': 28
        }
    )

    task1  # type: ignore
