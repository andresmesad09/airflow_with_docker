from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Andres M',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_backfill_v02',
    default_args=default_args,
    start_date=datetime(2022, 10, 15),  # to the past
    schedule_interval='@daily',
    catchup=False
    # ----
    # We can set this to True but we can:
    # 1. In airflow scheduler: docker exec -it <scheduler_container> bash
    # 2. airflow dags backfill -s START_DATE -e END_DATE <dag_id>
    # 3. exit
    # ----
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!'
    )
