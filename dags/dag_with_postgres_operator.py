from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_args = {
    'dag_id': 'dag_with_postgres_operator',
    'schedule': '0 0 * * *',
    'start_date': datetime(2022, 10, 20),
    'catchup': False,
    'description': 'DAG with Postgres Operator',
    'default_args': {
        'owner': 'Andres M',
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_test',
        sql='''CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
        )'''
    )

    task2 = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id='postgres_test',
        # Ref: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        sql=""" DELETE FROM dag_runs WHERE dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}' """
    )

    task3 = PostgresOperator(
        task_id='insert_rows',
        postgres_conn_id='postgres_test',
        # Ref: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        sql="""INSERT INTO dag_runs (dt, dag_id) values ( '{{ ds }}', '{{ dag.dag_id }}' )
        """
    )
    task1 >> task2 >> task3
