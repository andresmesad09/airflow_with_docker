from datetime import datetime, timedelta

from airflow.decorators import dag, task

dag_args = {
    'dag_id': 'dag_with_taskflow_api',
    'schedule': '@daily',
    'start_date': datetime(2022, 10, 17),
    'catchup': False,
    'description': 'DAG with Python Taskflow API',
    'default_args': {
        'owner': 'Andres M',
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }
}


@dag(**dag_args)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Andres',
            'last_name': 'Mesa'
        }

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} "
              f"and I am {age} years old!")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)


greet_dag = hello_world_etl()
