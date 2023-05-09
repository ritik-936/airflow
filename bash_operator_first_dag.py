from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner':'ritik',
    'retries':3,
    'retry_delay':timedelta(minutes=2),
}
with DAG(
    dag_id='bash_operator',
    description='this is our first dag',
    start_date=datetime(2023,4,21),
    schedule_interval='@once',
    default_args=default_args
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo hello world this is our first dag'
    )

    task2=BashOperator(
        task_id='second_task',
        bash_command='echo hello this is second task i am run after the first task'
    )

    task3=BashOperator(
        task_id='third_task',
        bash_command='echo hello this is third task running after 1st task'
    )
    task1>>task2
    task1>>task3
    # task1>>[task2,task3]