from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'ritik',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def greet(name):
    print(f'hello Mr {name}')
    return name

def get_name(ti):
    name=ti.xcom_pull(task_ids='python_print_statement')
    print(name)

with DAG(
    dag_id='python_operator_dag',
    description='this is the python dag. from coder2j',
    start_date=datetime(2023,4,21),
    schedule_interval='@once',
    default_args=default_args
) as dag:
    task1=PythonOperator(
        task_id='python_print_statement',
        #python_callable=lambda: print('hello world')
        python_callable=greet,
        op_kwargs={'name':'Ritik Jain'}
    )

    task2=PythonOperator(
        task_id='retuen_name',
        python_callable=get_name
    )
    task1>>task2



    
