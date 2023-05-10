# xcom are used to share data between the tasks
# we can't share the large file from the xcom such as pandas dataframe

from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import airflow.utils.dates

default_args={
    'owner':'ritik jain',
}
def wish(name):
    print(f'hello {name}')
    return name

def display_name(ti):
    name=ti.xcom_pull(task_ids='say_hello')
    print(name)

def set_xcom(ti):
    ti.xcom_push(key='first_name',value='ritik')
    ti.xcom_push(key='last_name',value='jain')

def get_multiple_xcom(ti):
    first_name=ti.xcom_pull(task_ids='set_xcom',key='first_name')
    last_name=ti.xcom_pull(task_ids='set_xcom',key='last_name')
    print(first_name)
    print(last_name)

with DAG (
    dag_id='python_operator',
    default_args=default_args,
    start_date=airflow.utils.dates.days_ago(3)

) as dag:
    task1=PythonOperator(
        task_id='say_hello',
        python_callable=wish,
        op_kwargs={"name":"ritik"}
    )

    task2=PythonOperator(
        task_id='check_xcom_for_name',
        python_callable=display_name
    )

    task3=PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom
    )

    task4=PythonOperator(
        task_id='print_multiple_xcom',
        python_callable=get_multiple_xcom
    )
    task1>>task2>>task3>>task4