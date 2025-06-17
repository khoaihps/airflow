from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def sample_task():
    print("This is a sample task")

with DAG(
    dag_id="health_check",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    task1 = PythonOperator(
        task_id="task1",
        python_callable=sample_task,
    )

    end = EmptyOperator(task_id="end")

    start >> task1 >> end
