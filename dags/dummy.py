from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dummy_dag_example',
    default_args=default_args,
    description='A simple dummy DAG example',
    schedule_interval=timedelta(days=1),  # DAG này chạy hàng ngày
)

# Định nghĩa các task trong DAG
start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Thiết lập chuỗi tác vụ (task dependencies)
start_task >> end_task
