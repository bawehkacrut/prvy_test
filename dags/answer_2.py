from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from python_scripts.transform \
    import transformDataset as run_transform

"""
BEGIN CONFIGURATION
# """

# Airflow Config
default_args = {
    'owner': 'brilliant',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 1, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': [],
    'retries': 15,
    'retry_delay': timedelta(seconds=100)
}

"""
END CONFIGURATION
"""

dag = DAG(
    dag_id='answer_2',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    catchup=False
)

 
start_date_range = (
    '{{ execution_date.in_timezone("Asia/Jakarta")'
    '.start_of("day").date() }}'
)
end_date_range = (
    '{{ execution_date.add(days=1).in_timezone("Asia/Jakarta")'
    '.start_of("day").date()}}'
)
run_date_ds = (
    '{{ execution_date.in_timezone("Asia/Jakarta")'
    '.start_of("day").date() }}'
)


task_1 = PythonOperator(
        task_id='transform_dataset',
        python_callable=run_transform,
        op_kwargs={
            "source_path": '/opt/airflow/dags/dataset/user_activity_pretest.csv'
        },
        dag=dag
    )
task_1


