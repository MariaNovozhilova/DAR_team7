import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scripts import source_to_ods


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 28)
}

dag = DAG(
    'source_to_ods',
    default_args=default_args,
    description='Transfer data from source to ods',
    schedule_interval=None,  
    catchup=False,           
)

start_step = EmptyOperator(
    task_id='start_step',
    dag=dag
)


copy_step = PythonOperator(
        task_id='copy_step',
        python_callable=source_to_ods.move_data,
        dag=dag,
    )

end_step = EmptyOperator(
    task_id='end_step',
    dag=dag
)

start_step >> copy_step >> end_step