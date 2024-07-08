import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from utils.constants import tables, source_schema, dest_schema
from sql.tables_init import tables_init, schema_init
from utils.helpers import fullName
from etls.copying_tables import copying_tables


default_args = {
    'owner': 'gena',
    'start_date': datetime(2024, 7, 1)
}


dag = DAG(
    'data_to_ods',
    default_args=default_args,
    description='Transfer data from source to ods',
    schedule_interval=None,  
    catchup=False,           
)


start_dag = EmptyOperator(
    task_id='start_dag',
    dag=dag
)


insertion_task=[]
for table in tables:
    copying_task = PythonOperator(
        task_id=f'copying_{table}',
        python_callable=copying_tables,
        op_kwargs={
            'src_schema': 'source_data', 
            'dest_schema': 'g_ods_test', 
            'table': table,
        },
        dag=dag
    )
    insertion_task.append(copying_task)


insertion_end = EmptyOperator(
    task_id='insertion_end',
    dag=dag
)


start_dag >> insertion_task >> insertion_end