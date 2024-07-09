from airflow import DAG
import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable


config = Variable.get("hello", deserialize_json=True)
dag = DAG('postgres', description='postgres',
          schedule_interval=config['time_start'],
          start_date=datetime.datetime(2021, 11, 7), catchup=False)
start_step = DummyOperator(task_id="start_step", dag=dag)
hello_step = PostgresOperator(task_id="insert_step",
                            sql="SELECT * FROM source_data.отрасли",
                            postgres_conn_id='test_conn',
                            dag=dag)
end_step = DummyOperator(task_id="end_step", dag=dag)

start_step >> hello_step >> end_step


