from datetime import datetime, date
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='backup_table_postgre',
    description='sample',
    start_date=datetime(2021,11, 10),
    schedule_interval='@daily'
)

tables = ['aisles', 'clients', 'departments', 'orders', 'products']
tasks = []


main_table = 'orders'

dummy_1 = DummyOperator(task_id='first', dag=dag)
dummy_2 = DummyOperator(task_id='last', dag=dag)

for table in tables:
    tasks.append(
        BackupPostgreOperator(
            task_id = f'{table}_backup_id',
            conf_path = os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml'),
            file_path = os.path.join('.', 'data', 'dshop_backup'),
            app = 'pg_dshop',
            table_name = table
        )

    )

dummy_1>>tasks>>dummy_2