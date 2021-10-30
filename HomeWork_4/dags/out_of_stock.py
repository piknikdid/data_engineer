from datetime import datetime
import os
from common.config import Config
from airflow import DAG
from common.http_custom_operator import Http_Custom_Operator

EXEC_DATE = '{{ ds }}'

dag = DAG(
    dag_id='out_of_stock_service',
    description='sample',
    start_date=datetime(2021,10,25, 19,40),
    end_date=datetime(2021,10,30, 19,40),
    schedule_interval='@daily'
)

t1 = Http_Custom_Operator(
    task_id='out_of_stock',
    dag=dag,
    configuration=Config(os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml')),
    date_st=EXEC_DATE,
    app='app',
    file_path = os.path.join('.', 'data')
                           )

t1