import sys, os, re
from .config import Config
from airflow.models import BaseOperator
import psycopg2

class BackupPostgreOperator(BaseOperator):
    def __init__(self, conf_path, file_path, app, table_name, *args, **kwargs):
        super(BackupPostgreOperator, self).__init__(*args, **kwargs)
        self.conf_path = conf_path
        self.file_path = file_path
        self.app = app
        self.table_name = table_name


    def execute(self, context):
        pg_creds = Config(self.conf_path)
        pg_creds = pg_creds.get_config(self.app)

        sql = f"COPY {self.table_name}  TO STDOUT WITH HEADER CSV"

        os.makedirs(self.file_path, exist_ok=True)

        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
            with open(file=os.path.join(self.file_path, f'{self.table_name}.csv'), mode='w') as csv_file:
                cursor.copy_expert(sql, csv_file)
