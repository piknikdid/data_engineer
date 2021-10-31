import json
import sys, os, re
import requests as rq
from datetime import datetime, date, timedelta
from airflow.models import BaseOperator
from .config import Config

class Http_Custom_Operator(BaseOperator):
    def __init__(self, config_path, date_st: datetime.date, app:str,  file_path, date_end: datetime.date = None, *args, **kwargs):
        super(Http_Custom_Operator, self).__init__(*args, **kwargs)
        self.config_path = config_path
        self.date_st = date_st
        self.app = app
        self.date_end = date_end
        self.file_path = file_path





    def execute(self, context ):
        conf = Config(self.config_path)
        conf = conf.get_config(self.app)


        main_url = conf['url']
        user = conf['username']
        pwd = conf['password']
        headers_auth = {'content-type': 'application/json'}

        token = self.get_token(main_url+conf['endpoint_auth'], user, pwd, headers_auth)
        token = "JWT " + token

        if self.date_end is not None:
            diff_days = (self.date_end - self.date_st).days
            for i in range(diff_days):
                headers_app = {'content-type': 'application/json', 'Authorization': token}
                date_api = str(self.date_st + timedelta(days=i))
                data = {"date": date_api}

                r = rq.get(main_url + conf['endpoint_app'], data=json.dumps(data), headers=headers_app)
                os.makedirs(os.path.join(self.file_path, date_api), exist_ok=True)
                with open(os.path.join(self.file_path, date_api) + '/data.json', 'w') as f:
                    json.dump(r.json(), f)
        else:
            headers_app = {'content-type': 'application/json', 'Authorization': token}
            date_api = str(self.date_st)
            data = {"date": date_api}
            r = rq.get(main_url + conf['endpoint_app'], data=json.dumps(data), headers=headers_app)
            os.makedirs(os.path.join(self.file_path, date_api), exist_ok=True)
            with open(os.path.join(self.file_path, date_api) + '/data.json', 'w') as f:
                json.dump(r.json(), f)





    def get_token(self, url: str, user: str, pwd: str, headers:dict):
        data = json.dumps({'username': user, 'password': pwd})
        r = rq.post(url=url, data=data, headers=headers, timeout=10)
        token = r.json()
        return token['access_token']

    def save_to_file(self, parameters: json, data: json):
        os.makedirs(self.file_system_path, exist_ok=True)
        directory_path = os.path.join(self.file_system_path, self.app_name)
        os.makedirs(directory_path, exist_ok=True)
        directory_path = os.path.join(directory_path, parameters[self.app_config['data parameter']])
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, self.app_name + '.json')
        with open(file_path, 'w') as f:
            json.dump(data, f)


