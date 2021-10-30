import json
import sys, os, re
import requests as rq
from datetime import datetime, date, timedelta
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class Http_Custom_Operator(BaseOperator):
    def __init__(self, configuration, date_st: datetime.date, app:str,  file_path, date_end: datetime.date = None):
        super(Http_Custom_Operator, self).__init__(*args, **kwargs)
        self.configuration = configuration,
        self.date_st = date_st,
        self.app = app,
        self.date_end = date_end,
        self.file_path = file_path





    def execute(self):
        conf = self.configuration.get_config(self.app)

        main_url = conf['url']
        user = conf['username']
        pwd = conf['password']
        headers_auth = {'content-type': 'application/json'}

        token = get_token(main_url+config_data['endpoint_auth'], user, pwd, headers_auth)
        token = "JWT " + token

        if self.date_end is not None:
            diff_days = (self.date_end - self.date_st).days
            for i in range(diff_days):
                headers_app = {'content-type': 'application/json', 'Authorization': token}
                date_api = str(self.date_st + timedelta(days=i))
                data = {"date": date_api}

                r = rq.get(main_url + config_data['endpoint_app'], data=json.dumps(data), headers=headers_app)
                os.makedirs(os.path.join(self.file_path, date_api), exist_ok=True)
                with open(os.path.join(self.file_path, date_api) + '/data.json', 'w') as f:
                    json.dump(r.json(), f)
        else:
            headers_app = {'content-type': 'application/json', 'Authorization': token}
            date_api = str(self.date_st)
            data = {"date": date_api}
            r = rq.get(main_url + config_data['endpoint_app'], data=json.dumps(data), headers=headers_app)
            os.makedirs(os.path.join(self.file_path, date_api), exist_ok=True)
            with open(os.path.join(self.file_path, date_api) + '/data.json', 'w') as f:
                json.dump(r.json(), f)





    def get_token(url: str, user: str, pwd: str, headers:dict):
        data = json.dumps({'username': user, 'password': pwd})
        r = rq.post(url=url, data=data, headers=headers, timeout=10)
        token = r.json()
        return token['access_token']


