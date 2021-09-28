import json
import sys, os, re
import requests as rq
from config import Config
from datetime import datetime, date, timedelta

def check_params():
    if re.match("^20[0-9][0-9](-)(0[1-9]|1[0-2])(-)(0[1-9]|1[0-9]|2[0-9]|3[0-1])$", sys.argv[1]) is None\
            and re.match("^20[0-9][0-9](-)(0[1-9]|1[0-2])(-)(0[1-9]|1[0-9]|2[0-9]|3[0-1])$", sys.argv[2]) is None:
        raise Exception('Input parameters must be date like yyyy-mm-dd')


def get_token(url: str, user: str, pwd: str, headers:dict):
    data = json.dumps({'username': user, 'password': pwd})
    r = rq.post(url=url, data=data, headers=headers, timeout=10)
    token = r.json()
    return token['access_token']


def app(config_data:dict, dates=False):

    main_url = config_data['url']
    user =  config_data['username']
    pwd = config_data['password']
    headers_auth = {'content-type': 'application/json'}

    directory_path = config_data['directory']
    os.makedirs(directory_path, exist_ok=True)

    token = get_token(main_url+config_data['endpoint_auth'], user, pwd, headers_auth)
    token = "JWT " + token


    if dates:
        diff_days = (date_to-date_from).days
        for i in range(diff_days):
            headers_app = {'content-type': 'application/json', 'Authorization': token}
            date_api = str(date_from + timedelta(days=i))
            data = {"date": date_api}

            r = rq.get(main_url+config_data['endpoint_app'], data=json.dumps(data), headers=headers_app)
            os.makedirs(os.path.join(directory_path, date_api) , exist_ok=True)
            with open(os.path.join(directory_path, date_api) + '/data.json', 'w') as f:
                json.dump(r.json(), f)
    else:
        headers_app = {'content-type': 'application/json', 'Authorization': token}
        date_api = str(date.today())
        data = {"date": date_api}
        r = rq.get(main_url + config_data['endpoint_app'], data=json.dumps(data), headers=headers_app)
        os.makedirs(os.path.join(directory_path, date_api), exist_ok=True)
        with open(os.path.join(directory_path, date_api) + '/data.json', 'w') as f:
            json.dump(r.json(), f)


if __name__=='__main__':
    configuration = Config(path='config.yaml')
    conf = configuration.get_config('app')
    if len(sys.argv) > 1:
        check_params()
        date_from = date.fromisoformat(sys.argv[1])
        date_to = date.fromisoformat(sys.argv[2])
        app(conf, dates=True)
    else:
        app(conf)

