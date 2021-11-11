import yaml

class Config:
    def __init__(self, path):
        with open(path, 'r') as f:
            self.conf = yaml.load(f)        

    def get_config(self, application):
        return self.conf.get(application)