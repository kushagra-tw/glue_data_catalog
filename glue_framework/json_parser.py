import json

class JsonParser:

    def parse_config(self,config_path):
        with open(config_path) as file:
             return json.loads(file.read())
