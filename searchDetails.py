import json


class searchDetails:
    def __init__(self, configFile):
        self.names = configFile['analysis']['searchItems'].split(',')
        self.language = "EN_US"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
