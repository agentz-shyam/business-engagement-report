import configparser

import requests

configFile = configparser.ConfigParser()


def businessStatus(env, businessID, businessAgentId):
    configFile.read(env + '_config.ini')
    api = "https://{}-business.agentz.ai/api/business/v1/subscriptions?businessId={}"
    headers = {"Authorization": configFile['analysis']['statusToken'], "Content-type": "application/json"}

    try:
        response = requests.get(url=api.format(env, businessID), headers=headers).json()
        for business in response['businessAgents']:
            if business['id'] == businessAgentId:
                return business["isEnabled"]
    except Exception as e:
        return False
