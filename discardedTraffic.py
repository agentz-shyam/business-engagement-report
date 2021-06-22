import configparser
import datetime as dt
import json

import pandas as pd
import pymysql
from pytz import timezone


def report(start, end, timeZone, businessAgentId, businessId, env):
    configFile = configparser.ConfigParser()
    configFile.read(env + '_config.ini')

    try:
        agentzFingerprint = list(map(int, configFile["analysis"]["fingerprint"].split(',')))
    except:
        agentzFingerprint = []

    db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["conversationDB"])

    my_timezone = timezone(timeZone)

    startDate = my_timezone.localize(dt.datetime.strptime(start + ' 00:00:00', "%Y-%m-%d %H:%M:%S")).astimezone(
        timezone('UTC'))
    startDate = startDate.strftime("%Y-%m-%d %H:%M:%S")

    endDate = my_timezone.localize(dt.datetime.strptime(end + ' 23:59:59', "%Y-%m-%d %H:%M:%S")).astimezone(
        timezone('UTC'))
    endDate = endDate.strftime("%Y-%m-%d %H:%M:%S")

    conversationQuery = "select * from  conversation where business_agent_id = '{}' and business_id = '{}' and " \
                        "created_date  >='{}' and created_date <='{}' order by created_date asc"
    conversationDF = pd.read_sql_query(conversationQuery.format(businessAgentId, businessId, startDate, endDate), db)

    if len(conversationDF['id']) > 0:
        try:
            messageQuery = "select * from message where conversation_id in ({}) and  created_date  >='{}' and  " \
                           "created_date <='{}' order by created_date asc"
            messagequeryDF = pd.read_sql_query(
                messageQuery.format(', '.join('"' + item + '"' for item in conversationDF['id']), startDate, endDate),
                db)
        except Exception as e:
            print(e)
            return 0, 0

        try:
            engageQuery = "select event_type, conversation_id from conversation_event where conversation_id in ({}) " \
                          "and business_agent_mapping_id = '{}' and (event_type='USER_INPUT' or (event_type=" \
                          "'CHAT_WINDOW_EVENT' and event_value='Phone_Call'))"
            engageQueryDF = pd.read_sql_query(engageQuery.format(', '.join('"' + item + '"' for item in
                                                                           conversationDF['id']), businessAgentId), db)
        except Exception as e:
            print(e)
            return 0, 0
        finally:
            db.close()

        sessionId = []
        sessionID = []
        intentName = []
        engagement = []

        for idx, row in conversationDF.iterrows():
            if "AdsBot-Google" in row[19] or "YandexBot" in row[19] or row[19] == "{}":
                continue

            ans = json.loads(row[19])
            try:
                fingerPrint = ans['fingerPrint']
            except:
                fingerIndices = [i for i, s in enumerate(ans) if 'fingerPrint' in s]
                fingerPrint = ans[fingerIndices[0]].split(":")[-1]

            if fingerPrint in agentzFingerprint:
                continue

            sessionId.append(row[0])

        def intentDetails(data):

            ans = json.loads(data)
            intent = 'NA'

            if ans["sender"]["senderType"] == "CONSUMER" and ans["payload"]["payloadType"] == "CHAT_WINDOW_EVENT":
                try:
                    intent = ans["payload"]["intentName"]
                except:
                    intent = 'NA'

            elif ans["sender"]["senderType"] == "CONSUMER" and ans["payload"]["payloadType"] == "TEXT":

                try:
                    intent = ans["payload"]["text"]
                except:
                    intent = 'NA'

            elif ans["sender"]["senderType"] == "BOT":
                try:
                    intent = ans["payload"]["intentName"]
                except:
                    intent = 'NA'

            return intent, ans["sessionId"]

        for session in sessionId:
            tempDF = messagequeryDF[messagequeryDF['conversation_id'] == session]
            response = list(map(intentDetails, tempDF['message']))
            for resp in response:
                intentName.append(resp[0])
                sessionID.append(resp[1])
                engagedSession = engageQueryDF[engageQueryDF["conversation_id"] == session]
                if len(engagedSession['event_type']) > 0:
                    engagement.append("engaged")
                else:
                    engagement.append("not engaged")

        messageDF = dict()
        messageDF["Session Id"] = sessionID
        messageDF["Intent"] = intentName
        messageDF["Engagement"] = engagement

        DF = pd.DataFrame(messageDF)

        try:

            dataDF2 = DF[DF["Engagement"] == "not engaged"]
            df5 = dataDF2[dataDF2["Intent"] == 'FullWindow_Close']

            df4 = dataDF2[dataDF2["Session Id"].isin(list(set(df5["Session Id"]))) == False]
            nowindwClose_noEngagement = len(list(set(df4["Session Id"])))
            totalSessions = len(list(set(DF["Session Id"])))
            return totalSessions, nowindwClose_noEngagement

        except:
            return 0, 0
    else:
        return 0, 0
