import datetime as dt
import json

import pandas as pd
import pymysql
from pytz import timezone

from deviceDiscarded import report


class deviceSessions:
    def __init__(self, configFile):
        self.configFile = configFile
        self.db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                  passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["conversationDB"])
        self.cur = self.db.cursor()

    def business_id(self, timezones, Date, start_date, businessAgentId):
        df = dict()
        desktop = []
        mobiles = []
        desktop_engagement = []
        mobile_engagement = []
        desktop_discarded = []
        mobile_discarded = []

        my_timezone = timezone(timezones)

        self.date = my_timezone.localize(dt.datetime.strptime(str(start_date) + ' 00:00:00', "%Y-%m-%d %H:%M:%S")
                                         ).astimezone(timezone('UTC'))
        self.endDate = my_timezone.localize(dt.datetime.strptime(str(Date) + ' 00:00:00', "%Y-%m-%d %H:%M:%S").replace(
            hour=23, minute=59, second=59)).astimezone(timezone('UTC'))

        query = "SELECT  * FROM `demo2-consumer-service`.conversation where business_agent_id='{}'" \
                " AND created_date >= '{}' AND created_date <= '{}'"

        engageQuery = "select * from conversation_event where business_agent_mapping_id='{}' and " \
                      "conversation_id in ({}) and (event_type='USER_INPUT' or (event_type='CHAT_WINDOW_EVENT' and " \
                      "event_value='Phone_Call'))"

        loadedSessions = 'select * from `demo2-consumer-service`.message where conversation_id in ({})'

        data = pd.read_sql_query(query.format(businessAgentId, self.date, self.endDate), self.db)

        if len(data['id']) > 0:
            loadedSessionsData = pd.read_sql_query(loadedSessions.format(', '.join('"' + item + '"' for item in
                                                                                   data['id'])), self.db)
            mobileSessions = []
            desktopSessions = []
            botSessions = []
            for convId, browser in zip(data['id'], data['browser_info']):
                if convId in list(loadedSessionsData['conversation_id'].unique()):
                    temp = json.loads(browser)
                    ua = temp['browserData']['ua']
                    if "AdsBot-Google" in ua or "YandexBot" in ua:
                        botSessions.append(convId)
                    elif temp["isMobile"] or temp["isIpad"]:
                        mobileSessions.append(convId)
                    else:
                        desktopSessions.append(convId)
            eventData = pd.read_sql_query(engageQuery.format(businessAgentId, ', '.join('"' + item + '"' for
                                                                                        item in data['id'])),
                                          self.db)
            desktopSessionsEngagement = 0
            for session in desktopSessions:
                engageDetails = eventData[eventData['conversation_id'] == session]
                if not engageDetails.empty:
                    desktopSessionsEngagement += 1

            mobileSessionsEngagement = 0
            for session in mobileSessions:
                engageDetails = eventData[eventData['conversation_id'] == session]
                if not engageDetails.empty:
                    mobileSessionsEngagement += 1

            desktop.append(len(desktopSessions))
            mobiles.append(len(mobileSessions))
            if len(desktopSessions) > 0:
                desktop_discarded.append(report(desktopSessions, start_date, Date, timezones, businessAgentId,
                                                self.configFile))
            else:
                desktop_discarded.append(0)

            if len(mobileSessions) > 0:
                mobile_discarded.append(report(mobileSessions, start_date, Date, timezones, businessAgentId,
                                               self.configFile))
            else:
                mobile_discarded.append(0)
            desktop_engagement.append(desktopSessionsEngagement)
            mobile_engagement.append(mobileSessionsEngagement)
        else:
            desktop.append(0)
            mobiles.append(0)
            desktop_discarded.append(0)
            mobile_discarded.append(0)
            desktop_engagement.append(0)
            mobile_engagement.append(0)

        df["Desktop traffic"] = desktop
        df["Desktop discarded traffic"] = desktop_discarded
        df['Mobile traffic'] = mobiles
        df["Mobile discarded traffic"] = mobile_discarded
        df['Desktop engagement'] = desktop_engagement
        df['Mobile engagement'] = mobile_engagement

        return df
