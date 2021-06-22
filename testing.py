import calendar
import configparser
import datetime as dt
import json
import logging
import os
import sys

import pymysql
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

from chatStatus import businessStatus
from discardedTraffic import report
from phoneFeatures import phoneFeatures

env = sys.argv[-1].lower()
configFile = configparser.ConfigParser()
configFile.read(env + '_config.ini')

timezone_list = pd.read_csv("./timezone_name.csv")
now = dt.datetime.now()
last_month = now - relativedelta(months=1)

lastMonthLog = "./business_weekly_report_{}.log".format(format(last_month, '%B'))
if os.path.isfile(lastMonthLog):
    os.remove(lastMonthLog)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s: [ %(message)s ]', '%m/%d/%Y %I:%M:%S %p')
console = logging.FileHandler("./business_weekly_report_{}.log".format(now.strftime("%B")))
console.setFormatter(fmt)
logger.addHandler(console)

headers = {"Authorization": "93cc004b-8b15-4776-9cb8-b7410360b61a", "Content-type": "application/json"}

# existingFileData = pd.read_excel('/Users/sharma/01-February-2021_to_28-February-2021.xlsx')
# existingFileContent = list(existingFileData['Business Name'])


class searchDetails:
    def __init__(self):
        self.names = configFile['analysis']['searchItems'].split(',')
        self.language = "EN_US"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class weeklyReport:
    def __init__(self):
        self.db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                  passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["reportDB"])
        self.cur = self.db.cursor()

        self.db1 = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                   passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["database"])
        self.cur1 = self.db1.cursor()

        self.db2 = pymysql.connect(host="common-db-19.cehfyojnbq1g.us-east-2.rds.amazonaws.com", user="sharma-agentz",
                                   passwd="test@123", db=configFile["analysis"]["businessDB"])
        self.cur2 = self.db2.cursor()

        self.businessName = None
        self.businessEmail = None

    def process(self, timezones, startDate, endDate, weekRange):
        self.startDate = startDate
        self.endDate = endDate
        self.weekRange = weekRange
        self.timezones = timezones

        timeZoneQuery = "SELECT business_id FROM business_agent_mapping WHERE timezone = '{}'"
        self.cur2.execute(timeZoneQuery.format(str(self.timezones)))

        timezonesBusinessID = [id[0] for id in list(self.cur2.fetchall())]
        finalDF = pd.DataFrame()

        if timezonesBusinessID:

            query = "SELECT business_agent_id, business_id  FROM report_master as a natural join report_event as b " \
                    "where b.reporting_date >= '{}' AND b.reporting_date <= '{}' AND b.status = 'SUCCESS'" \
                    " ORDER BY b.reporting_date ASC"
            self.cur.execute(query.format(self.startDate, self.endDate))

            for row in set(self.cur.fetchall()):
                self.startDate = startDate
                self.endDate = endDate
                self.row = row
                isEnabled = businessStatus(env, self.row[1], self.row[0])
                if self.row[1] in timezonesBusinessID and isEnabled:
                    self.businessDetails()
                    logger.info("{} : started to generate".format(str(self.businessName.encode('utf-8'))))
                    # if self.businessName in existingFileContent:
                    #     logger.info("it's already there")
                    #     continue
                    self.cur.execute(
                        "SELECT status FROM etl_status where business_agent_id = %s and start_date = %s and end_date = %s ",
                        (self.row[0], str(self.startDate), str(self.endDate),))

                    try:
                        self.ETLstatus = self.cur.fetchone()[0]
                    except:
                        self.ETLstatus = None

                    if (self.ETLstatus is None or self.ETLstatus == "NOT SENT") and self.ETLstatus != "FAILED":
                        logger.info("Process started for the businnes {}".format(self.row[0]))
                        self.dataFetch('statistics')
                        if self.APIresponse is not None:
                            self.stats = self.APIresponse
                            self.totalSession, self.discardValue = report(str(self.startDate), str(self.endDate),
                                                                          self.timezones, self.row[0], self.row[1], env)
                            self.stats["sessions"] = self.totalSession - self.discardValue
                            logger.info("Statistics data has been fetched")
                            reportingDF = self.messageFormation()
                            finalDF = pd.concat([finalDF, reportingDF], ignore_index=True)

            self.db.close()
            self.db1.close()
            self.db2.close()
            return finalDF

    def dataFetch(self, type):
        self.APIresponse = None

        self.requestParams = {
            "endDate": str(self.endDate),
            "businessAgentId": self.row[0],
            "startDate": str(self.startDate),
            "masterBusinessAgentId": "",
            "domainId": "",
            "type": type
        }

        self.url = configFile['analysis']['statsApi']

        response = requests.post(url=self.url, json=self.requestParams)

        if not str(response.status_code).startswith('50'):
            self.APIresponse = response.json()
            logger.info("dataFetch response data :{} ".format(self.APIresponse))

    def businessDetails(self):
        self.headers = {"Authorization": configFile['analysis']['token'], "Content-type": "application/json"}

        searchDict = searchDetails().toJSON()
        searchAPI = "https://" + env + configFile['analysis']['businessAgentApi'].format(self.row[0])

        try:
            response = json.loads(
                requests.post(searchAPI, data=searchDict, headers=self.headers).content.decode('utf-8'))
        except Exception as e:
            logger.info("Error in business details api")
            logger.error(e)

        try:
            self.businessName = response['agentValues'][0]['value'][0]
            logger.info("Business details have been fetched")
        except Exception as e:
            logger.info("Error in business Name details api")
            logger.error(e)

    def weeklyDataFetch(self):
        self.initialStart = self.startDate
        self.initialEnd = self.endDate

        self.previousMonthSessions = []
        self.previousMonthDiscards = []
        self.engagement = []
        self.offhours = []
        self.conversion = []

        weekstart, weekend = self.weekRange[1]
        self.startDate = weekstart
        self.endDate = weekend
        self.dataFetch('statistics')
        if self.APIresponse is None:
            self.dataFetch('statistics')
        if self.APIresponse is not None:
            previousTotal, previousDiscards = report(str(self.startDate), str(self.endDate), self.timezones,
                                                     self.row[0], self.row[1], env)
            self.previousMonthSessions.append(previousTotal)
            self.previousMonthDiscards.append(previousDiscards)
            self.engagement.append(self.APIresponse["engagement"])
            self.offhours.append(self.APIresponse["offEngagement"])
            self.conversion.append(self.APIresponse["conversion"])

        self.previousMonthSessions.insert(0, [])
        self.previousMonthDiscards.insert(0, [])
        self.engagement.insert(0, [])
        self.offhours.insert(0, [])
        self.conversion.insert(0, [])

        self.previousMonthSessions.insert(2, [])
        self.previousMonthDiscards.insert(2, [])
        self.engagement.insert(2, [])
        self.offhours.insert(2, [])
        self.conversion.insert(2, [])

    def messageFormation(self):
        self.weeklyDataFetch()
        logger.info("90 Days data for all the engagement trends are fetched")

        self.statusData = {
            "businessAgentId": self.row[0],
            "startDate": str(self.initialStart),
            "endDate": str(self.initialEnd),
            "status": "NOT SENT"
        }

        logger.info("Entry Created for the business agent in etl status table")

        self.reportingDF = pd.DataFrame()
        try:
            self.mobileFeatures = phoneFeatures(configFile, self.initialStart, self.initialEnd, self.row[0])
            self.reportingDF = self.fileHandling()
            logger.info("SENT status updated in the etl status table for the above business name")
            return self.reportingDF
        except Exception as e:
            logger.info("Error in data generation : {}".format(str(e)))
            return self.reportingDF

    def fileHandling(self):
        reportingDF = pd.DataFrame()
        reportingDF["Business Name"] = [self.businessName]

        try:
            trafficTrend = round(
                ((self.totalSession - self.previousMonthSessions[1]) / self.previousMonthSessions[1]) * 100, 2)
        except:
            trafficTrend = 100

        try:
            engagementTrend = round(((self.stats["engagement"] - self.engagement[1]) / self.engagement[1]) * 100, 2)
        except:
            engagementTrend = 100

        try:
            offhourTrend = round(((self.stats["offEngagement"] - self.offhours[1]) / self.offhours[1]) * 100, 2)
        except:
            offhourTrend = 100

        try:
            contactTrend = round(((self.stats["conversion"] - self.conversion[1]) / self.conversion[1]) * 100, 2)
        except:
            contactTrend = 100
        try:
            reportingDF["Traffic Trend (month over month)"] = [trafficTrend]
            reportingDF["Engagements Trend (month over month)"] = [engagementTrend]
            reportingDF["Off Hours Trend (month over month)"] = [offhourTrend]
            reportingDF["Contacts Trend (month over month)"] = [contactTrend]

            reportingDF["Total traffic\n(Total sessions created)"] = [self.totalSession]
            reportingDF["Discarded traffic\n(Sessions bounced off landing page without any activity)"] = [
                self.discardValue]

            try:
                discardedPercentage = self.discardValue / self.totalSession
            except:
                discardedPercentage = 0

            reportingDF["Discarded traffic %\n(Discarded traffic / Total traffic) "] = [
                round(discardedPercentage, 2) * 100]
            effectiveTraffic = self.totalSession - self.discardValue
            reportingDF["Sessions (Total traffic - Discarded traffic)"] = [effectiveTraffic]

            try:
                reportingDF["Sessions %\n(Effective traffic / Total Traffic) "] = [
                    round((effectiveTraffic / self.totalSession), 2) * 100]
            except:
                reportingDF["Sessions %\n(Effective traffic / Total Traffic) "] = [0]

            reportingDF["Engagements\n(Session with at least one activity with bot)"] = [self.stats["engagement"]]

            try:
                reportingDF["Engagement %\n(Engagements / Sessions)"] = [
                    round((self.stats["engagement"] / effectiveTraffic), 2) * 100]
            except:
                reportingDF["Engagement %\n(Engagements / Sessions)"] = [0]

            reportingDF["Off-hour Engagement"] = [self.stats["offEngagement"]]

            try:
                reportingDF["Off-hours engagement %\n(Off-hours enagement / Engagements)"] = [round(
                    (self.stats["offEngagement"] / self.stats["engagement"]), 2) * 100]
            except:
                reportingDF["Off-hours engagement %\n(Off-hours enagement / Engagements)"] = [0]

            reportingDF["Contacts"] = [self.stats["conversion"]]

            try:
                reportingDF["Contacts %\n(Contacts / Engagements)"] = [round(
                    (self.stats["conversion"] / self.stats["engagement"]), 2) * 100]
            except:
                reportingDF["Contacts %\n(Contacts / Engagements)"] = [0]

            reportingDF["Previous Month's total traffic"] = [self.previousMonthSessions[1]]
            reportingDF["Previous Month's discarded traffic"] = [self.previousMonthDiscards[1]]

            try:
                lastMonthDiscardedPercentage = self.previousMonthDiscards[1] / self.previousMonthSessions[1]
            except:
                lastMonthDiscardedPercentage = 0

            reportingDF["Previous Month's discarded traffic %"] = [round(lastMonthDiscardedPercentage, 2) * 100]
            lastMonthEffectiveTraffic = self.previousMonthSessions[1] - self.previousMonthDiscards[1]
            reportingDF["Previous Month's Sessions"] = [lastMonthEffectiveTraffic]

            try:
                reportingDF["Previous Month's Sessions %"] = [
                    round((lastMonthEffectiveTraffic / self.previousMonthSessions[1]), 2) * 100]
            except:
                reportingDF["Previous Month's Sessions %"] = [0]

            reportingDF["Previous Month Engagements"] = [self.engagement[1]]
            try:
                reportingDF["Previous Month's Engagement %"] = [
                    round((self.engagement[1] / lastMonthEffectiveTraffic), 2) * 100]
            except:
                reportingDF["Previous Month's Engagement %"] = [0]

            reportingDF["Previous Month Off-hours Engagement"] = [self.offhours[1]]
            try:
                reportingDF["Previous Month Off-hours engagement %"] = [
                    round((self.offhours[1] / self.engagement[1]), 2) * 100]
            except:
                reportingDF["Previous Month Off-hours engagement %"] = [0]

            reportingDF["Previous Month Contacts"] = [self.conversion[1]]
            try:
                reportingDF["Previous Month Contacts %"] = [round((self.conversion[1] / self.engagement[1]), 2) * 100]
            except:
                reportingDF["Previous Month Contacts %"] = [0]

            reportingDF["Deleted Contact"] = self.mobileFeatures["Deleted Contact"]
            reportingDF["SMS notifications"] = self.mobileFeatures["SMS notifications"]
            reportingDF["SMS Customers_to_Business"] = self.mobileFeatures["SMS Customers_to_Business"]
            reportingDF["SMS Business_to_Customer"] = self.mobileFeatures["SMS Business_to_Customer"]
            reportingDF["Email notifications"] = self.mobileFeatures["Email notifications"]
            reportingDF["Missed calls"] = self.mobileFeatures["Missed calls"]
            reportingDF["Notes"] = self.mobileFeatures["Notes"]
            reportingDF["Voice messages"] = self.mobileFeatures["Voice messages"]

        except Exception as e:
            logger.info("Error : %s" % str(e))
        return reportingDF


def dayCheck(timezones):
    initialEnd = dt.datetime.strptime("2021-02-28", "%Y-%m-%d").date()
    initialStart = initialEnd.replace(day=1)
    weekRange = []
    today = dt.datetime.strptime("2021-02-28", "%Y-%m-%d").date()
    for i in range(1, 4):
        d = today - relativedelta(months=i)
        _, num_days = calendar.monthrange(d.year, d.month)
        weekRange.append([dt.date(d.year, d.month, 1), dt.date(d.year, d.month, num_days)])

    weekRange.reverse()

    weekStart = initialStart.strftime('%d-%B-%Y')
    weekEnd = initialEnd.strftime('%d-%B-%Y')
    fileName = weekStart + "_to_" + weekEnd
    reportingDF = weeklyReport().process(timezones, initialStart, initialEnd, weekRange)
    try:
        if not reportingDF.empty:
            try:
                existingData = pd.read_excel("./" + fileName + ".xlsx")
                reportingDF = pd.concat([existingData, reportingDF], ignore_index=True)
            except:
                pass
            finally:
                reportingDF.to_excel("./" + fileName + ".xlsx", index=False)
    except Exception as e:
        logger.info("error in day check")
        logger.error(e)


for timezones in timezone_list['TimeZone']:
    print(timezones)
    dayCheck(timezones)