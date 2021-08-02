import calendar
import configparser
import datetime as dt
import json
import logging
import os
import sys

import pandas as pd
import pymysql
import requests
from dateutil.relativedelta import relativedelta

from chatStatus import businessStatus
from deviceSessions import deviceSessions
from discardedTraffic import report
from partnerHandler import partnerHandler
from phoneFeatures import phoneFeatures
from searchDetails import searchDetails

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
partnerHandler()
partnerJson = json.load(open("partnerNames.json"))
deviceSession = deviceSessions(configFile)


class EngagementReport:
    def __init__(self):
        self.db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                  passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["reportDB"])
        self.cur = self.db.cursor()

        self.db2 = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                                   passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["businessDB"])
        self.cur2 = self.db2.cursor()

        self.businessName = None
        self.businessEmail = None
        self.API_response = None

        self.previousMonthSessions = None
        self.previousMonthDiscards = None
        self.engagement = None
        self.offhours = None
        self.conversion = None

        self.initialStart = None
        self.initialEnd = None

        self.startDate = None
        self.endDate = None
        self.weekRange = None
        self.timezones = None

        self.businessAgentId = None
        self.businessId = None

        self.stats = dict()
        self.mobileFeatures = dict()
        self.deviceSessionDetails = dict()

        self.reportingDF = pd.DataFrame()

        self.totalSession = 0
        self.discardValue = 0

    def dataFetch(self):
        self.API_response = None

        requestParams = {
            "endDate": str(self.endDate),
            "businessAgentId": self.businessAgentId,
            "startDate": str(self.startDate),
            "masterBusinessAgentId": "",
            "domainId": "",
            "type": "statistics"
        }

        response = requests.post(url=configFile['analysis']['statsApi'], json=requestParams)

        if not str(response.status_code).startswith('50'):
            self.API_response = response.json()
            logger.info("dataFetch response data :{} ".format(self.API_response))

    def businessDetails(self):
        businessHeaders = {"Authorization": configFile['analysis']['token'], "Content-type": "application/json"}

        searchDict = searchDetails(configFile).toJSON()
        searchAPI = "https://" + env + configFile['analysis']['businessAgentApi'].format(self.businessAgentId)

        try:
            response = json.loads(
                requests.post(searchAPI, data=searchDict, headers=businessHeaders).content.decode('utf-8'))
        except Exception as E:
            logger.info("Error in fetching business details : {}".format(E))
            return

        try:
            self.businessName = response['agentValues'][0]['value'][0]
            logger.info("Business name has been fetched")
        except Exception as E:
            logger.info("Error in business Name details : {}".format(E))

    def weeklyDataFetch(self):
        self.initialStart = self.startDate
        self.initialEnd = self.endDate

        self.startDate, self.endDate = self.weekRange

        try:
            self.dataFetch()
            if self.API_response is None:
                self.dataFetch()
            logger.info("Fetched previous month data successfully")
        except Exception as E:
            logger.info("Error in charting API : {}".format(E))
            return

        try:
            if self.API_response is not None:
                previousTotal, previousDiscards = report(str(self.startDate), str(self.endDate), self.timezones,
                                                         self.businessAgentId, self.businessId, env)
                self.previousMonthSessions = previousTotal
                self.previousMonthDiscards = previousDiscards
                self.engagement = self.API_response["engagement"]
                self.offhours = self.API_response["offEngagement"]
                self.conversion = self.API_response["conversion"]
        except Exception as E:
            logger.info("Error in getting previous month data: {}".format(E))
            self.previousMonthDiscards = 0
            self.engagement = 0
            self.offhours = 0
            self.conversion = 0

    def messageFormation(self):
        self.weeklyDataFetch()

        try:
            self.mobileFeatures = phoneFeatures(configFile, self.initialStart, self.initialEnd, self.businessAgentId)
        except Exception as E:
            logger.info("Error in generating phone features : {}".format(E))
            return

        logger.info("Dataframe creation process started")

        try:
            self.reportingDF = self.fileHandling()
            logger.info("Dataframe created successfully")
        except Exception as E:
            logger.info("Error in data generation : {}".format(str(E)))
        finally:
            return self.reportingDF

    def fileHandling(self):
        tempReportingDF = pd.DataFrame()
        partnerName = None
        for partner, accounts in partnerJson.items():
            if " ".join(self.businessName.strip().lower().split()) in accounts:
                partnerName = partner

        tempReportingDF["Business Id"] = [self.businessId]
        tempReportingDF["Partner Name"] = [partnerName]
        tempReportingDF["Business Name"] = [self.businessName]

        try:
            trafficTrend = round(
                ((self.totalSession - self.previousMonthSessions) / self.previousMonthSessions) * 100, 2)
        except (ValueError, Exception):
            trafficTrend = 100

        try:
            engagementTrend = round(((self.stats["engagement"] - self.engagement) / self.engagement) * 100, 2)
        except (ValueError, Exception):
            engagementTrend = 100

        try:
            offhourTrend = round(((self.stats["offEngagement"] - self.offhours) / self.offhours) * 100, 2)
        except (ValueError, Exception):
            offhourTrend = 100

        try:
            contactTrend = round(((self.stats["conversion"] - self.conversion) / self.conversion) * 100, 2)
        except (ValueError, Exception):
            contactTrend = 100
        try:
            tempReportingDF["Traffic Trend (month over month)"] = [trafficTrend]
            tempReportingDF["Engagements Trend (month over month)"] = [engagementTrend]
            tempReportingDF["Off Hours Trend (month over month)"] = [offhourTrend]
            tempReportingDF["Contacts Trend (month over month)"] = [contactTrend]

            tempReportingDF["Total traffic\n(Total sessions created)"] = [self.totalSession]
            tempReportingDF["Discarded traffic\n(Sessions bounced off landing page without any activity)"] = [
                self.discardValue]

            try:
                discardedPercentage = self.discardValue / self.totalSession
            except (ValueError, Exception):
                discardedPercentage = 0

            tempReportingDF["Discarded traffic %\n(Discarded traffic / Total traffic) "] = [
                round(discardedPercentage, 2) * 100]
            effectiveTraffic = self.totalSession - self.discardValue
            tempReportingDF["Sessions (Total traffic - Discarded traffic)"] = [effectiveTraffic]

            try:
                tempReportingDF["Sessions %\n(Effective traffic / Total Traffic) "] = [
                    round((effectiveTraffic / self.totalSession), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Sessions %\n(Effective traffic / Total Traffic) "] = [0]

            tempReportingDF["Total Desktop traffic"] = self.deviceSessionDetails.get("Desktop traffic", 0)
            tempReportingDF["Desktop discarded traffic"] = self.deviceSessionDetails.get("Desktop discarded traffic", 0)
            tempReportingDF["Total Mobile traffic"] = self.deviceSessionDetails.get('Mobile traffic', 0)
            tempReportingDF["Mobile discarded traffic"] = self.deviceSessionDetails.get("Mobile discarded traffic", 0)

            tempReportingDF["Engagements\n(Session with at least one activity with bot)"] = [self.stats["engagement"]]

            try:
                tempReportingDF["Engagement %\n(Engagements / Sessions)"] = [
                    round((self.stats["engagement"] / effectiveTraffic), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Engagement %\n(Engagements / Sessions)"] = [0]

            tempReportingDF["Off-hour Engagement"] = [self.stats["offEngagement"]]

            try:
                tempReportingDF["Off-hours engagement %\n(Off-hours enagement / Engagements)"] = [round(
                    (self.stats["offEngagement"] / self.stats["engagement"]), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Off-hours engagement %\n(Off-hours enagement / Engagements)"] = [0]

            tempReportingDF["Desktop Engagement"] = self.deviceSessionDetails.get("Desktop engagement", 0)
            tempReportingDF["Mobile Engagement"] = self.deviceSessionDetails.get("Mobile engagement", 0)
            tempReportingDF["Contacts"] = [self.stats["conversion"]]

            try:
                tempReportingDF["Contacts %\n(Contacts / Engagements)"] = [round(
                    (self.stats["conversion"] / self.stats["engagement"]), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Contacts %\n(Contacts / Engagements)"] = [0]

            tempReportingDF["Previous Month's total traffic"] = [self.previousMonthSessions]
            tempReportingDF["Previous Month's discarded traffic"] = [self.previousMonthDiscards]

            try:
                lastMonthDiscardedPercentage = self.previousMonthDiscards / self.previousMonthSessions
            except (ValueError, Exception):
                lastMonthDiscardedPercentage = 0

            tempReportingDF["Previous Month's discarded traffic %"] = [round(lastMonthDiscardedPercentage, 2) * 100]
            lastMonthEffectiveTraffic = self.previousMonthSessions - self.previousMonthDiscards
            tempReportingDF["Previous Month's Sessions"] = [lastMonthEffectiveTraffic]

            try:
                tempReportingDF["Previous Month's Sessions %"] = [
                    round((lastMonthEffectiveTraffic / self.previousMonthSessions), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Previous Month's Sessions %"] = [0]

            tempReportingDF["Previous Month Engagements"] = [self.engagement]
            try:
                tempReportingDF["Previous Month's Engagement %"] = [
                    round((self.engagement / lastMonthEffectiveTraffic), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Previous Month's Engagement %"] = [0]

            tempReportingDF["Previous Month Off-hours Engagement"] = [self.offhours]
            try:
                tempReportingDF["Previous Month Off-hours engagement %"] = [
                    round((self.offhours / self.engagement), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Previous Month Off-hours engagement %"] = [0]

            tempReportingDF["Previous Month Contacts"] = [self.conversion]
            try:
                tempReportingDF["Previous Month Contacts %"] = [round((self.conversion / self.engagement), 2) * 100]
            except (ValueError, Exception):
                tempReportingDF["Previous Month Contacts %"] = [0]

            tempReportingDF["Deleted Contact"] = self.mobileFeatures.get("Deleted Contact", 0)
            tempReportingDF["SMS notifications"] = self.mobileFeatures.get("SMS notifications", 0)
            tempReportingDF["SMS Customers_to_Business"] = self.mobileFeatures.get("SMS Customers_to_Business", 0)
            tempReportingDF["SMS Business_to_Customer"] = self.mobileFeatures.get("SMS Business_to_Customer", 0)
            tempReportingDF["Email notifications"] = self.mobileFeatures.get("Email notifications", 0)
            tempReportingDF["Missed calls"] = self.mobileFeatures.get("Missed calls", 0)
            tempReportingDF["Notes"] = self.mobileFeatures.get("Notes", 0)
            tempReportingDF["Voice messages"] = self.mobileFeatures.get("Voice messages", 0)

        except Exception as E:
            logger.info("Error : %s" % str(E))
        finally:
            return tempReportingDF

    def process(self, businessTimezone, startDate, endDate, previousMonth):
        self.weekRange = previousMonth
        self.timezones = businessTimezone
        self.startDate = startDate
        self.endDate = endDate

        timeZoneQuery = "SELECT business_id FROM business_agent_mapping WHERE timezone = '{}'"
        self.cur2.execute(timeZoneQuery.format(str(self.timezones)))

        timezonesBusinessID = [ids[0] for ids in list(self.cur2.fetchall())]
        finalDF = pd.DataFrame()
        processedData = []

        if timezonesBusinessID:

            query = "SELECT business_agent_id, business_id  FROM report_master as a natural join report_event as b " \
                    "where b.reporting_date >= '{}' AND b.reporting_date <= '{}' AND b.status = 'SUCCESS'" \
                    " ORDER BY b.reporting_date ASC"
            self.cur.execute(query.format(self.startDate, self.endDate))

            for row in set(self.cur.fetchall()):
                self.startDate = startDate
                self.endDate = endDate
                self.businessAgentId = row[0]
                self.businessId = row[1]
                isEnabled = businessStatus(env, self.businessId, self.businessAgentId)
                if self.businessId in timezonesBusinessID and isEnabled:
                    processedData.append(self.businessAgentId)
                    self.businessDetails()
                    logger.info(
                        "{} - {}: started to generate".format(str(self.businessName.encode('utf-8')), self.businessId))
                    self.dataFetch()
                    if self.API_response is not None:
                        self.stats = self.API_response
                        self.totalSession, self.discardValue = report(str(self.startDate), str(self.endDate),
                                                                      self.timezones, self.businessAgentId,
                                                                      self.businessId, env)
                        self.deviceSessionDetails = deviceSession.business_id(self.timezones, str(self.endDate),
                                                                              str(self.startDate), self.businessAgentId)

                        self.stats["sessions"] = self.totalSession - self.discardValue
                        logger.info("Statistics data has been fetched")
                        finalDF = pd.concat([finalDF, self.messageFormation()], ignore_index=True)

        self.db.close()
        self.db2.close()
        return finalDF, processedData


db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                     passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["businessDB"])
cur = db.cursor()
cur.execute("select business_id from business_agent_mapping where is_enabled=1")
activeBusinessId = [ids[0] for ids in cur.fetchall()]
db.close()

for timezones in timezone_list['TimeZone']:
    logger.info("Processing {} timezone".format(timezones))
    initialEnd = dt.date.today() - dt.timedelta(days=2)
    initialStart = initialEnd.replace(day=1)
    today = initialEnd
    d = today - relativedelta(months=1)
    _, num_days = calendar.monthrange(d.year, d.month)
    weekRange = [dt.date(d.year, d.month, 1), dt.date(d.year, d.month, num_days)]
    fileName = initialStart.strftime('%d-%B-%Y') + "_to_" + initialEnd.strftime('%d-%B-%Y')
    logger.info("Current Report period : {}".format(fileName))
    logger.info("Previous month : {}".format(weekRange))

    try:
        reportingDF, processedData = EngagementReport().process(timezones, initialStart, initialEnd, weekRange)

    except Exception as e:
        logger.info("Error in object initialization : {}".format(e))
        continue

    try:
        if not reportingDF.empty:
            try:
                existingData = pd.read_excel("./" + fileName + ".xlsx")
                reportingDF = pd.concat([existingData, reportingDF], ignore_index=True)
            except Exception as e:
                logger.info("Problem while concatenating the excel: {}".format(e))
            finally:
                reportingDF.to_excel(fileName + ".xlsx", index=False)
    except Exception as e:
        logger.info("Error in Loading and parsing the data : {}".format(e))


reportingDF = pd.read_excel("./" + fileName + ".xlsx")
activeBusinessId = set(activeBusinessId)
sessionsBusinessId = set(reportingDF['Business Id'])
zeroSessionsBusinessId = list(activeBusinessId - sessionsBusinessId)

zeroSessionsDF = pd.DataFrame()
zeroSessionsDF['Business Id'] = zeroSessionsBusinessId

tempColumns = reportingDF.columns

for column in tempColumns[1:]:
    zeroSessionsDF[column] = 0

reportingDF.to_csv('businesses_with_engagements.csv', index=False)

# Verification Logic
logger.info("Differences in Engagements,Total Traffic and Discarded Traffic")

for i, business in reportingDF.iterrows():
    # Engagements(Session with at least one activity with bot) = Desktop Engagement	 + Mobile Engagement
    logger.info("Verifying {}".format(business['Business Id']))
    totalEngagement = business['Engagements\n(Session with at least one activity with bot)']
    desktopEngagement = business['Desktop Engagement']
    mobileEngagement = business['Mobile Engagement']
    diff = totalEngagement - (desktopEngagement + mobileEngagement)
    if abs(diff) > 0:
        logger.info("Difference in Engagement(Session with at least one activity with bot) - ")
        logger.info("Total Engagement - {} : Desktop Engagement + Mobile Engagement - {} + {}".format(totalEngagement, desktopEngagement, mobileEngagement))
        logger.info("The difference is {}".format(diff))
        business['Engagements\n(Session with at least one activity with bot)'] = desktopEngagement + mobileEngagement

    #  Total traffic (Total sessions created) = Total Desktop traffic + Total Mobile traffic
    totalTraffic = business['Total traffic\n(Total sessions created)']
    desktopTraffic = business['Total Desktop traffic']
    mobileTraffic = business['Total Mobile traffic']
    diff = totalTraffic - (desktopTraffic + mobileTraffic)
    if abs(diff) > 0:
        logger.info("Difference in Traffic(Total Sessions Created) - ")
        logger.info("Total Traffic - {} : Total Desktop traffic + Total Mobile traffic - {} + {}".format(
            totalTraffic, desktopTraffic, mobileTraffic))
        logger.info("The difference is {}".format(diff))
        business['Total traffic\n(Total sessions created)'] = desktopTraffic + mobileTraffic
    
    # Discarded traffic\n(Sessions bounced off landing page without any activity) = Desktop discarded traffic + Mobile discarded traffic

    discardedTraffic = business['Discarded traffic\n(Sessions bounced off landing page without any activity)']
    desktopDiscardedTraffic = business['Desktop discarded traffic']
    mobileDiscardedTraffic = business['Mobile discarded traffic']
    diff = discardedTraffic - (desktopDiscardedTraffic + mobileDiscardedTraffic)
    if abs(diff) > 0:
        logger.info("Difference in Discarded traffic(Sessions bounced off landing page without any activity) - ")
        logger.info("Discarded traffic - {} : Desktop discarded traffic + Mobile discarded traffic - {} + {}".
                    format(discardedTraffic, desktopDiscardedTraffic, mobileDiscardedTraffic))
        logger.info("The difference is {}".format(diff))
        business['Discarded traffic\n(Sessions bounced off landing page without any activity)'] = \
            desktopDiscardedTraffic + mobileDiscardedTraffic

    reportingDF.iloc[i, :] = business

finalDF = pd.concat([reportingDF, zeroSessionsDF])
# del finalDF['Partner Name']
# del finalDF['Business Name']
finalDF.to_csv('businesses_with_both_engagements_and_zero_sessions.csv', index=False)
