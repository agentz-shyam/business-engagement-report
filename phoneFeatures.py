import pymysql
import pandas as pd


def phoneFeatures(configFile, start, end, businessId):
    db = pymysql.connect(host=configFile["analysis"]["host"], user=configFile["analysis"]["user"],
                         passwd=configFile["analysis"]["pwd"], db=configFile["analysis"]["conversationDB"])

    contactMaster_query = 'SELECT * FROM contact_master where business_agent_mapping_id = "{}" and ' \
                          'updated_date >= "{}" and updated_date <= "{}"'
    contactNote_query = 'SELECT * FROM contact_note where contact_master_id in {}'

    contactMaster_df = pd.read_sql_query(contactMaster_query.format(businessId, start, end), db)
    if len(contactMaster_df['id']) > 1:
        contactNote_df = pd.read_sql_query(contactNote_query.format(tuple(contactMaster_df['id'])), db)
    else:
        contactNote_query = "SELECT * FROM contact_note where contact_master_id ='{}'"
        contactNote_df = pd.read_sql_query(contactNote_query.format(contactMaster_df['id']), db)
    contactNote_temp = contactNote_df['type'].value_counts()

    db.close()

    try:
        voiceMessage = len(
            list(filter(None, contactNote_df[contactNote_df['type'] == 'INCOMING_PHONE']['additional_info'])))
    except:
        voiceMessage = 0

    mobileFeatures = dict()
    try:
        mobileFeatures["Deleted Contact"] = [contactMaster_df['is_live'].value_counts()[0]]
    except:
        mobileFeatures["Deleted Contact"] = [0]
    try:
        mobileFeatures["SMS notifications"] = [contactNote_temp['SMS']]
    except:
        mobileFeatures["SMS notifications"] = [0]
    try:
        mobileFeatures["SMS Customers_to_Business"] = [
            contactNote_temp['INCOMING_SMS']]
    except:
        mobileFeatures["SMS Customers_to_Business"] = [0]
    try:
        mobileFeatures["SMS Business_to_Customer"] = [contactNote_temp['OUTGOING_SMS']]
    except:
        mobileFeatures["SMS Business_to_Customer"] = [0]
    try:
        mobileFeatures["Email notifications"] = [contactNote_temp['EMAIL']]
    except:
        mobileFeatures["Email notifications"] = [0]
    try:
        mobileFeatures["Missed calls"] = [contactNote_temp['INCOMING_PHONE']]
    except:
        mobileFeatures["Missed calls"] = [0]
    try:
        mobileFeatures["Notes"] = [contactNote_temp['BUSINESS_NOTE']]
    except:
        mobileFeatures["Notes"] = [0]
    try:
        mobileFeatures["Voice messages"] = [voiceMessage]
    except:
        mobileFeatures["Voice messages"] = [0]
    return mobileFeatures
