import pygsheets
from google.oauth2 import service_account
import json


def partnerHandler():
    partnerJson = json.load(open("partnerNames.json"))

    SCOPES = ("https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive")
    with open('creds.json') as source:
        service_account_info = json.load(source, )
    my_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

    client = pygsheets.authorize(custom_credentials=my_credentials)
    sheet = client.open_by_key("1PgVR1WTzyR6aZYjoFomzlNMORZjwZkO0uRTXa4swrY8")
    wks = sheet.worksheet_by_title('Accounts')
    sheet_data = wks.get_as_df()

    signpostName = []
    for name in sheet_data['Business Name']:
        name = " ".join(str(name).strip().lower().split())
        if len(name) > 0:
            signpostName.append(name)

    partnerJson['signpost'] = signpostName
    json.dump(partnerJson, open("partnerNames.json", "w"), ensure_ascii=True, indent=4)
