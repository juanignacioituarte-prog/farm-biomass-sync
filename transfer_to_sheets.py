import os
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

SYNC_CONFIG = [
    {
        "db_csv": "ndvi_data.csv",
        "db_range": "NDVI_Database!A1",
        "partial_csv": "partial.csv",
        "partial_range": "partial!A:B",
        "partial_start": "partial!A1"
    },
    {
        "db_csv": "ndvi_data_wainono.csv",
        "db_range": "NDVI_Wainono!A1",
        "partial_csv": "partial_wainono.csv",
        "partial_range": "partial_w!A:B",
        "partial_start": "partial_w!A1"
    }
]

def sync_data():
    for farm in SYNC_CONFIG:
        # 1. SYNC NDVI DATABASE
        if os.path.exists(farm['db_csv']):
            try:
                # fillna('') removes NaN tokens that crash the Google Sheets API
                ndvi_df = pd.read_csv(farm['db_csv'], header=None).fillna('')
                ndvi_values = ndvi_df.values.tolist()

                if ndvi_values:
                    service.spreadsheets().values().append(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['db_range'],
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body={'values': ndvi_values}
                    ).execute()
                    print(f"Synced {farm['db_csv']} to {farm['db_range']}")
            except Exception as e:
                print(f"Error syncing {farm['db_csv']}: {e}")

        # 2. SYNC PARTIAL GRAZING
        if os.path.exists(farm['partial_csv']):
            try:
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID,
                    range=farm['partial_range']
                ).execute()

                partial_df = pd.read_csv(farm['partial_csv'], header=None).fillna('')
                partial_values = partial_df.values.tolist()

                if partial_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['partial_start'],
                        valueInputOption='RAW',
                        body={'values': partial_values}
                    ).execute()
                    print(f"Updated {farm['partial_range']}")
            except Exception as e:
                print(f"Error syncing {farm['partial_csv']}: {e}")

if __name__ == "__main__":
    sync_data()
