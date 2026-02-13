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
        "db_range": "NDVI_Database!A2:E",  # Starts at A2 to keep header
        "partial_csv": "partial.csv",
        "partial_range": "partial!A2:B"    # Starts at A2 to keep header
    },
    {
        "db_csv": "ndvi_data_wainono.csv",
        "db_range": "NDVI_Wainono!A2:E",
        "partial_csv": "partial_wainono.csv",
        "partial_range": "partial_w!A2:B"
    }
]

def sync_data():
    for farm in SYNC_CONFIG:
        # 1. OVERWRITE NDVI DATABASE (From Row 2 down)
        if os.path.exists(farm['db_csv']):
            try:
                # Clear existing data from Row 2 downwards
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID, range=farm['db_range']).execute()

                ndvi_df = pd.read_csv(farm['db_csv'], header=None).fillna('')
                ndvi_values = ndvi_df.values.tolist()

                if ndvi_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['db_range'].split(':')[0], # Uses A2 or A2 equivalent
                        valueInputOption='RAW',
                        body={'values': ndvi_values}
                    ).execute()
                    print(f"Overwrote {farm['db_csv']} data (Headers preserved).")
            except Exception as e:
                print(f"Error overwriting {farm['db_csv']}: {e}")

        # 2. OVERWRITE PARTIAL GRAZING (From Row 2 down)
        if os.path.exists(farm['partial_csv']):
            try:
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID, range=farm['partial_range']).execute()

                partial_df = pd.read_csv(farm['partial_csv'], header=None).fillna('')
                partial_values = partial_df.values.tolist()

                if partial_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['partial_range'].split(':')[0],
                        valueInputOption='RAW',
                        body={'values': partial_values}
                    ).execute()
                    print(f"Overwrote {farm['partial_csv']} data (Headers preserved).")
            except Exception as e:
                print(f"Error overwriting {farm['partial_csv']}: {e}")

if __name__ == "__main__":
    sync_data()
