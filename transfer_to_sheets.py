import os
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
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
        # 1. SYNC NDVI DATABASE (Append)
        try:
            if os.path.exists(farm['db_csv']):
                ndvi_df = pd.read_csv(farm['db_csv'], header=None)
                ndvi_values = ndvi_df.values.tolist()

                if ndvi_values:
                    service.spreadsheets().values().append(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['db_range'],
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body={'values': ndvi_values}
                    ).execute()
                    print(f"Appended {len(ndvi_values)} records to {farm['db_range']}.")
        except Exception as e:
            print(f"NDVI Sync Error ({farm['db_csv']}): {e}")

        # 2. SYNC PARTIAL GRAZING (Clear and Update)
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=farm['partial_range']
            ).execute()

            if os.path.exists(farm['partial_csv']) and os.path.getsize(farm['partial_csv']) > 0:
                partial_df = pd.read_csv(farm['partial_csv'], header=None)
                partial_values = partial_df.values.tolist()

                if partial_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['partial_start'],
                        valueInputOption='RAW',
                        body={'values': partial_values}
                    ).execute()
                    print(f"Synced {len(partial_values)} events to {farm['partial_range']}.")
        except Exception as e:
            print(f"Partial Sync Error ({farm['partial_csv']}): {e}")

if __name__ == "__main__":
    sync_data()
