import os
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Setup Credentials
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

def sync_data():
    """
    Synchronizes the latest-image NDVI dataset and the Partial Grazing detections.
    """

    # 1. SYNC NDVI DATABASE (Append new records)
    try:
        if os.path.exists('ndvi_data.csv'):
            ndvi_df = pd.read_csv('ndvi_data.csv', header=None)
            ndvi_values = ndvi_df.values.tolist()

            if ndvi_values:
                service.spreadsheets().values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range='NDVI_Database!A1',
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                    body={'values': ndvi_values}
                ).execute()
                print(f"Successfully appended {len(ndvi_values)} records to NDVI_Database.")
    except Exception as e:
        print(f"NDVI Sync Error: {e}")

    # 2. SYNC PARTIAL GRAZING (Overwrite/Clear and Update)
    try:
        # Clear existing partial detections
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range='partial!A:B'
        ).execute()

        # Load new partial detections
        if os.path.exists('partial.csv') and os.path.getsize('partial.csv') > 0:
            partial_df = pd.read_csv('partial.csv', header=None)
            partial_values = partial_df.values.tolist()

            if partial_values:
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range='partial!A1',
                    valueInputOption='RAW',
                    body={'values': partial_values}
                ).execute()
                print(f"Synced {len(partial_values)} partial grazing detections.")
        else:
            print("No partial grazing events detected in the latest image. Sheet cleared.")

    except Exception as e:
        print(f"Partial Sync Error: {e}")

if __name__ == "__main__":
    sync_data()
