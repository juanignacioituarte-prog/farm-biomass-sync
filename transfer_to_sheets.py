import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io
import time

SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
FILE_PREFIX = 'ndvi_data' 
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
RANGE_NAME = 'NDVI_Database!A1'

def run_transfer():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/cloud-platform']
    )
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Wait for GEE file
    blobs = []
    for i in range(10):
        blobs = list(bucket.list_blobs(prefix=FILE_PREFIX))
        if blobs: break
        print(f"⏳ Waiting for GEE file (Attempt {i+1}/10)...")
        time.sleep(30)

    if not blobs:
        print("❌ No files found in bucket.")
        return

    for blob in blobs:
        print(f"Reading {blob.name}...")
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))

        # Fix column name if it came from GEE as 'ndvi_effective'
        if 'ndvi_effective' in df.columns and 'ndvi_mean' not in df.columns:
            df = df.rename(columns={'ndvi_effective': 'ndvi_mean'})

        # Deduplication logic
        try:
            sheet_data = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range="NDVI_Database!A:B"
            ).execute().get('values', [])
            existing_keys = {f"{r[0]}_{r[1]}" for r in sheet_data if len(r) >= 2}
        except:
            existing_keys = set()

        df['key'] = df['paddock_name'].astype(str) + "_" + df['date'].astype(str)
        df_new = df[~df['key'].isin(existing_keys)].copy()

        if not df_new.empty:
            # Clean numeric data
            df_new['ndvi_mean'] = pd.to_numeric(df_new['ndvi_mean'], errors='coerce').round(4)
            
            # Use columns that match your Sheet headers
            cols = ['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url']
            values = df_new[cols].fillna('').values.tolist()

            service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
                valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS',
                body={'values': values}
            ).execute()
            print(f"✅ Appended {len(values)} rows.")
        else:
            print("⏭️ No new records.")

        blob.delete()

if __name__ == "__main__":
    run_transfer()
