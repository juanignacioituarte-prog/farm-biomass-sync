import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

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

    # 1. Deduplication: Get existing keys from Sheet
    try:
        sheet_data = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="NDVI_Database!A:B"
        ).execute().get('values', [])
        existing_keys = {f"{r[0]}_{r[1]}" for r in sheet_data if len(r) >= 2}
    except:
        existing_keys = set()

    # 2. Process all CSVs in bucket
    blobs = list(bucket.list_blobs(prefix=FILE_PREFIX))
    all_new_rows = []

    for blob in blobs:
        print(f"Reading {blob.name}...")
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        
        # Rename column for consistency
        if 'ndvi_effective' in df.columns:
            df = df.rename(columns={'ndvi_effective': 'ndvi_mean'})

        # Filter out rows already in the sheet
        df['key'] = df['paddock_name'].astype(str) + "_" + df['date'].astype(str)
        df_new = df[~df['key'].isin(existing_keys)].copy()

        if not df_new.empty:
            df_new['ndvi_mean'] = pd.to_numeric(df_new['ndvi_mean'], errors='coerce').round(4)
            cols = ['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url']
            all_new_rows.extend(df_new[cols].fillna('').values.tolist())

    # 3. Append only new rows
    if all_new_rows:
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
            valueInputOption='USER_ENTERED', insertDataOption='INSERT_ROWS',
            body={'values': all_new_rows}
        ).execute()
        print(f"✅ Appended {len(all_new_rows)} rows to Google Sheets.")
    else:
        print("⏭️ No new records to append.")

    print("📁 Files preserved in Cloud Storage.")

if __name__ == "__main__":
    run_transfer()
