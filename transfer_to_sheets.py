import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
# GEE often adds numbers to the end, so we search for the prefix
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
    # Find the actual file (handles GEE sharding like ndvi_data000000000000.csv)
    blobs = list(bucket.list_blobs(prefix=FILE_PREFIX))
    
    if not blobs:
        print("❌ No CSV files found matching prefix in GCS.")
        return

    for blob in blobs:
        print(f"Reading {blob.name}...")
        df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))

        # Deduplication logic
        try:
            sheet_data = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range="NDVI_Database!A:B"
            ).execute().get('values', [])
            existing_keys = {f"{r[0]}_{r[1]}" for r in sheet_data if len(r) >= 2}
        except Exception as e:
            print(f"Note: Could not read existing data ({e}), assuming sheet is empty.")
            existing_keys = set()

        df['key'] = df['paddock_name'].astype(str) + "_" + df['date'].astype(str)
        df_new = df[~df['key'].isin(existing_keys)].copy()

        if df_new.empty:
            print(f"⏭️ No new records in {blob.name}.")
        else:
            # Match only columns that actually exist in the dataframe to prevent KeyError
            df_new['ndvi_mean'] = pd.to_numeric(df_new['ndvi_mean'], errors='coerce').round(4)
            
            # Dynamic column selection to avoid crashes
            available_cols = [c for c in ['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url'] if c in df_new.columns]
            values = df_new[available_cols].fillna('').values.tolist()

            service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': values}
            ).execute()
            print(f"✅ Appended {len(values)} rows from {blob.name}.")

        # Delete the file so it doesn't get processed again tomorrow
        blob.delete()
        print(f"🗑️ Deleted {blob.name} from bucket.")

if __name__ == "__main__":
    run_transfer()
