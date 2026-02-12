import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
FILE_PREFIX = 'ndvi_data' 
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'

def run_transfer():
    # 1. AUTHENTICATION
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/cloud-platform']
    )
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(BUCKET_NAME)

    # 2. SEARCH FOR CSVs
    blobs = list(bucket.list_blobs(prefix=FILE_PREFIX))
    if not blobs:
        print("❌ No CSV found in bucket.")
        return

    for blob in blobs:
        print(f"--- Processing File: {blob.name} ---")
        
        # 3. DOWNLOAD & READ DATA
        try:
            df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        except Exception as e:
            print(f"❌ Failed to download {blob.name}: {e}")
            continue

        # 4. DATA TRANSFORMATION
        # Rename 'ndvi_effective' to 'ndvi_mean' if it exists
        if 'ndvi_effective' in df.columns:
            df = df.rename(columns={'ndvi_effective': 'ndvi_mean'})
        
        # Check for target column
        if 'ndvi_mean' not in df.columns:
            print(f"⚠️ Warning: 'ndvi_mean' not found in {blob.name}. Available: {df.columns.tolist()}")
            continue

        # 5. VERIFICATION (No Appending/No Deleting)
        # We print a summary instead of sending to Google Sheets
        print(f"✅ Data processed successfully in memory.")
        print(df[['paddock_name', 'date', 'ndvi_mean']].head()) # Show first 5 rows
        
        # NOTE: blob.delete() has been removed. 
        # The file remains in 'ndvi-exports' for future use.
        print(f"📁 File '{blob.name}' has been kept in the bucket.\n")

if __name__ == "__main__":
    run_transfer()
