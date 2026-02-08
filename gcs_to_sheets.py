import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

# Setup Credentials
SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
FILE_NAME = 'ndvi_data.csv'
SPREADSHEET_ID = 'yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04' # Change this!
RANGE_NAME = 'NDVI_Database!A1'

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

def run_transfer():
    # 1. Download from GCS
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    content = blob.download_as_bytes()
    
    df = pd.read_csv(io.BytesIO(content))
    
    # 2. Fix the Index/Column Issue
    print(f"Columns found: {df.columns.tolist()}")
    
    # Standardize column names (stripping whitespace/lowering case)
    df.columns = [c.strip().lower() for c in df.columns]
    
    if 'date' not in df.columns:
        # If EE exported it as 'system:index' or something else, handle it:
        print("⚠️ 'date' column missing. Attempting to recover...")
        if 'system:index' in df.columns:
            df.rename(columns={'system:index': 'date'}, inplace=True)
        else:
            raise KeyError(f"Could not find 'date' column. Available: {df.columns}")

    # Ensure 'date' is first for clean Sheets viewing
    cols = ['date'] + [c for c in df.columns if c != 'date']
    df = df[cols].sort_values('date')

    # 3. Upload to Google Sheets
    service = build('sheets', 'v4', credentials=creds)
    values = df.values.tolist()
    # Add headers if sheet is empty (simplified for this example)
    body = {'values': [df.columns.tolist()] + values}
    
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        body=body
    ).execute()
    
    print("✅ Successfully appended data to Google Sheets!")

if __name__ == "__main__":
    run_transfer()
