import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
FILE_NAME = 'ndvi_data.csv' 
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
RANGE_NAME = 'NDVI_Database!A1'

def run_transfer():
    # 1. Authenticate and Download
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    
    print("Reading data from GCS...")
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))

    # 2. Data Cleaning
    # Remove Earth Engine internal columns that aren't useful in a spreadsheet
    cols_to_drop = ['.geo', 'system:index']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # Ensure 'date' is the first column if it exists
    if 'date' in df.columns:
        cols = ['date'] + [c for c in df.columns if c != 'date']
        df = df[cols]

    # Replace NaN with empty strings (JSON/Sheets API doesn't like 'NaN')
    df = df.fillna('')

    # 3. Prepare for Sheets
    # If the sheet is brand new, you might want the header. 
    # If appending daily, you might only want the data. 
    # This version appends headers + data:
    values = [df.columns.tolist()] + df.values.tolist()
    body = {'values': values}

    # 4. Execute Append
    service = build('sheets', 'v4', credentials=creds)
    try:
        request = service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        )
        response = request.execute()
        print(f"✅ Successfully appended {len(df)} rows to Google Sheets!")
    except Exception as e:
        print(f"❌ API Error: {e}")

if __name__ == "__main__":
    run_transfer()
