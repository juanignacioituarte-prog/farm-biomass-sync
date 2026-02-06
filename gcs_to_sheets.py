import os
import pandas as pd
import time
from datetime import datetime
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

# Configuration
SHEET_ID = "1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04"
SHEET_NAME = "NDVI_Database"  # Updated tab name
BUCKET_NAME = "ndvi-exports" 
FILE_NAME = "latest_biomass.csv"

def execute_with_retry(request, max_retries=5):
    retry_count = 0
    backoff = 2 
    while retry_count < max_retries:
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504] and retry_count < max_retries - 1:
                print(f"⚠️ Google API error. Retrying in {backoff}s...")
                time.sleep(backoff)
                retry_count += 1
                backoff *= 2
            else:
                raise e

def main():
    try:
        # 1. Setup Auth
        print("🔐 Authenticating...")
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
        creds = service_account.Credentials.from_service_account_file(creds_path)
        
        # 2. Download from GCS
        print(f"✅ Downloading from Bucket: {BUCKET_NAME}")
        storage_client = storage.Client(credentials=creds)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(FILE_NAME)
        blob.download_to_filename(FILE_NAME)

        # 3. Read and format CSV
        df = pd.read_csv(FILE_NAME)
        
        # Ensure the columns are in the correct order
        # (Assuming your CSV has these exact column headers)
        cols_to_keep = ['paddock_name', 'date', 'ndvi_effective', 'cloud_pc']
        df = df[cols_to_keep]

        # Add the 'Latest Update' timestamp as the 5th column
        sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['last_updated'] = sync_time

        # Convert to list for Sheets (Exclude headers to prevent duplicates in the rows)
        new_data = df.values.tolist()

        # 4. APPEND to Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        print(f"📊 Appending {len(new_data)} rows to {SHEET_NAME}...")
        
        # .append() finds the next available empty row automatically
        append_req = sheet.values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED", 
            insertDataOption="INSERT_ROWS",
            body={'values': new_data}
        )
        execute_with_retry(append_req)

        print(f"🚀 Sync Complete! All data appended successfully.")

        # Clean up the local file on the runner
        if os.path.exists(FILE_NAME):
            os.remove(FILE_NAME)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
