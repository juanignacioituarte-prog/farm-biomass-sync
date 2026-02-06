import os
import pandas as pd
import time
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

# Configuration
SHEET_ID = "1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04"
SHEET_NAME = "NDVI_Database"  # Ensure this matches your tab name
BUCKET_NAME = "ndvi-exports" # Update if your bucket name is different
FILE_NAME = "latest_biomass.csv"

def execute_with_retry(request, max_retries=5):
    """Executes a Google API request with exponential backoff for 5xx errors."""
    retry_count = 0
    backoff = 2 
    while retry_count < max_retries:
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504] and retry_count < max_retries - 1:
                print(f"⚠️ Google API 503/500 error. Retrying in {backoff}s...")
                time.sleep(backoff)
                retry_count += 1
                backoff *= 2
            else:
                raise e

def main():
    try:
        # 1. Setup Auth (Uses GitHub Secrets/Service Account)
        print("🔐 Authenticating...")
        # Path for GitHub Actions or local service account file
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
        creds = service_account.Credentials.from_service_account_file(creds_path)
        
        # 2. Download from GCS
        print(f"✅ Downloading: {FILE_NAME}")
        storage_client = storage.Client(credentials=creds)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(FILE_NAME)
        blob.download_to_filename(FILE_NAME)

        # 3. Read and format CSV
        df = pd.read_csv(FILE_NAME)
        # Ensure dates are strings for JSON transport
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        
        values = [df.columns.values.tolist()] + df.values.tolist()

        # 4. Push to Google Sheets with Retry Logic
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        print(f"📊 Updating Sheet: {SHEET_NAME}...")
        
        # Clear existing data first
        clear_req = sheet.values().clear(spreadsheetId=SHEET_ID, range=SHEET_NAME)
        execute_with_retry(clear_req)

        # Update with new data
        body = {'values': values}
        update_req = sheet.values().update(
            spreadsheetId=SHEET_ID, 
            range=SHEET_NAME,
            valueInputOption="RAW", 
            body=body
        )
        execute_with_retry(update_req)

        print("🚀 Sync Complete!")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
