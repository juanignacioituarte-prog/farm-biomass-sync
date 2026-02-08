import pandas as pd
from google.cloud import storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
import io

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'service-account.json'
BUCKET_NAME = 'ndvi-exports'
FILE_NAME = 'ndvi_data.csv'
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
# Ensure this matches your tab name exactly
RANGE_NAME = 'NDVI_Database!A1' 

def run_transfer():
    # 1. Authenticate
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)

    # 2. Download CSV from GCS
    print("📥 Downloading data from GCS...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        print(f"❌ Error downloading or reading CSV: {e}")
        return

    # 3. Handle Duplicates
    # We check the Sheet first to see which paddock/date combos already exist
    print("🔍 Checking for existing records to avoid duplicates...")
    try:
        # Fetching first two columns (Name and Date) for comparison
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range="NDVI_Database!A:B"
        ).execute()
        existing_rows = result.get('values', [])
        # Create a unique key set (e.g., "Paddock1_2026-02-08")
        existing_keys = {f"{row[0]}_{row[1]}" for row in existing_rows if len(row) >= 2}
    except Exception as e:
        print(f"⚠️ Could not read existing sheet data (might be empty): {e}")
        existing_keys = set()

    # Create keys for the new data
    df['key'] = df['name'].astype(str) + "_" + df['date'].astype(str)
    
    # Filter out rows that are already in the sheet
    df_new = df[~df['key'].isin(existing_keys)].copy()
    df_new.drop(columns=['key'], inplace=True)

    if df_new.empty:
        print("⏭️ No new unique records to add. Skipping upload.")
        return

    # 4. Upload to Google Sheets (NO HEADERS)
    # .values.tolist() gives us just the data rows
    values = df_new.fillna('').values.tolist()

    print(f"🚀 Appending {len(values)} new rows to Google Sheets...")
    try:
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': values} # Headers are NOT included here
        ).execute()
        print("✅ Successfully updated Google Sheets.")
    except Exception as e:
        print(f"❌ Failed to append to Sheets: {e}")

if __name__ == "__main__":
    run_transfer()
