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
RANGE_NAME = 'NDVI_Database!A1'

def run_transfer():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)

    # Download from GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    
    if not blob.exists():
        print("❌ CSV not found in GCS. Export might have failed.")
        return

    # Read CSV and clean column names
    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
    df.columns = df.columns.str.strip().str.lower()

    # Prevent Duplicates
    # We check columns A (name) and B (date) to see if we've already imported this paddock/date combo
    try:
        sheet_data = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="NDVI_Database!A:B"
        ).execute().get('values', [])
        existing_keys = {f"{r[0]}_{r[1]}" for r in sheet_data if len(r) >= 2}
    except Exception as e:
        print(f"⚠️ Could not fetch existing keys (normal for new sheets): {e}")
        existing_keys = set()

    # Create unique key for deduplication
    df['key'] = df['name'].astype(str) + "_" + df['date'].astype(str)
    df_new = df[~df['key'].isin(existing_keys)].copy()

    if df_new.empty:
        print("⏭️ No new records to append.")
    else:
        # UPDATED COLUMN LIST: Including map_token and tile_url
        cols = [
            'name', 
            'date', 
            'ndvi_effective', 
            'cloud_pc', 
            'latest-update', 
            'map_id', 
            'map_token', 
            'tile_url'
        ]
        
        # Data Formatting
        df_new['ndvi_effective'] = pd.to_numeric(df_new['ndvi_effective'], errors='coerce').round(4)
        
        # Ensure all required columns exist in the DF (prevents KeyError if one was missing)
        for col in cols:
            if col not in df_new.columns:
                df_new[col] = ""

        values = df_new[cols].fillna('').values.tolist()

        # Upload to Sheets
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': values}
        ).execute()
        print(f"✅ Appended {len(values)} new records to Sheets (including Map Tokens).")

    # CLEANUP GCS
    blob.delete()
    print("🧹 GCS file deleted to prepare for next sync.")

if __name__ == "__main__":
    run_transfer()
