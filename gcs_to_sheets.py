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
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)

    blob = storage_client.bucket(BUCKET_NAME).blob(FILE_NAME)
    if not blob.exists():
        print("❌ CSV not found in GCS.")
        return

    df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
    df.columns = df.columns.str.strip().str.lower()

    # Get existing Sheet data to prevent duplicates
    try:
        sheet_data = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="NDVI_Database!A:B"
        ).execute().get('values', [])
        existing = {f"{r[0]}_{r[1]}" for r in sheet_data if len(r) >= 2}
    except:
        existing = set()

    df['key'] = df['name'].astype(str) + "_" + df['date'].astype(str)
    df_new = df[~df['key'].isin(existing)].copy()

    if df_new.empty:
        print("⏭️ No new data.")
        return

    # Clean data and order columns
    cols = ['name', 'date', 'ndvi_effective', 'cloud_pc', 'latest-update', 'map_id']
    df_new['ndvi_effective'] = pd.to_numeric(df_new['ndvi_effective'], errors='coerce').round(4)
    values = df_new[cols].fillna('').values.tolist()

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='USER_ENTERED',
        insertDataOption='INSERT_ROWS',
        body={'values': values} # No Headers
    ).execute()
    print(f"✅ Appended {len(values)} rows.")

if __name__ == "__main__":
    run_transfer()
