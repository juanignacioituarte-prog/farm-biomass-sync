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
    # 1. Authenticate
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=creds)

    # 2. Download from GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
        print(f"✅ Downloaded {len(df)} rows from GCS.")
    except Exception as e:
        print(f"❌ Error downloading CSV: {e}")
        return

    # 3. Prevent Duplicates (Key = Paddock Name + Date)
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, 
            range="NDVI_Database!A:B"
        ).execute()
        existing_rows = result.get('values', [])
        existing_keys = {f"{row[0]}_{row[1]}" for row in existing_rows if len(row) >= 2}
    except Exception:
        existing_keys = set()

    # Create unique keys for incoming data
    df['key'] = df['name'].astype(str) + "_" + df['date'].astype(str)
    
    # Filter only new records
    df_new = df[~df['key'].isin(existing_keys)].copy()
    df_new.drop(columns=['key'], inplace=True)

    if df_new.empty:
        print("⏭️ No new unique data found. Sheet is already up to date.")
        return

    # 4. Upload (NO HEADERS)
    # We round NDVI to 4 decimal places for cleanliness
    if 'ndvi_effective' in df_new.columns:
        df_new['ndvi_effective'] = df_new['ndvi_effective'].round(4)

    values = df_new.fillna('').values.tolist()

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='USER_ENTERED',
        insertDataOption='INSERT_ROWS',
        body={'values': values} 
    ).execute()
    
    print(f"✅ Successfully appended {len(values)} new records to Sheets.")

if __name__ == "__main__":
    run_transfer()
