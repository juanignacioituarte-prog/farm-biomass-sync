import os
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage

# -----------------------------
# SETTINGS
# -----------------------------
GCS_BUCKET = "ndvi-exports"      
GCS_FILE_PREFIX = "latest_biomass"  
SHEET_ID = "1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04"
SHEET_NAME = "Sheet1"

# Secret
GCP_KEY_JSON = os.environ["EE_KEY"]

# -----------------------------
# INIT CLIENTS
# -----------------------------
credentials = service_account.Credentials.from_service_account_info(json.loads(GCP_KEY_JSON))
storage_client = storage.Client(credentials=credentials)
sheets_service = build('sheets', 'v4', credentials=credentials)

# -----------------------------
# GET LATEST CSV FROM GCS
# -----------------------------
bucket = storage_client.bucket(GCS_BUCKET)
blobs = list(bucket.list_blobs(prefix=GCS_FILE_PREFIX))

if not blobs:
    raise ValueError(f"No CSV files found in bucket '{GCS_BUCKET}' with prefix '{GCS_FILE_PREFIX}'")

latest_blob = max(blobs, key=lambda b: b.time_created)
print("✅ Downloading:", latest_blob.name)
data_bytes = latest_blob.download_as_bytes()

# Read CSV into pandas
df = pd.read_csv(pd.io.common.BytesIO(data_bytes))

# Keep only the 4 columns in the specified order
df = df[['paddock_name', 'date', 'ndvi_effective', 'cloud_pc']]

# -----------------------------
# FIND LAST ROW IN SHEET
# -----------------------------
sheet = sheets_service.spreadsheets()
result = sheet.values().get(spreadsheetId=SHEET_ID, range=SHEET_NAME).execute()
existing_values = result.get('values', [])

start_row = len(existing_values) + 1  # next empty row
print(f"📄 Writing to row {start_row} in {SHEET_NAME}")

# -----------------------------
# APPEND TO SHEET WITHOUT HEADERS
# -----------------------------
values = df.values.tolist()  # only data, no headers
body = {"values": values}

sheet.values().update(
    spreadsheetId=SHEET_ID,
    range=f"{SHEET_NAME}!A{start_row}",
    valueInputOption="RAW",
    body=body
).execute()

print(f"✅ {len(values)} rows appended to Sheet {SHEET_ID}")
