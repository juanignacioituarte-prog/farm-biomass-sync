import os
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage

# -----------------------------
# SETTINGS
# -----------------------------
GCS_BUCKET = "ndvi-exports"      # Your bucket name
GCS_FILE_PREFIX = "latest_biomass"  # Prefix of exported CSVs
SHEET_ID = "1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04"  # Your Sheet ID
SHEET_NAME = "NDVI_Database"              # Sheet tab name

# Secret
GCP_KEY_JSON = os.environ["EE_KEY"]  # Same service account JSON

# -----------------------------
# INIT GCP CLIENTS
# -----------------------------
credentials = service_account.Credentials.from_service_account_info(json.loads(GCP_KEY_JSON))

# Storage client
storage_client = storage.Client(credentials=credentials)

# Google Sheets client
sheets_service = build('sheets', 'v4', credentials=credentials)

# -----------------------------
# GET LATEST CSV FROM GCS
# -----------------------------
bucket = storage_client.bucket(GCS_BUCKET)
blobs = list(bucket.list_blobs(prefix=GCS_FILE_PREFIX))

if not blobs:
    raise ValueError(f"No CSV files found in bucket '{GCS_BUCKET}' with prefix '{GCS_FILE_PREFIX}'")

# Pick the most recent file
latest_blob = max(blobs, key=lambda b: b.time_created)

print("✅ Downloading:", latest_blob.name)
data_bytes = latest_blob.download_as_bytes()

# Read CSV into pandas
df = pd.read_csv(pd.io.common.BytesIO(data_bytes))

# Add a column for the export date for history tracking
df['export_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')

# -----------------------------
# FIND LAST ROW IN SHEET
# -----------------------------
sheet = sheets_service.spreadsheets()
result = sheet.values().get(spreadsheetId=SHEET_ID, range=SHEET_NAME).execute()
existing_values = result.get('values', [])

start_row = len(existing_values) + 1  # next empty row
print(f"📄 Writing to row {start_row} in {SHEET_NAME}")

# -----------------------------
# APPEND TO SHEET
# -----------------------------
values = [df.columns.tolist()] + df.values.tolist()  # include headers
body = {"values": values}

sheet.values().update(
    spreadsheetId=SHEET_ID,
    range=f"{SHEET_NAME}!A{start_row}",
    valueInputOption="RAW",
    body=body
).execute()

print(f"✅ {len(values)-1} rows appended to Sheet {SHEET_ID} starting at row {start_row}")
