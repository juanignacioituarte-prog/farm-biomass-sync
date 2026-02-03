import os
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage

# -----------------------------
# SETTINGS
# -----------------------------
GCS_BUCKET = "ndvi-exports"                  # Your GCS bucket
GCS_FILE_PREFIX = "latest_biomass"          # Prefix of exported CSV files
SHEET_ID = "1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04"  # Your Google Sheet ID
SHEET_NAME = "NDVI_Database"                        # Sheet tab name

# Secret: full JSON key for service account stored as GitHub secret EE_KEY
GCP_KEY_JSON = os.environ["EE_KEY"]

# -----------------------------
# INITIALIZE CLIENTS
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
df = pd.read_csv(pd.io.common.BytesIO(data_bytes))

# -----------------------------
# KEEP ONLY THE 4 SPECIFIED COLUMNS
# -----------------------------
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
# APPEND DATA TO SHEET (NO HEADERS)
# -----------------------------
values = df.values.tolist()  # only data rows
body = {"values": values}

sheet.values().update(
    spreadsheetId=SHEET_ID,
    range=f"{SHEET_NAME}!A{start_row}",
    valueInputOption="RAW",
    body=body
).execute()

print(f"✅ {len(values)} rows appended to Sheet {SHEET_ID}")
