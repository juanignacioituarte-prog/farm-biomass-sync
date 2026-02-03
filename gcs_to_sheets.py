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
SHEET_RANGE = "Sheet1!A1"        # Where to write in the sheet

# Secrets
GCP_KEY_JSON = os.environ["EE_KEY"]  # Use same service account JSON

# -----------------------------
# INIT GCP CLIENTS
# -----------------------------
# Storage client
credentials = service_account.Credentials.from_service_account_info(json.loads(GCP_KEY_JSON))
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

# -----------------------------
# WRITE TO GOOGLE SHEETS
# -----------------------------
# Convert to list of lists (Sheets API format)
values = [df.columns.tolist()] + df.values.tolist()

body = {"values": values}

result = sheets_service.spreadsheets().values().update(
    spreadsheetId=SHEET_ID,
    range=SHEET_RANGE,
    valueInputOption="RAW",
    body=body
).execute()

print(f"✅ {result.get('updatedCells')} cells updated in Sheet {SHEET_ID}")
