import os
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = 'credentials.json'
SPREADSHEET_ID = '1yGxWBMOLbWrzxwyMum3UgQkQdkAMra2PlQPBd8eIA04'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The "feed sync" spreadsheet that the Wainono app reads from. Supply the real
# spreadsheet id (the /d/<ID>/edit one, not the published /d/e/2PACX-... key) via
# the FEED_SYNC_SHEET_ID secret, and give the service account Editor access to it.
FEED_SYNC_SHEET_ID = os.environ.get('FEED_SYNC_SHEET_ID', '').strip()

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)

SYNC_CONFIG = [
    {
        "db_csv": "ndvi_data.csv",
        "db_range": "NDVI_Database!A2:E",  # Starts at A2 to keep header
        "partial_csv": "partial.csv",
        "partial_range": "partial!A2:B"    # Starts at A2 to keep header
    },
    {
        "db_csv": "ndvi_data_wainono.csv",
        "db_range": "NDVI_Wainono!A2:E",
        "partial_csv": "partial_wainono.csv",
        "partial_range": "partial_w!A2:B",
        # Also mirrored into the "ndvi" tab (gid 288741439) of the feed sync sheet.
        "feed_sync_gid": 288741439,
        "feed_sync_header": ["paddock", "date", "ndvi", "cloud", "tileUrl"]
    }
]

def quote_title(title):
    """Wrap a tab name for A1 notation, escaping any embedded single quotes."""
    return "'" + title.replace("'", "''") + "'"

def title_for_gid(spreadsheet_id, gid):
    """Look up a tab's current name by its gid, so renames don't break the sync."""
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets.properties(sheetId,title)').execute()
    for sheet in meta.get('sheets', []):
        props = sheet['properties']
        if props['sheetId'] == gid:
            return props['title']
    raise ValueError(f"No tab with gid {gid} in spreadsheet {spreadsheet_id}")

def sync_data():
    for farm in SYNC_CONFIG:
        # 1. OVERWRITE NDVI DATABASE (From Row 2 down)
        if os.path.exists(farm['db_csv']):
            try:
                # Clear existing data from Row 2 downwards
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID, range=farm['db_range']).execute()

                ndvi_df = pd.read_csv(farm['db_csv'], header=None).fillna('')
                ndvi_values = ndvi_df.values.tolist()

                if ndvi_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['db_range'].split(':')[0], # Uses A2 or A2 equivalent
                        valueInputOption='RAW',
                        body={'values': ndvi_values}
                    ).execute()
                    print(f"Overwrote {farm['db_csv']} data (Headers preserved).")
            except Exception as e:
                print(f"Error overwriting {farm['db_csv']}: {e}")

        # 2. OVERWRITE PARTIAL GRAZING (From Row 2 down)
        if os.path.exists(farm['partial_csv']):
            try:
                service.spreadsheets().values().clear(
                    spreadsheetId=SPREADSHEET_ID, range=farm['partial_range']).execute()

                partial_df = pd.read_csv(farm['partial_csv'], header=None).fillna('')
                partial_values = partial_df.values.tolist()

                if partial_values:
                    service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=farm['partial_range'].split(':')[0],
                        valueInputOption='RAW',
                        body={'values': partial_values}
                    ).execute()
                    print(f"Overwrote {farm['partial_csv']} data (Headers preserved).")
            except Exception as e:
                print(f"Error overwriting {farm['partial_csv']}: {e}")

def sync_to_feed_sync():
    """Mirror the NDVI results into the feed sync spreadsheet's own tab."""
    if not FEED_SYNC_SHEET_ID:
        print('No FEED_SYNC_SHEET_ID set. Skipping feed sync sheet.')
        return

    for farm in SYNC_CONFIG:
        gid = farm.get('feed_sync_gid')
        if gid is None or not os.path.exists(farm['db_csv']):
            continue

        try:
            tab = quote_title(title_for_gid(FEED_SYNC_SHEET_ID, gid))

            ndvi_df = pd.read_csv(farm['db_csv'], header=None).fillna('')
            values = ndvi_df.values.tolist()
            if not values:
                print(f"{farm['db_csv']} is empty. Leaving {tab} untouched.")
                continue

            header = farm.get('feed_sync_header')
            if header:
                values.insert(0, header)

            # This tab is owned by the sync, so the whole thing is replaced each run.
            service.spreadsheets().values().clear(
                spreadsheetId=FEED_SYNC_SHEET_ID, range=f'{tab}!A:E').execute()

            service.spreadsheets().values().update(
                spreadsheetId=FEED_SYNC_SHEET_ID,
                range=f'{tab}!A1',
                valueInputOption='RAW',
                body={'values': values}
            ).execute()
            print(f"Wrote {len(values)} rows from {farm['db_csv']} to {tab} in the feed sync sheet.")
        except Exception as e:
            print(f"Error writing {farm['db_csv']} to the feed sync sheet: {e}")

from datetime import datetime

try:
    import psycopg2
    from psycopg2.extras import execute_values
    from dotenv import load_dotenv
    load_dotenv('.env')
    DB_SYNC_AVAILABLE = True
except ImportError as e:
    print(f'Postgres deps unavailable ({e}). Skipping Supabase sync.')
    DB_SYNC_AVAILABLE = False

def sync_to_database():
    if not DB_SYNC_AVAILABLE:
        return

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print('No DATABASE_URL found. Skipping Supabase sync.')
        return
    
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Get all farms
        cursor.execute('SELECT id, name FROM "Farm"')
        farms = {name.lower(): id for id, name in cursor.fetchall()}
        
        # Map of farmId -> {paddock_name: paddockId}
        cursor.execute('SELECT id, "farmId", name FROM "Paddock"')
        paddocks = {}
        for p_id, f_id, p_name in cursor.fetchall():
            if f_id not in paddocks:
                paddocks[f_id] = {}
            paddocks[f_id][p_name.lower().strip()] = p_id

        for farm in SYNC_CONFIG:
            f_name = 'wainono' if 'wainono' in farm['db_csv'] else 'default farm'
            f_id = farms.get(f_name)
            
            if not f_id:
                print(f"Farm not found in DB for {farm['db_csv']}")
                continue
            
            # 1. Sync NDVI
            if os.path.exists(farm['db_csv']):
                ndvi_df = pd.read_csv(farm['db_csv'], header=None).fillna('')
                records_to_insert = []
                for _, row in ndvi_df.iterrows():
                    p_name = str(row[0]).lower().strip()
                    if not p_name: continue
                    p_id = paddocks.get(f_id, {}).get(p_name)
                    if not p_id: continue
                    
                    try:
                        dt = datetime.strptime(str(row[1]), '%d/%m/%Y')
                    except ValueError:
                        continue
                    
                    ndvi = float(row[2]) if str(row[2]).strip() else None
                    cloud = float(row[3]) if str(row[3]).strip() else None
                    tile = str(row[4]) if str(row[4]).strip() else None
                    
                    if ndvi is None: continue
                    
                    cursor.execute('DELETE FROM "PastureRecord" WHERE "paddockId" = %s AND date = %s AND type = %s', (p_id, dt, 'SATELLITE'))
                    
                    import uuid
                    records_to_insert.append((str(uuid.uuid4()), p_id, dt, None, ndvi, None, cloud, tile, 'SATELLITE'))
                
                if records_to_insert:
                    execute_values(cursor, 'INSERT INTO "PastureRecord" (id, "paddockId", date, cover, ndvi, "growthRate", "cloudCover", "tileUrl", type) VALUES %s', records_to_insert)
                    print(f"Inserted {len(records_to_insert)} NDVI records into DB for {farm['db_csv']}")

            # 2. Sync Partials
            if os.path.exists(farm['partial_csv']):
                partial_df = pd.read_csv(farm['partial_csv'], header=None).fillna('')
                cursor.execute('DELETE FROM "PaddockPartial" WHERE "farmId" = %s', (f_id,))
                
                partials_to_insert = []
                for _, row in partial_df.iterrows():
                    p_name = str(row[0]).strip()
                    status = str(row[1]).strip()
                    if not p_name: continue
                    
                    import uuid
                    partials_to_insert.append((str(uuid.uuid4()), f_id, p_name, status))
                
                if partials_to_insert:
                    execute_values(cursor, 'INSERT INTO "PaddockPartial" (id, "farmId", "paddockName", status) VALUES %s', partials_to_insert)
                    print(f"Inserted {len(partials_to_insert)} Partials into DB for {farm['partial_csv']}")

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print('Error syncing to DB:', e)

if __name__ == "__main__":
    sync_data()
    sync_to_feed_sync()
    sync_to_database()
