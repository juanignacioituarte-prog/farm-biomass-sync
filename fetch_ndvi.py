import ee
import requests
import pandas as pd
import json
import csv
import io
import numpy as np
from datetime import datetime, timedelta

# --- AUTHENTICATION ---
with open('credentials.json') as f:
    cred_data = json.load(f)
    service_account_email = cred_data['client_email']

auth = ee.ServiceAccountCredentials(service_account_email, 'credentials.json')
ee.Initialize(auth)

# 1. Setup Dates (Increased range to 90 days to find 3 clear images)
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

FARMS = [
    {
        "name": "paddocks",
        "url": "https://storage.googleapis.com/ndvi-exports/paddocks.geojson",
        "db_file": "ndvi_data.csv"
    },
    {
        "name": "wainono",
        # Boundaries now come from the "Paddock Boundaries" tab of the feed sync sheet
        # (published CSV) instead of the static wainono.geojson export.
        "csv_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ1OhQkUXzp_TuFwnePsBSp2XlHE7Pw165eReEsOUyLwSldUuvviIdx-M8j0bbII2SYc7trwpjfM6aA/pub?gid=1320161965&single=true&output=csv",
        "db_file": "ndvi_data_wainono.csv"
    }
]

def load_boundaries_from_csv(url):
    """Build a GeoJSON FeatureCollection from a published Google Sheet.

    Expects the columns paddockId, name, farmId, calcArea, geometryType, geometryJson,
    where geometryJson holds the raw coordinates array for the given geometryType.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    resp.encoding = 'utf-8'

    features = []
    skipped = 0
    for row in csv.DictReader(io.StringIO(resp.text)):
        name = (row.get('name') or '').strip()
        raw_geom = (row.get('geometryJson') or '').strip()
        if not name or not raw_geom:
            skipped += 1
            continue

        try:
            coords = json.loads(raw_geom)
        except json.JSONDecodeError:
            print(f"  Skipping {name}: geometryJson is not valid JSON.")
            skipped += 1
            continue

        try:
            area = float(row.get('calcArea') or 0)
        except ValueError:
            area = 0

        features.append({
            "type": "Feature",
            "geometry": {
                "type": (row.get('geometryType') or 'MultiPolygon').strip(),
                "coordinates": coords
            },
            "properties": {
                "name": name,
                "paddockId": (row.get('paddockId') or '').strip(),
                "farmId": (row.get('farmId') or '').strip(),
                "calcArea": area
            }
        })

    if not features:
        raise ValueError(f"No usable paddock geometries found at {url}")

    print(f"  Loaded {len(features)} paddocks from sheet ({skipped} rows skipped).")
    return {"type": "FeatureCollection", "features": features}

def load_boundaries(farm):
    if farm.get('csv_url'):
        return load_boundaries_from_csv(farm['csv_url'])
    resp = requests.get(farm['url'])
    resp.raise_for_status()
    return resp.json()

def process_paddocks(paddock, img_ndvi):
    geom = paddock.geometry()
    area = geom.area().divide(10000)

    stats = img_ndvi.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom,
        scale=10
    )

    return paddock.set({
        'paddock_name': paddock.get('name'),
        'ndvi_mean': stats.get('NDVI_mean'),
        'area_ha': area
    })

for farm in FARMS:
    try:
        print(f"Processing farm: {farm['name']}...")
        paddocks = ee.FeatureCollection(load_boundaries(farm))

        s2_col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(paddocks)
            .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
            .sort('system:time_start', False)
        )

        # Get the last 3 images
        image_list = s2_col.toList(3)
        count = image_list.length().getInfo()
        
        if count == 0:
            print(f"No clear images found for {farm['name']}. Skipping.")
            continue

        all_rows = []

        for i in range(count):
            image = ee.Image(image_list.get(i))
            img_date = image.date().format('dd/MM/yyyy').getInfo()
            cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
            img_ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')

            print(f" Analyzing image {i+1}/{count} from {img_date}...")
            analyzed_features = paddocks.map(lambda p: process_paddocks(p, img_ndvi)).getInfo()

            viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
            tile_url = img_ndvi.getMapId(viz)['tile_fetcher'].url_format

            for f in analyzed_features['features']:
                p = f['properties']
                m_val = p['ndvi_mean'] if p['ndvi_mean'] is not None else ""
                
                # Format: Paddock Name, Date, NDVI, Cloud%, TileURL
                all_rows.append([p['paddock_name'], img_date, m_val, cloud_pc, tile_url])

        pd.DataFrame(all_rows).replace([np.nan, 'NaN'], '', regex=True).to_csv(farm['db_file'], index=False, header=False)
        print(f"Successfully saved {count} dates for {farm['name']}.")
        
    except Exception as e:
        print(f"Error processing {farm['name']}: {e}")
