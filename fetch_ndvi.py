import ee
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

# --- AUTHENTICATION ---
with open('credentials.json') as f:
    cred_data = json.load(f)
    service_account_email = cred_data['client_email']

auth = ee.ServiceAccountCredentials(service_account_email, 'credentials.json')
ee.Initialize(auth)
print(f"Authenticated as {service_account_email}")

# 1. Setup Dates (still needed to limit search window)
end_date = datetime.now()
start_date = end_date - timedelta(days=21)

# 2. Load paddocks from GeoJSON (same as JS)
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"
resp = requests.get(GEOJSON_URL)
paddocks = ee.FeatureCollection(resp.json())

# 3. Sentinel‑2 collection (but we will pick only the latest image)
s2_col = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(paddocks)
    .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
    .sort('system:time_start', False)  # newest first
)

# --- SELECT ONLY THE LATEST IMAGE ---
latest_image = s2_col.first()

# --- ANALYSIS ON THE SINGLE IMAGE ---
def analyze_paddock(paddock):
    img_ndvi = latest_image.normalizedDifference(['B8', 'B4']).rename('NDVI')

    # EXACT SAME REDUCER AS JS
    stats = img_ndvi.reduceRegion(
        reducer=ee.Reducer.percentile([10, 90]),
        geometry=paddock.geometry(),
        scale=10
    )

    p10 = ee.Number(stats.get('NDVI_p10'))
    p90 = ee.Number(stats.get('NDVI_p90'))
    spread = p90.subtract(p10)

    # EXACT SAME LOGIC AS JS
    is_partial = (
        spread.gt(0.16)
        .and(p90.gt(0.78))
        .and(p10.lt(0.72))
    )

    return paddock.set({
        'paddock_name': paddock.get('name'),
        'p10_val': p10,
        'p90_val': p90,
        'ndvi_spread': spread,
        'is_partial': is_partial,
        'date': latest_image.date().format('dd/MM/yyyy'),
        'image_id': latest_image.get('system:index'),
        'cloud_pc': latest_image.get('CLOUDY_PIXEL_PERCENTAGE')
    })

# Run analysis
results = paddocks.map(analyze_paddock)

# Pull results to Python
full_list = results.getInfo()

# --- Build tile URLs ---
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
tile_cache = {}

def get_tile_url(image_id):
    if image_id in tile_cache:
        return tile_cache[image_id]

    img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{image_id}")
    ndvi = img.normalizedDifference(['B8', 'B4'])
    map_info = ndvi.getMapId(viz)
    url = map_info['tile_fetcher'].url_format
    tile_cache[image_id] = url
    return url

# --- Build CSV rows ---
rows = []
partial_rows = []

for f in full_list['features']:
    p = f['properties']
    tile_url = get_tile_url(p['image_id'])

    rows.append([
        p['paddock_name'],
        p['date'],
        p['ndvi_mean'] if 'ndvi_mean' in p else None,
        p['cloud_pc'],
        tile_url
    ])

    if p['is_partial'] == 1:
        partial_rows.append([p['paddock_name'], 'Partial'])

# Save CSVs
pd.DataFrame(rows).to_csv('ndvi_data.csv', index=False, header=False)
pd.DataFrame(partial_rows).to_csv('partial.csv', index=False, header=False)

print("Analysis complete using ONLY the latest image.")
