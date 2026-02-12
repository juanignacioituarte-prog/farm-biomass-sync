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
print(f"✅ Authenticated as {service_account_email}")

# 1. Setup Dates
end_date = datetime.now()
start_date = end_date - timedelta(days=21)

# 2. Load Shapes from GeoJSON
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"
resp = requests.get(GEOJSON_URL)
paddocks = ee.FeatureCollection(resp.json())

# 3. Sentinel‑2 collection
s2_col = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(paddocks)
    .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
)

# --- ADD TILE URL TO EACH IMAGE ---
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

def add_tile_url(image):
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    map_info = ndvi.getMapId(viz)
    tile_url = ee.String(map_info.get('tile_fetcher').get('url_format'))
    return image.set('tile_url', tile_url)

s2_with_tiles = s2_col.map(add_tile_url)

# --- ANALYSIS PER IMAGE ---
def analyze_collection(image):
    img_ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    img_date = image.date().format('dd/MM/yyyy')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    tile_url = image.get('tile_url')

    def process_paddocks(paddock):
        stats = img_ndvi.reduceRegion(
            reducer=ee.Reducer.mean().combine(
                reducer2=ee.Reducer.percentile([10, 90]),
                sharedInputs=True
            ),
            geometry=paddock.geometry(),
            scale=10
        )

        p10 = ee.Number(stats.get('NDVI_p10'))
        p90 = ee.Number(stats.get('NDVI_p90'))
        spread = p90.subtract(p10)

        is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

        return paddock.set({
            'paddock_name': paddock.get('name'),
            'ndvi_mean': stats.get('NDVI_mean'),
            'cloud_pc': cloud_pc,
            'is_partial': is_partial,
            'date': img_date,
            'tile_url': tile_url
        })

    return paddocks.map(process_paddocks)

# Flatten results
all_results = s2_with_tiles.map(analyze_collection).flatten()

# 4. partial.csv
partial_detections = all_results.filter(ee.Filter.eq('is_partial', 1)).getInfo()
unique_partials = {}

for f in partial_detections['features']:
    name = f['properties']['paddock_name']
    unique_partials[name] = 'Partial'

pd.DataFrame(unique_partials.items()).to_csv('partial.csv', index=False, header=False)

# 5. ndvi_data.csv (now includes tile_url)
full_list = all_results.sort('system:time_start', False).getInfo()
rows = []

for f in full_list['features']:
    p = f['properties']
    rows.append([
        p['paddock_name'],
        p['date'],
        p['ndvi_mean'],
        p['cloud_pc'],
        p['tile_url']  # <-- NEW
    ])

pd.DataFrame(rows).to_csv('ndvi_data.csv', index=False, header=False)

print("Tile URLs included. NDVI sync complete.")
