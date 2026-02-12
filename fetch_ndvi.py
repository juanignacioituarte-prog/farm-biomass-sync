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

# 1. Setup Dates
end_date = datetime.now()
start_date = end_date - timedelta(days=21)

# 2. Load Shapes
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"
resp = requests.get(GEOJSON_URL)
paddocks = ee.FeatureCollection(resp.json())

# 3. Get Latest Image
s2_col = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(paddocks)
    .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
    .sort('system:time_start', False)
)

latest_image = s2_col.first()
img_date = latest_image.date().format('dd/MM/yyyy').getInfo()
cloud_pc = latest_image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
img_ndvi = latest_image.normalizedDifference(['B8', 'B4']).rename('NDVI')

# 4. Analysis
def process_paddocks(paddock):
    geom = paddock.geometry()
    area = geom.area().divide(10000)
    
    stats = img_ndvi.reduceRegion(
        reducer=ee.Reducer.mean().combine(
            reducer2=ee.Reducer.percentile([10, 90]),
            sharedInputs=True
        ),
        geometry=geom,
        scale=10
    )

    p10 = ee.Number(stats.get('NDVI_p10'))
    p90 = ee.Number(stats.get('NDVI_p90'))
    spread = p90.subtract(p10)

    # Detection logic
    is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

    return paddock.set({
        'paddock_name': paddock.get('name'),
        'ndvi_mean': stats.get('NDVI_mean'),
        'is_partial': is_partial,
        'area_ha': area
    })

analyzed_features = paddocks.map(process_paddocks).getInfo()

# 5. Build Tile URL
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
tile_url = img_ndvi.getMapId(viz)['tile_fetcher'].url_format

# 6. Generate Exports
rows = []
partial_rows = []

for f in analyzed_features['features']:
    p = f['properties']
    
    # ALWAYS add to main rows (regardless of size)
    rows.append([p['paddock_name'], img_date, p['ndvi_mean'], cloud_pc, tile_url, "", ""])

    # ONLY add to partial sheet if detected AND > 3ha
    if p['is_partial'] == 1 and p['area_ha'] > 3.0:
        partial_rows.append([p['paddock_name'], 'Partial'])

# Save
pd.DataFrame(rows).to_csv('ndvi_data.csv', index=False, header=False)
pd.DataFrame(partial_rows).to_csv('partial.csv', index=False, header=False)
print(f"Done. {len(rows)} paddocks updated. {len(partial_rows)} flagged as partial (>3ha).")
