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

# 3. Get Collection for the last 21 days
s2_col = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(paddocks)
    .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40))
)

def analyze_collection(image):
    img_ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    img_date = image.date().format('dd/MM/yyyy')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')

    def process_paddocks(paddock):
        # Combined reducer: mean + percentiles
        stats = img_ndvi.reduceRegion(
            reducer=ee.Reducer.mean().combine(
                reducer2=ee.Reducer.percentile([10, 90]),
                sharedInputs=True
            ),
            geometry=paddock.geometry(),
            scale=10
        )

        # Correct reducer output keys
        p10 = ee.Number(stats.get('NDVI_p10'))
        p90 = ee.Number(stats.get('NDVI_p90'))
        spread = p90.subtract(p10)

        # Detection logic
        is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

        return paddock.set({
            'paddock_name': paddock.get('name'),
            'ndvi_mean': stats.get('NDVI_mean'),   # <-- FIXED
            'cloud_pc': cloud_pc,
            'is_partial': is_partial,
            'date': img_date
        })

    return paddocks.map(process_paddocks)

# Flatten the collection of features into one list
all_results = s2_col.map(analyze_collection).flatten()

# 4. Generate partial.csv (Last 21 days detections)
partial_detections = all_results.filter(ee.Filter.eq('is_partial', 1)).getInfo()

unique_partials = {}
for f in partial_detections['features']:
    name = f['properties']['paddock_name']
    unique_partials[name] = 'Partial'

partial_rows = [[name, status] for name, status in unique_partials.items()]
pd.DataFrame(partial_rows).to_csv('partial.csv', index=False, header=False)

# 5. Generate ndvi_data.csv (Latest-first NDVI data)
full_list = all_results.sort('system:time_start', False).getInfo()

ndvi_rows = []
for f in full_list['features']:
    props = f['properties']
    ndvi_rows.append([
        props['paddock_name'],
        props['date'],
        props['ndvi_mean'],
        props['cloud_pc']
    ])

pd.DataFrame(ndvi_rows).to_csv('ndvi_data.csv', index=False, header=False)

print(f"Detections in 21 days: {len(partial_rows)}")
