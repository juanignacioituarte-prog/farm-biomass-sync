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

# 1. Setup Dates & Farm Config (Increased to 30 days for better coverage)
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

FARMS = [
    {
        "name": "paddocks",
        "url": "https://storage.googleapis.com/ndvi-exports/paddocks.geojson",
        "db_file": "ndvi_data.csv",
        "partial_file": "partial.csv"
    },
    {
        "name": "wainono",
        "url": "https://storage.googleapis.com/ndvi-exports/wainono.geojson",
        "db_file": "ndvi_data_wainono.csv",
        "partial_file": "partial_wainono.csv"
    }
]

def process_paddocks(paddock, img_ndvi):
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

    # Detection logic (Unchanged)
    is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

    return paddock.set({
        'paddock_name': paddock.get('name'),
        'ndvi_mean': stats.get('NDVI_mean'),
        'is_partial': is_partial,
        'area_ha': area
    })

for farm in FARMS:
    try:
        print(f"Processing farm: {farm['name']}...")
        resp = requests.get(farm['url'])
        paddocks = ee.FeatureCollection(resp.json())

        # 2. Get Latest Image for this farm's bounds
        s2_col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(paddocks)
            .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50)) # Slightly relaxed to 50%
            .sort('system:time_start', False)
        )

        latest_image = s2_col.first()
        
        # Check if image exists before proceeding
        if latest_image.getInfo() is None:
            print(f"No clear images found for {farm['name']} in the date range. Skipping.")
            continue

        img_date = latest_image.date().format('dd/MM/yyyy').getInfo()
        cloud_pc = latest_image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
        img_ndvi = latest_image.normalizedDifference(['B8', 'B4']).rename('NDVI')

        # 3. Analysis
        analyzed_features = paddocks.map(lambda p: process_paddocks(p, img_ndvi)).getInfo()

        # 4. Build Tile URL
        viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
        tile_url = img_ndvi.getMapId(viz)['tile_fetcher'].url_format

        # 5. Generate Exports
        rows = []
        partial_rows = []

        for f in analyzed_features['features']:
            p = f['properties']
            rows.append([p['paddock_name'], img_date, p['ndvi_mean'], cloud_pc, tile_url, "", ""])

            if p['is_partial'] == 1 and p['area_ha'] > 3.0:
                partial_rows.append([p['paddock_name'], 'Partial'])

        pd.DataFrame(rows).to_csv(farm['db_file'], index=False, header=False)
        pd.DataFrame(partial_rows).to_csv(farm['partial_file'], index=False, header=False)
        print(f"Successfully created {farm['db_file']}")

    except Exception as e:
        print(f"Error processing {farm['name']}: {e}")
