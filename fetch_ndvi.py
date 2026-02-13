import ee
import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta

# --- AUTHENTICATION ---
with open('credentials.json') as f:
    cred_data = json.load(f)
    service_account_email = cred_data['client_email']

auth = ee.ServiceAccountCredentials(service_account_email, 'credentials.json')
ee.Initialize(auth)

# 1. Setup Dates & Farm Config
end_date = datetime.now()
start_date = end_date - timedelta(days=30) # Increased to 30 days for better coverage

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

    p10 = ee.Number(stats.get('NDVI_p10', 0))
    p90 = ee.Number(stats.get('NDVI_p90', 0))
    mean_val = stats.get('NDVI_mean')
    spread = p90.subtract(p10)

    # Detection logic
    is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

    return paddock.set({
        'paddock_name': paddock.get('name'), # Using "name" from GeoJSON
        'ndvi_mean': mean_val,
        'is_partial': is_partial,
        'area_ha': area
    })

for farm in FARMS:
    try:
        print(f"Processing farm: {farm['name']}...")
        resp = requests.get(farm['url'])
        paddocks = ee.FeatureCollection(resp.json())

        s2_col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(paddocks)
            .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50))
            .sort('system:time_start', False)
        )

        latest_image = s2_col.first()
        if latest_image.getInfo() is None:
            print(f"Skipping {farm['name']}: No clear images found.")
            continue

        img_date = latest_image.date().format('dd/MM/yyyy').getInfo()
        cloud_pc = latest_image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
        img_ndvi = latest_image.normalizedDifference(['B8', 'B4']).rename('NDVI')

        analyzed_features = paddocks.map(lambda p: process_paddocks(p, img_ndvi)).getInfo()

        viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
        tile_url = img_ndvi.getMapId(viz)['tile_fetcher'].url_format

        rows = []
        partial_rows = []

        for f in analyzed_features['features']:
            p = f['properties']
            m_val = p['ndvi_mean'] if p['ndvi_mean'] is not None else ""
            
            rows.append([p['paddock_name'], img_date, m_val, cloud_pc, tile_url, "", ""])

            if p['is_partial'] == 1 and p['area_ha'] > 3.0:
                partial_rows.append([p['paddock_name'], 'Partial'])

        # Sanitize data: replace NaN with empty strings before saving CSV
        pd.DataFrame(rows).replace([np.nan, 'NaN'], '', regex=True).to_csv(farm['db_file'], index=False, header=False)
        pd.DataFrame(partial_rows).replace([np.nan, 'NaN'], '', regex=True).to_csv(farm['partial_file'], index=False, header=False)
        print(f"Successfully processed {farm['name']}")
        
    except Exception as e:
        print(f"Error processing {farm['name']}: {e}")
