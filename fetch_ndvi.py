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

# 1. Setup Dates (Increased range to 90 days to find 3 clear images)
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

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

    is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))

    return paddock.set({
        'paddock_name': paddock.get('name'),
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
        all_partials = []

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

                # Only flag partials for the absolute LATEST image (index 0)
                if i == 0 and p['is_partial'] == 1 and p['area_ha'] > 3.0:
                    all_partials.append([p['paddock_name'], 'Partial'])

        pd.DataFrame(all_rows).replace([np.nan, 'NaN'], '', regex=True).to_csv(farm['db_file'], index=False, header=False)
        pd.DataFrame(all_partials).replace([np.nan, 'NaN'], '', regex=True).to_csv(farm['partial_file'], index=False, header=False)
        print(f"Successfully saved {count} dates for {farm['name']}.")
        
    except Exception as e:
        print(f"Error processing {farm['name']}: {e}")
