import ee
import requests
import pandas as pd
from datetime import datetime, timedelta

# Initialize Earth Engine
ee.Initialize()

# 1. Setup Dates
end_date = datetime.now()
start_date = end_date - timedelta(days=21)

# 2. Load Shapes
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"
resp = requests.get(GEOJSON_URL)
paddocks = ee.FeatureCollection(resp.json())

# 3. Get Collection for the last 21 days
s2_col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
          .filterBounds(paddocks)
          .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
          .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40)))

def analyze_collection(image):
    # Standard NDVI
    img_ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    img_date = image.date().format('dd/MM/yyyy')
    
    def process_paddocks(paddock):
        stats = img_ndvi.reduceRegion(
            reducer=ee.Reducer.mean().combine(
                reducer=ee.Reducer.percentile([10, 90]),
                sharedInputs=True
            ),
            geometry=paddock.geometry(),
            scale=10
        )
        
        p10 = ee.Number(stats.get('NDVI_p10'))
        p90 = ee.Number(stats.get('NDVI_p90'))
        spread = p90.subtract(p10)
        
        # Detection logic (TP17/MP17 calibrated)
        is_partial = spread.gt(0.16).And(p90.gt(0.78)).And(p10.lt(0.72))
        
        return paddock.set({
            'paddock_name': paddock.get('name'),
            'ndvi_mean': stats.get('NDVI'),
            'is_partial': is_partial,
            'date': img_date
        })
    
    return paddocks.map(process_paddocks)

# Flatten the collection of features into one list
all_results = s2_col.map(analyze_collection).flatten()

# 4. Generate partial.csv (Last 21 days detections)
# We group by name to see if it was partial AT ANY POINT in the last 21 days
partial_detections = all_results.filter(ee.Filter.eq('is_partial', 1)).getInfo()
unique_partials = {}
for f in partial_detections['features']:
    name = f['properties']['paddock_name']
    date = f['properties']['date']
    unique_partials[name] = date # Keeps the most recent date detected

partial_rows = [[name, 'Partial'] for name in unique_partials.keys()]
pd.DataFrame(partial_rows).to_csv('partial.csv', index=False, header=False)

# 5. Generate ndvi_data.csv (Standard Latest Only)
# For the main dashboard, we still only want the single most recent data point per paddock
latest_data = all_results.sort('system:time_start', False)
# (Logic to pick only the single latest per paddock name goes here if needed for clean DB)
# For now, saving all 21 days of history to the DB:
full_list = all_results.getInfo()
ndvi_rows = [[f['properties']['paddock_name'], f['properties']['date'], f['properties']['ndvi_mean']] for f in full_list['features']]
pd.DataFrame(ndvi_rows).to_csv('ndvi_data.csv', index=False, header=False)

print(f"Detections in 21 days: {len(partial_rows)}")
