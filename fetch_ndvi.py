import ee
import json
import time
import os
import requests

# 1. AUTHENTICATION
service_account_path = 'service-account.json'
if os.path.exists(service_account_path):
    with open(service_account_path) as f:
        credentials = json.load(f)
    ee.Initialize(
        ee.ServiceAccountCredentials(credentials['client_email'], service_account_path),
        project='ndvi-project-484422'
    )
else:
    ee.Initialize(project='ndvi-project-484422')

# 2. FETCH PADDOCKS
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"
def get_paddocks():
    response = requests.get(GEOJSON_URL)
    response.raise_for_status()
    return ee.FeatureCollection(response.json())

allPaddocks = get_paddocks()

# 3. DATE & COLLECTION
end_date = ee.Date(time.strftime('%Y-%m-%d'))
start_date = end_date.advance(-14, 'day')

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks.geometry())
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40)))

# 4. PROCESSING
def process_image(image):
    # Get the date object
    ee_date = image.date()
    
    # Explicitly get year, month, and day as numbers, then format them
    # This bypasses the Joda-Time .format() pattern matching issues
    year = ee_date.get('year').format('%d')
    month = ee_date.get('month').format('%02d')
    day = ee_date.get('day').format('%02d')
    
    date_str = ee.String(year).cat('-').cat(month).cat('-').cat(day)
    
    # For the timestamp, we can use a similar concatenation if needed
    hour = ee_date.get('hour').format('%02d')
    minute = ee_date.get('minute').format('%02d')
    update_time = date_str.cat(' ').cat(hour).cat(':').cat(minute)
    
    # ... rest of your code ...
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    img_id = image.id()
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi_effective')
    
    def stats_per_paddock(paddock):
        stats = ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=paddock.geometry(),
            scale=10,
            maxPixels=1e9
        )
        return ee.Feature(None, {
            'paddock_name': paddock.get('name'), 
            # Use the concatenated strings here
            'date': date_str,
            'ndvi_effective': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'last_update': update_time,
            'image_id': img_id 
        })
    return allPaddocks.map(stats_per_paddock)

# 5. TILE URLS
unique_ids = ee.List(results_fc.aggregate_array('image_id')).distinct().getInfo()
map_metadata = {}
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for iid in unique_ids:
    img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{iid}")
    map_info = img.normalizedDifference(['B8', 'B4']).getMapId(viz)
    map_metadata[iid] = {'tile_url': map_info['tile_fetcher'].url_format}

def attach_maps(f):
    img_id = f.get('image_id')
    meta = ee.Dictionary(map_metadata).get(img_id)
    return f.set({'tile_url': ee.Dictionary(meta).get('tile_url')})

final_results = results_fc.map(attach_maps)

# 6. EXPORT
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='NDVI_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['paddock_name', 'date', 'ndvi_effective', 'cloud_pc', 'last_update', 'tile_url']
)
task.start()
print(f"Task Started: {task.id}")
