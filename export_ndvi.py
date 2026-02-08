import ee
import json
import time
import os
import requests

# 1. AUTHENTICATION
# In GitHub Actions, we create this file from the Secret before running
service_account_path = 'service-account.json'

if os.path.exists(service_account_path):
    with open(service_account_path) as f:
        credentials = json.load(f)
    ee.Initialize(
        ee.ServiceAccountCredentials(credentials['client_email'], service_account_path),
        project='ndvi-project-484422'
    )
else:
    # Local fallback
    ee.Initialize(project='ndvi-project-484422')

# 2. FETCH PADDOCKS FROM GEOJSON URL
GEOJSON_URL = "https://storage.googleapis.com/ndvi-exports/paddocks.geojson"

def get_paddocks_from_url(url):
    print(f"Fetching boundaries from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    geojson_data = response.json()
    return ee.FeatureCollection(geojson_data)

allPaddocks = get_paddocks_from_url(GEOJSON_URL)

# 3. SET DATE RANGE & COLLECTION
end_date = ee.Date(time.strftime('%Y-%m-%d'))
start_date = end_date.advance(-14, 'day')

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks.geometry())
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 40)))

# 4. PROCESSING LOGIC
def process_image(image):
    date_str = image.date().format('YYYY-MM-DD')
    update_time = image.date().format('YYYY-MM-DD HH:mm')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    img_id = image.id()
    
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi_mean')
    
    def stats_per_paddock(paddock):
        stats = ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=paddock.geometry(),
            scale=10,
            maxPixels=1e9
        )
        return ee.Feature(None, {
            'paddock_name': paddock.get('name'), 
            'date': date_str,
            'ndvi_mean': stats.get('ndvi_mean'),
            'cloud_pc': cloud_pc,
            'last_update': update_time,
            'image_id': img_id 
        })
    
    return allPaddocks.map(stats_per_paddock)

results_fc = collection.map(process_image).flatten().filter(ee.Filter.notNull(['ndvi_mean']))

# 5. GENERATE MAP TILES
print("Generating Tile URLs...")
unique_ids = ee.List(results_fc.aggregate_array('image_id')).distinct().getInfo()

map_metadata = {}
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for iid in unique_ids:
    img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{iid}")
    map_info = img.normalizedDifference(['B8', 'B4']).getMapId(viz)
    map_metadata[iid] = {
        'tile_url': map_info['tile_fetcher'].url_format
    }

def attach_maps(f):
    img_id = f.get('image_id')
    meta = ee.Dictionary(map_metadata).get(img_id)
    return f.set({'tile_url': ee.Dictionary(meta).get('tile_url')})

final_results = results_fc.map(attach_maps)

# 6. EXPORT
print("Starting Export Task...")
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='NDVI_Paddock_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url']
)

task.start()

# Monitor
while task.active():
    time.sleep(15)
    print(f"⏳ GEE Task Status: {task.status()['state']}")

print(f"Final Status: {task.status()['state']}")
