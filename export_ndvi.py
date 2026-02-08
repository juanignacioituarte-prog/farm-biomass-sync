import ee
import json
import time
import os

# 1. AUTHENTICATION
service_account_path = 'service-account.json'
with open(service_account_path) as f:
    credentials = json.load(f)

ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

# 2. ASSETS
allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')

# Get last 14 days
end_date = ee.Date(time.strftime('%Y-%m-%d'))
start_date = end_date.advance(-14, 'day')

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

# 3. PROCESSING
def process_image(image):
    # Get image-wide properties
    date_str = image.date().format('YYYY-MM-DD')
    update_time = image.date().format('YYYY-MM-DD HH:mm')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    img_id = image.id()
    
    # Calculate NDVI
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi_effective')
    
    # Generate Map ID for this specific image
    viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
    map_info = ndvi_img.getMapId(viz)
    tile_url = map_info['tile_fetcher'].url_format
    map_token = map_info['token']

    def stats_per_paddock(paddock):
        stats = ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=paddock.geometry(),
            scale=10,
            maxPixels=1e9
        )
        # Create a feature for EACH paddock
        return ee.Feature(None, {
            'paddock_name': paddock.get('name'), 
            'date': date_str,
            'ndvi_mean': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'last_update': update_time,
            'tile_url': tile_url,
            'map_token': map_token
        })
    
    return allPaddocks.map(stats_per_paddock)

# Map over images and flatten into a single list of paddock-level features
results = collection.map(process_image).flatten().filter(ee.Filter.notNull(['ndvi_mean']))

# 4. EXPORT
task = ee.batch.Export.table.toCloudStorage(
    collection=results,
    description='NDVI_Paddock_Level_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url', 'map_token']
)

task.start()
print(f"🚀 Task {task.id} started. Monitoring...")

while task.active():
    time.sleep(15)
    print(f"⏳ Status: {task.status()['state']}")

if task.status()['state'] == 'COMPLETED':
    print("✅ Export complete. Individual paddock data is in GCS.")
else:
    print(f"❌ Error: {task.status().get('error_message')}")
