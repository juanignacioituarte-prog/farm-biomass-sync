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
end_date = ee.Date(time.strftime('%Y-%m-%d'))
start_date = end_date.advance(-14, 'day')

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

# 3. SERVER-SIDE PROCESSING
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

# 4. CLIENT-SIDE MAP ID GENERATION
print("Generating Map IDs...")
unique_ids = ee.List(results_fc.aggregate_array('image_id')).distinct().getInfo()

map_metadata = {}
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for iid in unique_ids:
    img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{iid}")
    # Calculate NDVI again just for the MapId
    map_info = img.normalizedDifference(['B8', 'B4']).getMapId(viz)
    map_metadata[iid] = {
        'tile_url': map_info['tile_fetcher'].url_format,
        'map_token': map_info['token']
    }

# Re-attach metadata to the collection
def attach_maps(f):
    img_id = f.get('image_id')
    meta = ee.Dictionary(map_metadata).get(img_id)
    return f.set({
        'tile_url': ee.Dictionary(meta).get('tile_url'),
        'map_token': ee.Dictionary(meta).get('map_token')
    })

final_results = results_fc.map(attach_maps)

# 5. EXPORT
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='NDVI_Paddock_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['paddock_name', 'date', 'ndvi_mean', 'cloud_pc', 'last_update', 'tile_url', 'map_token']
)

task.start()
print(f"🚀 Task {task.id} started.")
while task.active():
    time.sleep(20)
    print(f"⏳ Status: {task.status()['state']}")
