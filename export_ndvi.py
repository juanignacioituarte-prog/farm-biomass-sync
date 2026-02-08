import ee
import json
import time
import os

# 1. AUTHENTICATION
service_account_path = 'service-account.json'
if not os.path.exists(service_account_path):
    print("❌ service-account.json missing!")
    exit(1)

with open(service_account_path) as f:
    credentials = json.load(f)

ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

# 2. ASSETS & DYNAMIC DATES
allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')

# Get last 14 days automatically
end_date = ee.Date(time.strftime('%Y-%m-%d')) 
start_date = end_date.advance(-14, 'day')

print(f"📅 Running for range: {start_date.format('YYYY-MM-DD').getInfo()} to {end_date.format('YYYY-MM-DD').getInfo()}")

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

# 3. PROCESSING
def process_image(image):
    date = image.date().format('YYYY-MM-DD')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    update_time = image.date().format('YYYY-MM-DD HH:mm')
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
            'name': paddock.get('name'), 
            'date': date,
            'ndvi_effective': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'latest-update': update_time,
            'image_id': img_id
        })
    
    return allPaddocks.map(stats_per_paddock)

results = collection.map(process_image).flatten().filter(ee.Filter.notNull(['ndvi_effective']))

# 4. MAP ID GENERATION
print("Calculating Map IDs...")
unique_img_ids = ee.List(results.aggregate_array('image_id')).distinct().getInfo()

map_id_dict = {}
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for img_id in unique_img_ids:
    img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{img_id}")
    map_id_dict[img_id] = img.normalizedDifference(['B8', 'B4']).getMapId(viz)['mapid']

def attach_map_id(f):
    return f.set('map_id', ee.Dictionary(map_id_dict).get(f.get('image_id')))

final_results = results.map(attach_map_id)

# 5. EXPORT & WAIT
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='NDVI_Daily_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['name', 'date', 'ndvi_effective', 'cloud_pc', 'latest-update', 'map_id']
)

task.start()
print(f"🚀 Task {task.id} started. Polling status...")

while task.active():
    time.sleep(30)
    status = task.status()['state']
    print(f"⏳ Status: {status}")

if task.status()['state'] == 'COMPLETED':
    print("✅ Export complete. CSV is ready in GCS.")
else:
    print(f"❌ Export failed: {task.status().get('error_message')}")
    exit(1)
