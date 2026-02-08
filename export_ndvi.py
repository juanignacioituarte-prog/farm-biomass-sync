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

# 2. ASSETS & COLLECTION
allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')

# Get latest 14 days
end_date = ee.Date(time.time() * 1000)
start_date = end_date.advance(-14, 'day')

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

# 3. PROCESSING
def process_image(image):
    date = image.date().format('YYYY-MM-DD')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    update_time = image.date().format('YYYY-MM-DD HH:mm')
    
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi_effective')
    
    def stats_per_paddock(paddock):
        stats = ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=paddock.geometry(),
            scale=10,
            maxPixels=1e9
        )
        return ee.Feature(None, {
            'name': paddock.get('name'), # Property from your Asset
            'date': date,
            'ndvi_effective': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'latest-update': update_time,
            'image_id': image.id()
        })
    
    return allPaddocks.map(stats_per_paddock)

results = collection.map(process_image).flatten().filter(ee.Filter.notNull(['ndvi_effective']))

# 4. MAP ID GENERATION (Client-side loop)
print("Generating Map IDs...")
image_ids = collection.aggregate_array('system:index').getInfo()
map_id_dict = {}
viz = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for idx in image_ids:
    img = collection.filter(ee.Filter.eq('system:index', idx)).first()
    map_id_dict[idx] = img.normalizedDifference(['B8', 'B4']).getMapId(viz)['mapid']

def attach_map_id(f):
    return f.set('map_id', ee.Dictionary(map_id_dict).get(f.get('image_id')))

final_results = results.map(attach_map_id)

# 5. EXPORT & WAIT
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='NDVI_Sync_Task',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['name', 'date', 'ndvi_effective', 'cloud_pc', 'latest-update', 'map_id']
)

task.start()
print(f"🚀 Task started (ID: {task.id}). Waiting for completion...")

while task.active():
    print(f"⏳ Status: {task.status()['state']}...")
    time.sleep(30)

if task.status()['state'] == 'COMPLETED':
    print("✅ CSV is ready in GCS.")
else:
    print(f"❌ Error: {task.status().get('error_message')}")
    exit(1)
