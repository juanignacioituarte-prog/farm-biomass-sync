import ee
import json
import time

# Initialize
service_account_path = 'service-account.json'
with open(service_account_path) as f:
    credentials = json.load(f)
ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')
collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(ee.Date('2026-02-01'), ee.Date('2026-02-08'))
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

def process_image(image):
    date = image.date().format('YYYY-MM-DD')
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi')
    
    def extract(feat):
        stats = ndvi_img.reduceRegion(reducer=ee.Reducer.mean(), geometry=feat.geometry(), scale=10)
        return feat.set({'name': feat.get('name'), 'date': date, 'ndvi': stats.get('ndvi'), 'img_id': image.id()})
    
    return allPaddocks.map(extract)

results = collection.map(process_image).flatten().filter(ee.Filter.notNull(['ndvi']))

# 1. Start Export
task = ee.batch.Export.table.toCloudStorage(
    collection=results,
    description='Daily_NDVI_Sync',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['name', 'date', 'ndvi']
)
task.start()
print(f"🚀 Task started: {task.id}")

# 2. POLL UNTIL FINISHED (The "Deep Thought" Fix)
while task.active():
    status = task.status()['state']
    print(f"⏳ Task status: {status}...")
    time.sleep(30)

final_status = task.status()
if final_status['state'] == 'COMPLETED':
    print("✅ Export finished! File is now in GCS.")
else:
    print(f"❌ Export failed: {final_status.get('error_message')}")
    exit(1)
