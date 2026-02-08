import ee
import json

# Initialize Earth Engine
service_account_path = 'service-account.json'
with open(service_account_path) as f:
    credentials = json.load(f)
ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

# 1. LOAD ASSET
allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')

# 2. SET UP COLLECTION
start_date = ee.Date(ee.Date.fromYMD(2026, 2, 8).advance(-14, 'day'))
end_date = ee.Date.fromYMD(2026, 2, 8)

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

# --- STEP A: CALCULATE NDVI (SERVER SIDE) ---
def calculate_ndvi_stats(image):
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
        return paddock.set({
            'name': paddock.get('name'), # Explicitly keep the paddock name
            'date': date,
            'ndvi_effective': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'latest-update': update_time,
            'image_id': image.id()
        })
    
    return allPaddocks.map(stats_per_paddock)

results = collection.map(calculate_ndvi_stats).flatten()
results = results.filter(ee.Filter.notNull(['ndvi_effective']))

# --- STEP B: GENERATE MAP IDs (CLIENT SIDE) ---
print("Calculating Map IDs...")
unique_images = collection.toList(collection.size())
image_list = unique_images.getInfo() 

map_id_dict = {}
viz_params = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}

for img_info in image_list:
    img = ee.Image(img_info['id'])
    ndvi_layer = img.normalizedDifference(['B8', 'B4'])
    map_id_dict[img_info['id']] = ndvi_layer.getMapId(viz_params)['mapid']

# --- STEP C: ATTACH MAP IDs TO RESULTS ---
def attach_map_id(feature):
    img_id = feature.get('image_id')
    ee_map_dict = ee.Dictionary(map_id_dict)
    return feature.set('map_id', ee_map_dict.get(img_id))

final_results = results.map(attach_map_id)

# 3. EXPORT
task = ee.batch.Export.table.toCloudStorage(
    collection=final_results,
    description='Paddock_NDVI_Corrected',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['name', 'date', 'ndvi_effective', 'cloud_pc', 'latest-update', 'map_id']
)

task.start()
print("🚀 Export started. Paddock 'name' and 'map_id' are both included.")
