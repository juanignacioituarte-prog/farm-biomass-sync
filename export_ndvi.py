import ee
import json

# Initialize Earth Engine
service_account_path = 'service-account.json'
with open(service_account_path) as f:
    credentials = json.load(f)
ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

# 1. LOAD YOUR ASSET
allPaddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')

# 2. SET UP DATA COLLECTION
# Fetching the last 14 days to keep the file size manageable and data fresh
start_date = ee.Date(ee.Date.fromYMD(2026, 2, 8).advance(-14, 'day'))
end_date = ee.Date.fromYMD(2026, 2, 8)

collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(allPaddocks)
              .filterDate(start_date, end_date)
              .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)))

def process_image(image):
    date = image.date().format('YYYY-MM-DD')
    cloud_pc = image.get('CLOUDY_PIXEL_PERCENTAGE')
    update_time = ee.Date(image.get('system:time_start')).format('YYYY-MM-DD HH:mm')
    
    # Calculate NDVI
    ndvi_img = image.normalizedDifference(['B8', 'B4']).rename('ndvi_effective')
    
    # GENERATE MAP ID for this specific image
    # Visualization params: NDVI usually ranges 0 to 1 for vegetation
    viz_params = {'min': 0, 'max': 1, 'palette': ['red', 'yellow', 'green']}
    map_info = ndvi_img.getMapId(viz_params)
    current_map_id = map_info['mapid']
    
    def calculate_paddock_stats(feature):
        stats = ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e9
        )
        
        return feature.set({
            'paddock_name': feature.get('paddock_name'), # Ensure this matches your asset property
            'date': date,
            'ndvi_effective': stats.get('ndvi_effective'),
            'cloud_pc': cloud_pc,
            'latest-update': update_time,
            'map_id': current_map_id
        })
    
    return allPaddocks.map(calculate_paddock_stats)

# Flatten results
results = collection.map(process_image).flatten()
results = results.filter(ee.Filter.notNull(['ndvi_effective']))

# 3. EXPORT TO GCS
# Added 'map_id' to the selectors
task = ee.batch.Export.table.toCloudStorage(
    collection=results,
    description='Paddock_NDVI_With_MapID',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV',
    selectors=['paddock_name', 'date', 'ndvi_effective', 'cloud_pc', 'latest-update', 'map_id']
)

task.start()
print("🚀 Export started. New MapIDs will be included for each date.")
