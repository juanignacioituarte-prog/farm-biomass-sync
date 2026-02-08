import ee
import os
import json

# Authenticate using the service account key file
service_account_path = 'service-account.json'
with open(service_account_path) as f:
    credentials = json.load(f)

ee.Initialize(ee.ServiceAccountCredentials(credentials['client_email'], service_account_path))

# Define parameters
# Replace with your actual geometry/coords
roi = ee.Geometry.Point([-122.4194, 37.7749]).buffer(1000) 
collection = (ee.ImageCollection("MODIS/061/MOD13Q1")
              .filterDate('2024-01-01', '2024-12-31')
              .filterBounds(roi))

def extract_data(image):
    stats = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=roi,
        scale=250
    )
    # CRITICAL: Format the date here so it exists in the CSV
    date = image.date().format('YYYY-MM-DD')
    return ee.Feature(None, stats).set('date', date)

data_features = collection.map(extract_data)

# Export to Google Cloud Storage
task = ee.batch.Export.table.toCloudStorage(
    collection=data_features,
    description='NDVI_Daily_Export',
    bucket='ndvi-exports',
    fileNamePrefix='ndvi_data',
    fileFormat='CSV'
)

task.start()
print("✅ Earth Engine task started...")
