import ee
import datetime
import os
import json

# --- 1. AUTHENTICATION (The "Login" Step) ---
gee_json = os.getenv('GEE_JSON')

if gee_json:
    gee_key = json.loads(gee_json)
    credentials = ee.ServiceAccountCredentials(
        gee_key['client_email'], 
        key_data=gee_json
    )
    ee.Initialize(credentials, project='ndvi-project-484422')
    print(f"Logged in as: {gee_key['client_email']}")
else:
    # This part runs if you test it on your local computer
    ee.Initialize(project='ndvi-project-484422')

# --- 2. YOUR FARM LOGIC (Exactly as your script) ---
all_paddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')
target_names = ['Back TP1', 'BP1', 'BP1 Dry', 'BP3', 'BP4', 'BP7', 'BP8', 'C1', 'C2', 'C3', 'C4', 'C5', 'Centre Left', 'Centre Right', 'Cottage', 'MP 15', 'MP 16', 'MP11', 'MP12', 'MP13', 'MP14', 'TP1', 'TP10', 'TP11', 'TP13', 'TP14', 'TP15', 'TP16', 'TP17', 'TP18', 'TP19', 'TP20', 'TP5', 'TP6', 'TP7','MP 17','TP3','12', 'TP8', 'TP9']
paddocks = all_paddocks.filter(ee.Filter.inList('name', target_names))

now = ee.Date(datetime.datetime.now())
s2 = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
      .filterBounds(paddocks.geometry())
      .filterDate(now.advance(-45, 'day'), now)
      .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 50))
      .sort('system:time_start', False)
      .first())

def process_analysis(feature):
    geom = feature.geometry()
    ndvi = s2.normalizedDifference(['B8', 'B4']).rename('ndvi')
    
    # Strip Line Logic (Median)
    median_dict = ndvi.reduceRegion(reducer=ee.Reducer.median(), geometry=geom, scale=10)
    median_val = ee.Number(median_dict.get('ndvi'))
    
    ungrazed_mask = ndvi.gt(median_val)
    grazed_mask = ndvi.lte(median_val)
    
    total_area = geom.area()
    grazed_area = ee.Image.pixelArea().updateMask(grazed_mask).reduceRegion(
        reducer=ee.Reducer.sum(), geometry=geom, scale=10
    ).get('area')
    
    percent_grazed = ee.Number(grazed_area).divide(total_area).multiply(100)
    
    final_ndvi = ee.Algorithms.If(
        percent_grazed.gte(15),
        ndvi.updateMask(ungrazed_mask).reduceRegion(reducer=ee.Reducer.mean(), geometry=geom, scale=10).get('ndvi'),
        ndvi.reduceRegion(reducer=ee.Reducer.mean(), geometry=geom, scale=10).get('ndvi')
    )
    
    return feature.set({
        'paddock_name': feature.get('name'),
        'date': s2.date().format('YYYY-MM-dd'),
        'ndvi_effective': final_ndvi,
        'percent_grazed': percent_grazed,
        'cloud_pc': s2.get('CLOUDY_PIXEL_PERCENTAGE')
    })

# --- 3. APPLY & EXPORT ---
if s2:
    analyzed_paddocks = paddocks.map(process_analysis)

    task = ee.batch.Export.table.toDrive(
        collection=analyzed_paddocks,
        description='Farm_Biomass_Update',
        folder='FarmData',
        fileNamePrefix='latest_biomass',
        fileFormat='CSV',
        selectors=['paddock_name', 'date', 'ndvi_effective', 'percent_grazed', 'cloud_pc']
    )

    task.start()
    print("Success: Task sent to Google Earth Engine.")
else:
    print("Error: No clear Sentinel-2 images found in the last 45 days.")
