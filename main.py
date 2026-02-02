

# 2. Define Assets (Exact match to your script)
all_paddocks = ee.FeatureCollection('projects/ndvi-project-484422/assets/myfarm_paddocks')
target_names = [
    'Back TP1', 'BP1', 'BP1 Dry', 'BP3', 'BP4', 'BP7', 'BP8', 'C1', 'C2', 'C3', 
    'C4', 'C5', 'Centre Left', 'Centre Right', 'Cottage', 'MP 15', 'MP 16', 
    'MP11', 'MP12', 'MP13', 'MP14', 'TP1', 'TP10', 'TP11', 'TP13', 'TP14', 
    'TP15', 'TP16', 'TP17', 'TP18', 'TP19', 'TP20', 'TP5', 'TP6', 'TP7','MP 17','TP3','12', 'TP8', 'TP9'
]
paddocks = all_paddocks.filter(ee.Filter.in_list('name', target_names))

# 3. Get Latest Sentinel-2 Image
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
    
    # 1. Strip Line Logic
    median_dict = ndvi.reduceRegion(
        reducer=ee.Reducer.median(),
        geometry=geom,
        scale=10
    )
    median_val = ee.Number(median_dict.get('ndvi'))
    
    ungrazed_mask = ndvi.gt(median_val)
    grazed_mask = ndvi.lte(median_val)
    
    # 2. Area Calculation (15% Rule)
    total_area = geom.area()
    grazed_area = ee.Image.pixelArea().updateMask(grazed_mask).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geom,
        scale=10
    ).get('area')
    
    percent_grazed = ee.Number(grazed_area).divide(total_area).multiply(100)
    
    # 3. Final NDVI Selection
    final_ndvi = ee.Algorithms.If(
        percent_grazed.gte(15),
        ndvi.updateMask(ungrazed_mask).reduceRegion(
            reducer=ee.Reducer.mean(), geometry=geom, scale=10
        ).get('ndvi'),
        ndvi.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=geom, scale=10
        ).get('ndvi')
    )
    
    return feature.set({
        'paddock_name': feature.get('name'),
        'date': s2.date().format('YYYY-MM-dd'),
        'ndvi_effective': final_ndvi,
        'percent_grazed': percent_grazed,
        'cloud_pc': s2.get('CLOUDY_PIXEL_PERCENTAGE')
    })

# 4. Map Analysis and Export
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
print("Task Started: GEE is now processing the latest biomass data.")
