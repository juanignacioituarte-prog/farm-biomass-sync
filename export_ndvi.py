import ee
import json
import os
import datetime

# --------------------------------------------
# AUTHENTICATION (DO NOT EDIT)
# --------------------------------------------
key_json = os.environ["EE_KEY"]

credentials = ee.ServiceAccountCredentials(
    email=json.loads(key_json)["client_email"],
    key_data=key_json
)

ee.Initialize(credentials)

# --------------------------------------------
# NZ PADDOCK NDVI EXPORT
# --------------------------------------------

allPaddocks = ee.FeatureCollection(
    'projects/ndvi-project-484422/assets/myfarm_paddocks'
)

targetNames = [
    'Back TP1','BP1','BP1 Dry','BP3','BP4','BP7','BP8',
    'C1','C2','C3','C4','C5','Centre Left','Centre Right',
    'Cottage','MP 15','MP 16','MP11','MP12','MP13','MP14',
    'MP 17','TP1','TP3','TP5','TP6','TP7','TP8','TP9',
    'TP10','TP11','TP13','TP14','TP15','TP16','TP17',
    'TP18','TP19','TP20','12'
]

paddocks = allPaddocks.filter(
    ee.Filter.inList('name', targetNames)
)

now = ee.Date(datetime.datetime.utcnow())

s2 = (
    ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(paddocks.geometry())
    .filterDate(now.advance(-45, 'day'), now)
    .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', 50))
    .sort('system:time_start', False)
    .first()
)

ndvi = s2.normalizedDifference(['B8', 'B4']).rename('ndvi')

def analyze(feature):
    geom = feature.geometry()
    clipped = ndvi.clip(geom)

    median = ee.Number(
        clipped.reduceRegion(
            ee.Reducer.median(), geom, 10
        ).get('ndvi')
    )

    ungrazed = clipped.gt(median)
    grazed = clipped.lte(median)

    totalArea = geom.area()

    grazedArea = ee.Number(
        ee.Image.pixelArea()
        .updateMask(grazed)
        .reduceRegion(
            ee.Reducer.sum(), geom, 10
        ).get('area')
    )

    percentGrazed = grazedArea.divide(totalArea).multiply(100)

    effectiveNdvi = ee.Algorithms.If(
        percentGrazed.gte(15),
        clipped.updateMask(ungrazed)
        .reduceRegion(ee.Reducer.mean(), geom, 10)
        .get('ndvi'),
        clipped.reduceRegion(ee.Reducer.mean(), geom, 10)
        .get('ndvi')
    )

    return feature.set({
        'paddock_name': feature.get('name'),
        'ndvi_effective': effectiveNdvi,
        'percent_grazed': percentGrazed,
        'cloud_pc': s2.get('CLOUDY_PIXEL_PERCENTAGE'),
        'date': s2.date().format('YYYY-MM-dd')
    })

output = paddocks.map(analyze)

task = ee.batch.Export.table.toDrive(
    collection=output,
    description='Farm_Biomass_Update',
    folder='FarmData',
    fileNamePrefix='latest_biomass',
    fileFormat='CSV',
    selectors=[
        'paddock_name',
        'date',
        'ndvi_effective',
        'percent_grazed',
        'cloud_pc'
    ]
)

task.start()

print("✅ NDVI export task started")
