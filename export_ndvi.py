import ee
import json
import os
import datetime

# ============================================
# AUTHENTICATION (SERVICE ACCOUNT)
# ============================================
# EE_KEY must contain the full service account JSON
key_json = os.environ["EE_KEY"]
key_dict = json.loads(key_json)

credentials = ee.ServiceAccountCredentials(
    email=key_dict["client_email"],
    key_data=key_json
)

ee.Initialize(credentials)

# ============================================
# LOAD PADDOCKS (NZ FARM)
# ============================================
allPaddocks = ee.FeatureCollection(
    "projects/ndvi-project-484422/assets/myfarm_paddocks"
)

targetNames = [
    "Back TP1","BP1","BP1 Dry","BP3","BP4","BP7","BP8",
    "C1","C2","C3","C4","C5","Centre Left","Centre Right",
    "Cottage","MP 15","MP 16","MP11","MP12","MP13","MP14",
    "MP 17","TP1","TP3","TP5","TP6","TP7","TP8","TP9",
    "TP10","TP11","TP13","TP14","TP15","TP16","TP17",
    "TP18","TP19","TP20","12"
]

paddocks = allPaddocks.filter(
    ee.Filter.inList("name", targetNames)
)

# ============================================
# SENTINEL-2 SELECTION (LAST 45 DAYS)
# ============================================
now = ee.Date(datetime.datetime.utcnow())

s2 = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(paddocks.geometry())
    .filterDate(now.advance(-45, "day"), now)
    .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", 50))
    .sort("system:time_start", False)
    .first()
)

# ============================================
# NDVI IMAGE
# ============================================
ndvi = s2.normalizedDifference(["B8", "B4"]).rename("ndvi")

# ============================================
# PER-PADDOCK ANALYSIS
# ============================================
def analyze(feature):
    geom = feature.geometry()
    clipped = ndvi.clip(geom)

    median = ee.Number(
        clipped.reduceRegion(
            ee.Reducer.median(),
            geom,
            scale=10,
            bestEffort=True
        ).get("ndvi")
    )

    ungrazed = clipped.gt(median)
    grazed = clipped.lte(median)

    total_area = geom.area()

    grazed_area = ee.Number(
        ee.Image.pixelArea()
        .updateMask(grazed)
        .reduceRegion(
            ee.Reducer.sum(),
            geom,
            scale=10,
            bestEffort=True
        ).get("area")
    )

    percent_grazed = grazed_area.divide(total_area).multiply(100)

    effective_ndvi = ee.Algorithms.If(
        percent_grazed.gte(15),
        clipped.updateMask(ungrazed)
        .reduceRegion(
            ee.Reducer.mean(),
            geom,
            scale=10,
            bestEffort=True
        ).get("ndvi"),
        clipped.reduceRegion(
            ee.Reducer.mean(),
            geom,
            scale=10,
            bestEffort=True
        ).get("ndvi")
    )

    return feature.set({
        "paddock_name": feature.get("name"),
        "ndvi_effective": effective_ndvi,
        "percent_grazed": percent_grazed,
        "cloud_pc": s2.get("CLOUDY_PIXEL_PERCENTAGE"),
        "image_date": s2.date().format("YYYY-MM-dd")
    })

output = paddocks.map(analyze)

# ============================================
# EXPORT PADDOCK STATS TO GCS
# ============================================
GCS_BUCKET = "ndvi-exports"

stats_task = ee.batch.Export.table.toCloudStorage(
    collection=output,
    description="Farm_NDVI_Stats",
    bucket=GCS_BUCKET,
    fileNamePrefix="latest_biomass",
    fileFormat="CSV"
)

stats_task.start()

# ============================================
# GENERATE MAP ID (CLIENT-SIDE)
# ============================================
vis_params = {
    "min": -0.1,
    "max": 0.9,
    "palette": ["brown", "yellow", "green"]
}

map_info = ndvi.getMapId(vis_params)

map_id = map_info["mapid"]
map_token = map_info["token"]
timestamp = datetime.datetime.utcnow().isoformat()

# ============================================
# EXPORT MAP METADATA TO GCS
# ============================================
metadata_fc = ee.FeatureCollection([
    ee.Feature(None, {
        "map_id": map_id,
        "map_token": map_token,
        "timestamp_utc": timestamp,
        "image_date": s2.date().format("YYYY-MM-dd"),
        "cloud_pc": s2.get("CLOUDY_PIXEL_PERCENTAGE")
    })
])

metadata_task = ee.batch.Export.table.toCloudStorage(
    collection=metadata_fc,
    description="NDVI_Map_Metadata",
    bucket=GCS_BUCKET,
    fileNamePrefix="latest_map_metadata",
    fileFormat="CSV"
)

metadata_task.start()

print("✅ NDVI paddock stats export started")
print("🗺️ NDVI map metadata export started")
print("📦 GCS bucket:", GCS_BUCKET)
