import ee
import json
import time
import os
import requests
import pandas as pd

# ... (Keep your Auth and Process logic from the previous script) ...

# 5. GENERATE TILE URLs & COLLECT DATA
print("Collecting data from GEE...")
# Instead of an export task, we get the data directly to the script
raw_results = final_results.getInfo()
features = raw_results.get('features', [])

data_list = []
for f in features:
    props = f['properties']
    # Calculate cover using your formula (e.g., NDVI * 2500)
    ndvi = props.get('ndvi_mean')
    cover = Math.round(ndvi * 2500) if ndvi is not None else 0
    
    data_list.append({
        'paddock_name': props.get('paddock_name'),
        'date': props.get('date'),
        'cover': cover,
        'ndvi_mean': ndvi,
        'cloud_pc': props.get('cloud_pc'),
        'map_id': props.get('tile_url') # This is the tile_url for your dashboard
    })

# 6. SAVE TO CSV
if data_list:
    df = pd.DataFrame(data_list)
    df.to_csv('paddocks_ndvi.csv', index=False)
    print(f"✅ Successfully saved {len(data_list)} rows to paddocks_ndvi.csv")
else:
    print("⚠️ No data found to save.")
