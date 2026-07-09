#
# made by fixedotter on 28.04.2026
# Optimized for row-by-row CSV output
#

import os
import json
import pandas as pd
import logging

if __name__ == "__main__":
  # logging setup
  logging.basicConfig(filename='./tmp/json2csv.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
  logging.info("Starting JSON to CSV conversion process.")

  # num files
  num_files = len([f for f in os.listdir('./data') if f.startswith('ztf.ztf_202') and f.endswith('_derived.json')])
  logging.info(f"Found {num_files} json files to process.")
  
  # loop through each json file in the directory
  for filename in os.listdir('./data'):
    if filename.startswith('ztf.ztf_202') and filename.endswith('_derived.json'):
      with open(os.path.join('./data', filename), 'r') as f:
        data = json.load(f)

        logging.info(f"Processing file: {filename} with {len(data)} asteroids.")

        # FIX 1: Use a list to hold row data
        file_rows = []

        # looping through each object in the json list
        for index, obj in enumerate(data):
          logging.info(f"now doing mf uhh {obj.get('ssnamenr', 'unknown')}")
          try:
            # grabbing the data safely using .get() or nested checks
            ssnamenr = obj.get('ssnamenr', None)
            
            # Safe digging for period
            period = None
            if 'periods' in obj and 'periods' in obj['periods'] and len(obj['periods']['periods']) > 1:
              period = obj['periods']['periods'][1].get('period', None)
            
            # Safe digging for color calculation
            color = None
            if 'phaseCurve' in obj and 'g' in obj['phaseCurve'] and 'r' in obj['phaseCurve']:
              color = obj['phaseCurve']['g'].get('H', 0) - obj['phaseCurve']['r'].get('H', 0)

          except Exception as e:
            logging.error(f"Error processing asteroid at index {index} in file {filename}: {e}")
            ssnamenr, period, color = None, None, None
            
          # FIX 2: Append a dictionary mapping headers to values for this specific row
          file_rows.append({
            'ssnamenr': ssnamenr,
            'period': period,
            'color': color
          })

        # FIX 3: Feed the list of dictionaries straight to the DataFrame constructor
        df = pd.DataFrame(file_rows)
        
        # writing dataframe to csv (columns will automatically align to dictionary keys)
        header = ['ssnamenr', 'period', 'color']
        output_filename = f'./data/{filename.replace(".json", "_stripped.csv")}'
        df.to_csv(output_filename, index=False, columns=header)

  print("All json files have been processed and converted to csv.")