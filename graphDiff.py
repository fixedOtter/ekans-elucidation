#
# written by fixedotter on 22.04.2026
#

import re
import json
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def process_files():
  # first grabbing the actual json files
  files_list = glob.glob('./data/ztf.ztf_202*_derived.json')

  all_data = []
  
  # making sure we have the right files lol
  print(f"Found {len(files_list)} files to process.")
  for file in files_list:
    # same here - kinda just verifying
    print(f"Processing file: {file}")

    with open(file, 'r') as f:
      # grab the json data from the file
      data = json.load(f)
      
    # grabbing the year from the filename
    year = re.search(r'(202\d)', file).group(1)
    print(f"Extracted year: {year} from filename.")

    # actually grabbing individual objects from the json data
    for obj in data:

      # doing our best!
      try:
        # getting color
        H_g = obj["phaseCurve"]["g"]["H"]
        H_r = obj["phaseCurve"]["r"]["H"]
        color = H_g - H_r

        # getting period
        period = obj["periods"]["periods"][1]["period"]

        # only appending if we have both color and period, otherwise we skip
        if not pd.isnull(color) and not pd.isnull(period):
          all_data.append({"ssnamenr": obj["ssnamenr"], "color": color, "period": period, "year": year})

      # error handling
      except KeyError as e:
        print(f"Missing key {e} in object with ssnamenr {obj.get('ssnamenr', 'unknown')}. Skipping this object.")
        continue

  # converting to pandas dataframe
  df = pd.DataFrame(all_data)
  return df

def plot_color_vs_period(df):
  plt.figure(figsize=(10, 6))
  scatter = plt.scatter(df['period'], df['color'], c=df['year'].astype(int), cmap='viridis', alpha=0.7)
  plt.colorbar(scatter, label='Year')
  plt.xlabel('Period (hrs)')
  plt.ylabel('Color (g - r)')
  plt.title('Period vs Color for Objects')
  plt.grid(True)
  plt.show()

def plot_period_vs_period(df):
  plt.figure(figsize=(10, 6))
  scatter = plt.scatter(df['period'], df['year'], alpha=0.7) # c=df['year'].astype(int), cmap='viridis',
  plt.colorbar(scatter, label='Year')
  plt.xlabel('Period (hrs)')
  plt.ylabel('Year')
  plt.title('Period vs Year for Objects')
  plt.grid(True)
  plt.show()

def plot_experiment(df):
  years = df['year'].astype(int)
  plt.figure(figsize=(10, 6))
  year1 = years.min()
  year2 = years.min() + 1

  plt.scatter(df[df['year'] == str(year1)]['period'], df[df['year'] == str(year2)]['period'], label=f'Year {year1}', alpha=0.7)
  plt.xlabel(f'Period in Year {year1} (hrs)')
  plt.ylabel(f'Period in Year {year2} (hrs)')
  plt.title(f'Period Comparison Between Years {year1} and {year2}')
  plt.grid(True)
  plt.show()

if __name__ == "__main__":
  df = process_files()
  if not df.empty:
    print("DataFrame created successfully with the following columns:", df.columns.tolist())
    # plot_color_vs_period(df)
    # plot_period_vs_period(df)
    plot_experiment(df)
    print(df.head())
  else:
    print("No valid data found to create DataFrame.")