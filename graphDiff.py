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

def get_matching_objects(df, year1, year2):
  # filter the dataframe to only include objects from the specified years
  filtered_df1 = df[df['year'] == str(year1)]
  filtered_df2 = df[df['year'] == str(year2)]

  # merge the two dataframes on ssnamenr
  matching_objects = filtered_df1.merge(filtered_df2, on='ssnamenr') # suffixes=('_1', '_2')

  return matching_objects

def plot_color_vs_period(df):
  plt.figure(figsize=(10, 6), layout='constrained')
  scatter = plt.scatter(df['period'], df['color'], c=df['year'].astype(int), cmap='viridis', alpha=0.7)
  plt.colorbar(scatter, label='Year')
  plt.xlabel('Period (hrs)', fontsize=18, fontweight='bold')
  plt.ylabel('Color (g - r)', fontsize=18, fontweight='bold')
  plt.title('Period vs Color for Objects', fontsize=24, fontweight='bold')
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
  # plot
  plt.figure(figsize=(10, 6))

  # scatter all the shit
  scatter = plt.scatter(df['period_x'], df['period_y'], alpha=0.2, c="darkblue", s=20)
  
  # putting it in log log 
  plt.xscale("log")
  plt.yscale("log")

  # some labels and title
  plt.xlabel(f'Period in Year {df["year_x"].iloc[0]} (hrs)')
  plt.ylabel(f'Period in Year {df["year_y"].iloc[0]} (hrs)')
  plt.title(f'Period Comparison Between Years {df["year_x"].iloc[0]} and {df["year_y"].iloc[0]}')
  
  # raylee said no grid
  # plt.grid(True)

  # custom ticks
  ax = plt.gca()
  custom_ticks = [2, 12, 24, 48, 100, 1000, 5000]
  custom_labels = ['2 hr', '12 hrs', '24 hrs', '48 hrs', '100 hrs', '1k hrs', '5k hrs']
  ax.set_xticks(custom_ticks)
  ax.set_xticklabels(custom_labels)
  ax.set_yticks(custom_ticks)
  ax.set_yticklabels(custom_labels)

  plt.show()

def plot_2_2(df_array):
  # custom ticks
  custom_ticks_y = [2, 12, 24, 48, 100, 1000, 5000]
  custom_labels_y = ['2 hr', '12 hrs', '24 hrs', '48 hrs', '100 hrs', '1k hrs', '5k hrs']
  custom_ticks_x = [2, 24, 100, 1000, 5000]
  custom_labels_x = ['2 hr', '24 hrs', '100 hrs', '1k hrs', '5k hrs']

  fig, axs = plt.subplots(2,2, sharex=True, sharey=True, layout='constrained') #, figsize=(15, 12)
  fig.suptitle('Period Comparisons Between Different Year Pairs', fontsize=24, fontweight='bold')

  # grid plotting
  axs[0,0].scatter(df_array[0]['period_x'], df_array[0]['period_y'], alpha=0.1, c="blue", s=20)
  axs[0,0].set_title(f'2020 vs 2021', fontsize=18, fontweight='bold')

  axs[0,1].scatter(df_array[1]['period_x'], df_array[1]['period_y'], alpha=0.1, c="orange", s=20)
  axs[0,1].set_title(f'2021 vs 2022', fontsize=18, fontweight='bold')

  axs[1,0].scatter(df_array[2]['period_x'], df_array[2]['period_y'], alpha=0.1, c="green", s=20)
  axs[1,0].set_title(f'2022 vs 2023', fontsize=18, fontweight='bold')

  axs[1,1].scatter(df_array[3]['period_x'], df_array[3]['period_y'], alpha=0.1, c="red", s=20)
  axs[1,1].set_title(f'2020 vs 2023', fontsize=18, fontweight='bold')

  # things we donig to all the plots
  for ax in axs.flatten():
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.tick_params(labelsize=12)
    ax.set_xticks(custom_ticks_x)
    ax.set_xticklabels(custom_labels_x)
    ax.set_yticks(custom_ticks_y)
    ax.set_yticklabels(custom_labels_y)

  plt.show()


if __name__ == "__main__":
  df = process_files()
  if not df.empty:
    print("DataFrame created successfully with the following columns:", df.columns.tolist())
    plot_color_vs_period(df)
    # plot_period_vs_period(df)
    matched1 = get_matching_objects(df, 2020, 2021)
    matched2 = get_matching_objects(df, 2021, 2022)
    matched3 = get_matching_objects(df, 2022, 2023)
    matched4 = get_matching_objects(df, 2020, 2023)

    # print(f"Found {len(matched)} matching objects between 2022 and 2023.")
    # print([matched1.head(), matched2.head(), matched3.head(), matched4.head()])
    plot_2_2([matched1, matched2, matched3, matched4])
    # print(all_matched.head())
  else:
    print("No valid data found to create DataFrame.")