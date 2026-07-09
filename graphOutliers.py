#
# made by fixedotter 14.05.2026
#

import os
import matplotlib.pyplot as plt
#import numpy as np
import pandas as pd
#from dotenv import load_dotenv

if __name__ == "__main__":
  # get the list of csv files
  num_files = len([f for f in os.listdir('./data') if f.startswith('outliers_202') and f.endswith('.csv')])
  print(f"Found {num_files} outlier files to process.")

  # empty dataframe to hold all outliers
  all_outliers_df = pd.DataFrame()

  # put each file into pandas dataframe
  for filename in os.listdir('./data'):
    try:
      if filename.startswith('outliers_202') and filename.endswith('.csv'):
        # grab dataframe from file
        df = pd.read_csv(os.path.join('./data', filename))
        print(f"Processing file: {filename} with {len(df)} outliers.")

        # this just rounds those floats to zero. really just for testing
        df['errorMean'] = df['errorMean'].astype(int)
        df['errorStdDev'] = df['errorStdDev'].astype(int)

        

        # append current to the all outliers dataframe
        all_outliers_df = pd.concat([all_outliers_df, df], ignore_index=True)
    
    # error handling
    except Exception as e:
      print(f"Error processing file {filename}: {e}")
      continue

  # plot each outlier rank over time
  for i, col in enumerate(all_outliers_df.columns):

    # only plot if not ssnamenr
    if col != 'ssnamenr':
      plt.figure(i,figsize=(10,6))
      plt.plot(all_outliers_df[col], all_outliers_df['ssnamenr'], marker='o', linestyle='', label=col)
      plt.title(f'Outliers in {col}')
      plt.xlabel(col[1])
      plt.ylabel('ssnamenr')
      plt.legend()
      plt.grid()
      plt.show()