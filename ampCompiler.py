#
# made by fixedotter on 12.08.2026
#

# package imports
import os
import csv
import time
import logging
import numpy as np
import pandas as pd
import astropy.timeseries as ts
from dotenv import load_dotenv
from pymongo import MongoClient

# what am i trying to do?
# so basically I have these pre compiled measurements in the csv files
# if i want to calculate the amplitude i kinda need to blow them all up and get all the measurements
# then i can calculate the amplitude for each object
# write them to each of the CSV files

# first read the CSV files

# from the CSV files, get all the SSNAMENR values

# time in days, color corrected mag, error on mags, and rotation period in hours
def fit_sine(time, mag, error, period):
  # Convert time to hours
  time = time * 24.0

  # Calculate frequency
  freq = 1.0 / (period / 2.0)

  # Create Lomb-Scargle model
  model = ts.LombScargle(time % (period / 2.0), mag, error)

  # Generate fit over one period
  t_fit = np.linspace(0, period, 1000)
  y_fit = model.model(t_fit, freq)

  # Calculate amplitude
  amplitude = np.max(y_fit) - np.min(y_fit)

  return t_fit, y_fit, amplitude

def probeMONGO(URI, MONGO_USER, MONGO_PASS):
  logging.info(f"Connecting to MongoDB at {URI} with user {MONGO_USER}")
  connectionString = "mongodb://%s:%s@%s/" % ( MONGO_USER, MONGO_PASS, URI )
  client = MongoClient(connectionString)

  # client.admin.command("ping") # ping
  # print("Connected!\n") # then we connected!

  return client



# takes a year and returns the jd time range
def getTimeRange(year):
  logging.info(f"Getting time range for year: {year}")
  match year:
    case 2020:
      return (2458849.5, 2459214.5)  # 2020-01-01 to 2020-12-31
    case 2021:
      return (2459215.5, 2459580.5)  # 2021-01-01 to 2021-12-31
    case 2022:
      return (2459581.5, 2459946.5)  # 2022-01-01 to 2022-12-31
    case 2023:
      return (2459947.5, 2460312.5)  # 2023-01-01 to 2023-12-31
    case 2024:
      return (2460313.5, 2460678.5)  # 2024-01-01 to 2024-12-31
    case _:
      logging.error(f"Year {year} is not supported.")
      return (None, None)  # return nada

# create a function that takes a list of files and processes them

if __name__ == "__main__":
  # logging setup
  logging.basicConfig(filename='./tmp/ampCompiler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  logging.getLogger('pymongo').setLevel(logging.WARNING)  # set pymongo logging to warning to reduce noise
  logging.info("Starting Amplitude Calculation.")

  # how long does this take
  start_time = time.time()

  # tells python to look for .env file
  logging.info("Loading environment variables from .env file.")
  load_dotenv()
  URI = os.environ["URI"] # grabs uri from .env
  MONGO_USER = os.environ["MONGO_USER"] # grabs username from .env
  MONGO_PASS = os.environ["MONGO_PASS"] # grabs password from .env

  client = probeMONGO(URI, MONGO_USER, MONGO_PASS)  # connect to mongo

  client.admin.command("ping") # ping
  logging.info("MongoDB Connected!") # then we connected!

  # how many files to grab
  files = [f for f in os.listdir('./data') if f.startswith('ztf.ztf_20') and f.endswith('_derived_stripped.csv')]
  num_files = len(files)
  logging.info(f"Found {num_files} outlier files to process.")

  # put each file into pandas dataframe
  for file_id, filename in enumerate(files,1):
    try:
      logging.info(f"[{file_id}/{num_files}] Processing file: {filename}")

      # grabbing all the data
      reader = csv.reader(open(os.path.join('./data', filename), 'r'))
      header = next(reader)  # skip header
      rows = [row for row in reader] # grab all rows
      logging.info(f"Read {len(rows)} rows from {filename}.")

      rows = np.array(rows)  # convert to numpy array for easier processing

      # grabbing the individual columns, so it's easier to concat with np
      ssnamenr_list = rows[:, 0]  # convert ssnamenr column
      period_list = rows[:, 1]  # convert rotper column
      color_list = rows[:, 2]  # convert color column

      # here is where i should be doing this with the ssnamenrs
      # at this point, i have a list of ssnamenr's and the filenames
      # i need to get the year from the filename, then get the time range for that year
      # then i need to query the mongo database for the measurements for that ssnamenr in that time range
      # then i can calculate the amplitude using fit_sine


      year = filename.split('.')[1].split('_')[1]  # extract year from filename
      # logging.info(f"Extracted year {year} from filename {filename}.")
      time_range = getTimeRange(int(year))  # get the time range for that year
      # logging.info(f"Time range for year {year}: {time_range[0]} to {time_range[1]}.")

      amplitude_list = []  # to hold amplitudes for each ssnamenr

      for row_id,ssnamenr in enumerate(ssnamenr_list,1):  # loop through each ssnamenr
        ssnamenr = int(ssnamenr)  # convert ssnamenr to int
        # logging.info(f"[{row_id}/{len(rows)}] Processing SSNAMENR: {ssnamenr}")

        query = {
          "$and": [
            {"ssnamenr": ssnamenr},
            {"jd": {"$gte": time_range[0], "$lte": time_range[1]}}
          ]
        }
        # logging.info(f"Querying MongoDB for SSNAMENR {ssnamenr} with time range {time_range[0]} to {time_range[1]}.")
        # logging.info(f"MongoDB Query: {query}")

        # get measurements from mongo
        measurements = list(client["ztf"]["ztf"].find(query)) # pymongo cursor
        # logging.info(f"Found {len(measurements)} measurements for SSNAMENR {ssnamenr} in year {year}.")
        # logging.info(f"Measurements: {measurements[:5]}")  # log first 5 measurements for brevity

        if measurements:
          time_arr = np.array([m['jd']*24.0 for m in measurements])  # convert time to hours
          # logging.info(f"Time array: {time_arr[:5]}")  # log first 5 time values for brevity
          mag = np.array([m['magpsf'] for m in measurements])
          #logging.info(f"Mag array: {mag[:5]}")  # log first 5 mag values for brevity
          error = np.array([m['sigmapsf'] for m in measurements])
          # logging.info(f"Error array: {error[:5]}")  # log first 5 error values for brevity

          # logging.info(f"Rows look like this: {rows[:5]}")  # log first 5 rows for brevity

          period_val = rows[rows[:, 0] == str(ssnamenr), 1][0]  # get the period for this ssnamenr
          period = np.float64(period_val) # convert period to np.float64
          # logging.info(f"Using period {period} for SSNAMENR {ssnamenr}.")

          t_fit, y_fit, amplitude = fit_sine(time_arr, mag, error, period)
          amplitude = str(amplitude)  # convert
          amplitude_list.append(amplitude)
        else:
          logging.warning(f"No measurements found for SSNAMENR {ssnamenr} in year {year}. Appending amplitude as 0.")
          # amplitude_list.append(0)  # append 0 if no measurements found

      # testing to see if the amplitude_list is the same length as the rows
      if len(amplitude_list) != len(rows):
        logging.error(f"Length mismatch: amplitude_list has {len(amplitude_list)} elements, but rows has {len(rows)} elements.")
        raise ValueError("Length of amplitude_list does not match number of rows.")

      # print the first 5 amplitudes for debugging
      # logging.info(f"First 5 amplitudes: {amplitude_list[:5]}")

      # write amplitude back to csv file
      header.append('amplitude')
      rows = np.column_stack((ssnamenr_list, period_list, color_list, amplitude_list))  # add amplitude to rows

      # save header and rows to an updated CSV file
      updated_filename = os.path.join('./data', f'updated_{filename}')
      with open(updated_filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)  # write header
        writer.writerows(rows)  # write rows

    # error handling
    except Exception as e:
      logging.error(f"Error processing file {filename}: {e}")
      continue

  # logging the time it took
  end_time = time.time()
  elapsed_time = end_time - start_time
  logging.info(f"Finished processing {filename} in {elapsed_time:.2f} seconds.")
