# created by gunnar march 19 2026

import csv # to read csv file
import numpy as np # to convert to numpy array

def main(filename):
  header = []
  rows = []

  file = filename if filename else "data/SS1_rederive.csv" # default filename if not provided

  # open the csv file
  with open(file, 'r') as data:
    # reader object
    reader = csv.reader(data)
    # header 
    header = next(reader) # read the header
    # get rows
    for row in reader:
      rows.append(row) # add the row to the list of rows

  # now we need to process the data 
  rows = np.array(rows) # convert to numpy array for easier processing
  rows = rows[:, [0, 2]] # grab the ssnamenr and rotper columns
  header = header[0], header[2] # update the header to reflect the columns we grabbed

  # print(header, rows)

  return header, rows