#
# made by gunnar 10.20.2025
# refactored by gemini 03.22.2026
#
import os # pulled in for environmen variables
import time # pulled in for calc time taken
import asyncio # pulled in to run asynchronously
import numpy as np # num py we all fw dis
import matplotlib.pyplot as pyplot # pulled in to plot data
from dotenv import load_dotenv # pulled in for .env environment variables
from pymongo import AsyncMongoClient # pulled in for mongo connection

import csvReader # pulled in to read csv file

# tells python to look for .env file
load_dotenv()
URI = os.environ["URI"] # grabs uri from .env
MONGO_USER = os.environ["MONGO_USER"] # grabs username from .env
MONGO_PASS = os.environ["MONGO_PASS"] # grabs password from .env
CSV_FILE = os.environ.get("CSV_FILE", "data/SS1_rederive.csv") # grabs csv file from .env
# Grabs our new environment variable (defaults to False if not found)
COMPARE_TO_CSV = os.environ.get("COMPARE_TO_CSV", "False").lower() == "true" 

async def main():
  try:
    start_time = time.time() # defines start time
    connectionString = "mongodb://%s:%s@%s/" % ( MONGO_USER, MONGO_PASS, URI ) # defines string to connect
    client = AsyncMongoClient(connectionString) # actually connects

    await client.admin.command("ping") # awaits ping
    print("Connected!\n") # then we connected!

    gunguntestCol = client["ztf"]["ss2_gunguntest"] # where gungun collection is
    realCol = client["ztf"]["ss2_gunguntest_2023"] # where "real" collection is
    [header, rows] = csvReader.main(CSV_FILE) # reads csv file and gets header and rows

    compare2CSV = COMPARE_TO_CSV # boolean for csv or mongo data via .env

    gungunCount = await gunguntestCol.count_documents({}) # counts documents on gungun
    print("I found %d objects in gunguntest!" % gungunCount) # prints how many was found

    # Initialize shared variables for our plots
    plot_ssnr, plot_g1, plot_c1 = [], [], []
    current_data_map = {} # Shared map to store our target baseline periods

    if not compare2CSV:
      print("Comparing against current Mongo collection!")
      currentCount = await realCol.count_documents({}) # counts documents on "real"
      print("I found %d objects in current!" % currentCount) # prints how many was found
      
      # query of objects with string value higher than "900"
      query900 = {
        "$and": [
          {"ssnamenr": {"$gt": "900"}},
          {"periods.numberObservations": {"$gt": 50}}
        ]
      }

      # gets these objects from existing    
      currentObjects = realCol.find(query900) # runs custom on "real" collection

      # saves to array to query gungun and real using list comprehension
      realObjects = [object["ssnamenr"] async for object in currentObjects]

      # query for both collections
      queryssnr = {"ssnamenr": {"$in": realObjects}}
      current_cursor = realCol.find(queryssnr)
      gungun_cursor = gunguntestCol.find(queryssnr)

      # build a map of all the current data cursor
      async for obj in current_cursor:
          if "periods" in obj and isinstance(obj["periods"], dict):
              try:
                  # daniel has these deriven into three different periods, we grab 1
                  current_data_map[obj["ssnamenr"]] = obj["periods"]["periods"][1]["period"]
              except (IndexError, KeyError):
                  continue

    else:
      print("Comparing against CSV data!")
      print("I found %d objects in csv!" % len(rows)) # prints how many was found
      
      # csv rows is a numpy array where index 0 is ssnamenr, index 1 is rotper
      csv_ssnrs = rows[:, 0].tolist()
      
      # build map of csv data
      for row in rows:
        try:
            # Store the period as a float so the math calculation works later
            current_data_map[row[0]] = float(row[1])
        except ValueError:
            continue
            
      # set up the gungun cursor to only look for ssnrs from the CSV
      queryssnr = {"ssnamenr": {"$in": csv_ssnrs}}
      gungun_cursor = gunguntestCol.find(queryssnr)

    # ------------------------------------------------------------------
    # Unified Comparison Loop
    # ------------------------------------------------------------------
    async for obj in gungun_cursor:
        ssnr = obj["ssnamenr"]
        # only append if in both
        if ssnr in current_data_map and "periods" in obj:
            try:
                # grab the periods from gungun col
                g_period = obj["periods"]["periods"][1]["period"]
                
                # append the gungundata and the targeted baseline data
                plot_ssnr.append(ssnr)
                plot_g1.append(g_period)
                plot_c1.append(current_data_map[ssnr])
            except (IndexError, KeyError):
                continue
            
    # "accurate" calculation
    # first make them np arrays
    plot_g1 = np.array(plot_g1)
    plot_c1 = np.array(plot_c1)

    if len(plot_g1) == 0:
        print("No matching objects found to compare!")
    else:
        # then we can do the math
        accuracy1 = (plot_g1 == plot_c1).sum() / len(plot_g1)
        print(f"how accurate .12-2hr: {accuracy1}")

        # how many are on the line tho?
        on_line1 = np.isclose(plot_g1, plot_c1, rtol=0.1).sum() / len(plot_g1)
        print(f"On line for period 1: {on_line1}")

        # plotting stuff
        fig, axs = pyplot.subplots(1, figsize=(10, 15))
        fig.tight_layout(pad=5.0)
        
        axs.loglog(plot_g1, plot_c1, color='blue', alpha=0.5)
        axs.set_title(".12 - 5000 hr period")
        axs.set(xlabel='gungun data', ylabel='baseline data (2023 or csv)')

        # Add a y=x line to see deviation
        lims = [np.min([axs.get_xlim(), axs.get_ylim()]), np.max([axs.get_xlim(), axs.get_ylim()])]
        axs.plot(lims, lims, 'k--', alpha=0.75, zorder=0)

        pyplot.show()
    
    total_time = (time.time() - start_time)

    # TODO:
    # do a little math 
    # use array1/array2 where > spec
    # divide by whole len

    print("Closing connection...") # tells us
    await client.close() # it actually closes connection to mongo (best practice)
    print("\nDonesies! It only took %ss\n" % total_time) # prints how long it took

  except Exception as err: # in case error is made
    raise Exception("You done fucked up: ", err)

# actually runs the asynchronous function
asyncio.run(main())