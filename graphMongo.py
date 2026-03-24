#
# made by gunnar 10.20.2025
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
COMPARE_COL = os.environ["COMPARE_COL"] # grabs compare collection
CSV_FILE = os.environ["CSV_FILE"] # grabs csv file from .env

async def main():
  try:
    start_time = time.time() # defines start time
    connectionString = "mongodb://%s:%s@%s/" % ( MONGO_USER, MONGO_PASS, URI ) # defines string to connect
    client = AsyncMongoClient(connectionString) # actually connects

    await client.admin.command("ping") # awaits ping
    print("Connected!\n") # then we connected!

    gunguntestCol = client["ztf"]["ss1_gunguntest"] # where gungun collection is
    realCol = client["ztf"][COMPARE_COL] # where "real" collection is
    [header, rows] = csvReader.main(CSV_FILE) # reads csv file and gets header and rows

    compare2CSV = True # boolean for csv or mongo data

    gungunCount = await gunguntestCol.count_documents({}) # counts documents on gungun
    print("I found %d objects in gunguntest!" % gungunCount) # prints how many was found

    # initialize the arrays
    # we'll append to these as we loop through, and verify they exist in both queries.
    plot_ssnr, plot_g1, plot_c1 = [], [], []
    current_data_map = {}

    if not compare2CSV:
      print("comparing to mongo")
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
                  current_data_map[obj["ssnamenr"]] = obj["periods"]["periods"][1]["period"] # period
                  # current_data_map[obj["ssnamenr"]] = obj["phaseCurve"]["r"]["H"] # red color absolute magnitude

              except (IndexError, KeyError):
                  continue
        
    else:
      print("comparing to csv")
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

    async for obj in gungun_cursor:
        ssnr = obj["ssnamenr"]
        # only append if in both
        if ssnr in current_data_map and "periods" in obj:
            try:
                # grab the periods from gungun col
                g_period = obj["periods"]["periods"][1]["period"] # period
                # g_period = obj["phaseCurve"]["r"]["H"] # red color abs mag
                
                
                # now we append the gungundata and the current data
                plot_ssnr.append(ssnr)
                plot_g1.append(g_period)
                plot_c1.append(current_data_map[ssnr])
            except (IndexError, KeyError):
                continue
            
    # first make them np arrays
    plot_g1 = np.array(plot_g1)
    plot_c1 = np.array(plot_c1)

    # then we can do the math
    # accuracy = (plot_g1 == plot_c1).sum() / len(plot_g1)
    # print(f"how accurate: %f" % accuracy)

    # how many are on the line tho?
    on_line1 = np.isclose(plot_g1, plot_c1, rtol=0.1).sum() / len(plot_g1)
    print(f"on line: %f" % on_line1)

    # plot things
    ax = pyplot.gca()
    ax.scatter(plot_g1, plot_c1, label="data points", alpha=0.1, c="cyan", s=5)
    # ax.plot(plot_c1, plot_c1, color="blue", label="perfect line", alpha=0.5)

    # lines to show aliasing
    ax.axhline(y=24, color='purple', linestyle='--', alpha=0.25)
    ax.axhline(y=48, color='purple', linestyle='--', alpha=0.25)
    ax.axvline(x=12, color='green', linestyle='--', alpha=0.25)
    ax.axvline(x=24, color='green', linestyle='--', alpha=0.25)
    ax.axvline(x=48, color='green', linestyle='--', alpha=0.25)

    # plotting in log log
    ax.set_yscale("log")
    ax.set_xscale("log")

    # custom gungun tick
    gungun_custom_ticks = [2, 12, 24, 48, 100, 1000, 5000]
    gungun_custom_labels = ['2 hr', '12 hrs', '24 hrs', '48 hrs', '100 hrs', '1k hrs', '5k hrs']
    ax.set_xticks(gungun_custom_ticks)
    ax.set_xticklabels(gungun_custom_labels)

    # custom gowanlock tick
    gowanlock_custom_ticks = [2, 10, 24, 48, 100, 1000, 5000]
    gowanlock_custom_labels = ['2 hr', '10 hrs', '24 hrs', '48 hrs', '100 hrs', '1k hrs', '5k hrs']
    ax.set_yticks(gowanlock_custom_ticks)
    ax.set_yticklabels(gowanlock_custom_labels)

    # details for the plot
    ax.set_xlabel("Gunnar data")
    ax.set_ylabel("Dr Gowanlock data")
    ax.set_title("Dr Gowanlock vs Gunnar data for Period W/ %.2f %% on line within 10%%" % (on_line1*100))
    # ax.legend()

    total_time = (time.time() - start_time)
    pyplot.show()
    

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