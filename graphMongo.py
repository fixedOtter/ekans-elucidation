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

# tells python to look for .env file
load_dotenv()
URI = os.environ["URI"] # grabs uri from .env
MONGO_USER = os.environ["MONGO_USER"] # grabs username from .env
MONGO_PASS = os.environ["MONGO_PASS"] # grabs password from .env

async def main():
  try:
    start_time = time.time() # defines start time
    connectionString = "mongodb://%s:%s@%s/" % ( MONGO_USER, MONGO_PASS, URI ) # defines string to connect
    # print("Attempting to connect to Mongo with connection string: %s\n" % connectionString) # debug
    client = AsyncMongoClient(connectionString) # actually connects

    await client.admin.command("ping") # awaits ping
    print("Connected!\n") # then we connected!

    gunguntestCol = client["ztf"]["snapshot_2_derived_properties_gunguntest"] # where gungun collection is
    realCol = client["ztf"]["snapshot_2_derived_properties"] # where "real" collection is

    gungunCount = await gunguntestCol.count_documents({}) # counts documents on gungun
    currentCount = await realCol.count_documents({}) # counts documents on "real"
    print("I found %d objects in gunguntest!" % gungunCount) # prints how many was found
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

    # saves to array to query gungun and real
    realObjects = []
    async for object in currentObjects:
      realObjects.append(object["ssnamenr"])

    # print(realObjects)

    # new and improved query
    queryssnr = {"ssnamenr": {"$in": realObjects}}

    # rewritten code so i don't exhaust the cursors
    current_cursor = realCol.find(queryssnr)
    gungun_cursor = gunguntestCol.find(queryssnr)

    # build a map of all the current data cursor
    # doing this so i'm not double dipping on one cursor
    current_data_map = {}
    async for obj in current_cursor:
        if "periods" in obj and isinstance(obj["periods"], dict):
            try:
                # daniel has these deriven into three different periods
                current_data_map[obj["ssnamenr"]] = [
                    obj["periods"]["periods"][1]["period"],
                    obj["periods"]["periods"][3]["period"],
                    obj["periods"]["periods"][5]["period"]
                ]
            except (IndexError, KeyError):
                continue

    # initialize the arrays
    # we'll append to these as we loop through, and verify they exist in both queries.
    plot_ssnr = []
    plot_g1, plot_c1 = [], []
    plot_g2, plot_c2 = [], []
    plot_g3, plot_c3 = [], []

    async for obj in gungun_cursor:
        ssnr = obj["ssnamenr"]
        # only append if in both
        if ssnr in current_data_map and "periods" in obj:
            try:
                # grab the periods from gungun col
                g_periods = [
                    obj["periods"]["periods"][1]["period"],
                    obj["periods"]["periods"][3]["period"],
                    obj["periods"]["periods"][5]["period"]
                ]
                
                # now we append the gungundata and the current data
                plot_ssnr.append(ssnr)
                
                plot_g1.append(g_periods[0])
                plot_c1.append(current_data_map[ssnr][0])
                
                plot_g2.append(g_periods[1])
                plot_c2.append(current_data_map[ssnr][1])
                
                plot_g3.append(g_periods[2])
                plot_c3.append(current_data_map[ssnr][2])
            except (IndexError, KeyError):
                continue
            
    # "accurate" calculation
      # first make them np arrays
    plot_g1 = np.array(plot_g1)
    plot_c1 = np.array(plot_c1)
    plot_g2 = np.array(plot_g2)
    plot_c2 = np.array(plot_c2)
    plot_g3 = np.array(plot_g3)
    plot_c3 = np.array(plot_c3)
      # then we can do the math
    accuracy1 = (plot_g1 == plot_c1).sum() / len(plot_g1)
    accuracy2 = (plot_g2 == plot_c2).sum() / len(plot_g2)
    accuracy3 = (plot_g3 == plot_c3).sum() / len(plot_g3)
      # printing overall accuracy ( kinda lame )
    print(f"how accurate .12-2hr: {accuracy1}")
    print(f"Accuracy for period 2: {accuracy2}")
    print(f"Accuracy for period 3: {accuracy3}")

      # how many are on the line tho?
    on_line1 = np.isclose(plot_g1, plot_c1, rtol=0.1).sum() / len(plot_g1)
    on_line2 = np.isclose(plot_g2, plot_c2, rtol=0.1).sum() / len(plot_g2)
    on_line3 = np.isclose(plot_g3, plot_c3, rtol=0.1).sum() / len(plot_g3)
      # printing how many are on the line ( more interesting )
    print(f"On line for period 1: {on_line1}")
    print(f"On line for period 2: {on_line2}")
    print(f"On line for period 3: {on_line3}")

    # plotting stuff
    fig, axs = pyplot.subplots(4, figsize=(10, 15))
    fig.tight_layout(pad=5.0)
    
    # plot 1: make sure we actually do have the correct ids
    axs[0].scatter(plot_ssnr, plot_ssnr, alpha=0.5)
    axs[0].set_title("ssnr check")

    # # plot 2: 
    axs[1].scatter(plot_g1, plot_c1, color='blue', alpha=0.5)
    axs[1].set_title(".12 - 2 hr period")
    
    # Plot 3: Period 2 Comparison
    axs[2].scatter(plot_g2, plot_c2, color='green', alpha=0.5)
    axs[2].set_title("2 - 100 hr period")

    # Plot 4: Period 3 Comparison
    # axs[3].scatter(plot_g3, plot_c3, color='red', alpha=0.5)
    # axs[3].set_title("2.1 - 5000 hr period")

    for ax in axs:
        ax.set(xlabel='Gungun Data', ylabel='Daniel Data')
        # Add a y=x line to see deviation
        lims = [np.min([ax.get_xlim(), ax.get_ylim()]), np.max([ax.get_xlim(), ax.get_ylim()])]
        ax.plot(lims, lims, 'k--', alpha=0.75, zorder=0)

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