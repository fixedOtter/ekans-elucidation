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
    
    currentObjects = realCol.find(queryssnr) # gets ssnamenr objects
    gungunObjects = gunguntestCol.find(queryssnr) # runs query on gungun collection

    # # initializing arrays
    # gungunObjData1 = [[],[]]
    # currentObjData1 = [[],[]]
    # gungunObjData2 = [[],[]]
    # currentObjData2 = [[],[]]
    # gungunObjData3 = [[],[]]
    # currentObjData3 = [[],[]]

    # better arrays
    currentssnr = []
    current1 = []
    current2 = []
    current3 = []
    gungunssnr = []
    gungun1 = []
    gungun2 = []
    gungun3 = []

    # saves object data for current
    async for object in currentObjects:
      ssnr = object["ssnamenr"]
      per1 = object["periods"]["periods"][1]["period"]
      per2 = object["periods"]["periods"][3]["period"]
      per3 = object["periods"]["periods"][5]["period"]

      currentssnr.append(ssnr)
      current1.append(per1)
      current2.append(per2)
      current3.append(per3)


    # saves object data for gungun
    async for object in gungunObjects:
      ssnr = object["ssnamenr"]
      per1 = object["periods"]["periods"][1]["period"]
      per2 = object["periods"]["periods"][3]["period"]
      per3 = object["periods"]["periods"][5]["period"]

      gungunssnr.append(ssnr)
      gungun1.append(per1)
      gungun2.append(per2)
      gungun3.append(per3)

    # plotting stuff
    fig, axs = pyplot.subplots(4)
    fig.suptitle('name v name & period v period')
    axs[0].scatter(gungunssnr, currentssnr, label='ssnamenr')
    axs[1].scatter(gungun1, current1, label='masked light periods .12-2 hr')
    axs[2].scatter(gungun2, current2, label='masked light periods 2-100 hr')
    axs[3].scatter(gungun3, current3, label='masked light periods 2.1-5000 hr')
    for ax in axs.flat:
      ax.set(xlabel='gungun', ylabel='daniel')
      ax.legend()

    pyplot.show()


    # # first clean the queried data
    # async for gObj in gungunObjects:
    #   async for cObj in currentObjects:
    #     # if ssnamenr exists in other array, append
    #     if (cObj["ssnamenr"] == gObj["ssnamenr"]):
    #       print('in if statement')
    #       gper1 = gObj["periods"]["periods"][1]["period"]
    #       gper2 = gObj["periods"]["periods"][3]["period"]
    #       gper3 = gObj["periods"]["periods"][5]["period"]
    #       cper1 = cObj["periods"]["periods"][1]["period"]
    #       cper2 = cObj["periods"]["periods"][3]["period"]
    #       cper3 = cObj["periods"]["periods"][5]["period"]
    #       ssnamenr = gObj["ssnamenr"]

    #       gungunObjData1[0].append(ssnamenr)
    #       gungunObjData2[0].append(ssnamenr)
    #       gungunObjData3[0].append(ssnamenr)
    #       gungunObjData1[1].append(gper1)
    #       gungunObjData2[1].append(gper2)
    #       gungunObjData3[1].append(gper3)

    #       currentObjData1[0].append(ssnamenr)
    #       currentObjData2[0].append(ssnamenr)
    #       currentObjData3[0].append(ssnamenr)
    #       currentObjData1[1].append(cper1)
    #       currentObjData2[1].append(cper2)
    #       currentObjData3[1].append(cper3)

    # async for object in gungunObjects: # loops through all objects in what was found
    #   if (object["periods"] != 0):
    #     per1 = object["periods"]["periods"][1]["period"]
    #     per2 = object["periods"]["periods"][3]["period"]
    #     per3 = object["periods"]["periods"][5]["period"]
    #     ssnamenr = object["ssnamenr"]
    #     gungunObjData1[0].append(ssnamenr)
    #     gungunObjData2[0].append(ssnamenr)
    #     gungunObjData3[0].append(ssnamenr)
    #     gungunObjData1[1].append(per1)
    #     gungunObjData2[1].append(per2)
    #     gungunObjData3[1].append(per3)
    
    # async for object in currentObjects:
    #   if (object["periods"] != 0):
    #     per1 = object["periods"]["periods"][1]["period"]
    #     per2 = object["periods"]["periods"][3]["period"]
    #     per3 = object["periods"]["periods"][5]["period"]
    #     ssnamenr = object["ssnamenr"]
    #     currentObjData1[0].append(ssnamenr)
    #     currentObjData2[0].append(ssnamenr)
    #     currentObjData3[0].append(ssnamenr)
    #     currentObjData1[1].append(per1)
    #     currentObjData2[1].append(per2)
    #     currentObjData3[1].append(per3)

    # for gName in gungunObjData1[0]:
    #   for cName in currentObjData1[0]:
    #     if (gName )
    
    # sort arrays (should sort both of them based on ssnamenr?)
    # gungunObjData1[0], gungunObjData1[1] = zip(*sorted(zip(gungunObjData1[0], gungunObjData1[1])))
    # currentObjData1[0], currentObjData1[1] = zip(*sorted(zip(currentObjData1[0], currentObjData1[1])))


    # makes some stuff for plotting
    # x1 = np.arange(0,len(gungunObjData),1)
    # x2 = np.arange(0,len(currentObjData),1)
    # y1 = np.array(gungunObjData)
    # y2 = np.array(currentObjData)

    # print(x1)

    # fig, axs = pyplot.subplots(3)
    # fig.suptitle('period')
    # axs[0].scatter(gungunObjData1[0], gungunObjData1[1], label='gungun')
    # axs[0].scatter(currentObjData1[0], currentObjData1[1], label='daniel')
    # axs[1].scatter(gungunObjData2[0], gungunObjData2[1], label='gungun')
    # axs[1].scatter(currentObjData2[0], currentObjData2[1], label='daniel')
    # axs[2].scatter(gungunObjData3[0], gungunObjData3[1], label='gungun')
    # axs[2].scatter(currentObjData3[0], currentObjData3[1], label='daniel')
    # for ax in axs.flat:
    #   ax.set(xlabel='gungun', ylabel='daniel')
    #   ax.legend()

    # pyplot.show()


    # pyplot.scatter(gungunObjData1[0], gungunObjData1[1])
    # pyplot.scatter(currentObjData1[0], currentObjData1[1])
    # pyplot.xlabel('ssnamenr')
    # pyplot.ylabel('period')
    # pyplot.show()
    
    total_time = (time.time() - start_time)


    print("Closing connection...") # tells us
    await client.close() # it actually closes connection to mongo (best practice)
    print("\nDonesies! It only took %ss\n" % total_time) # prints how long it took
  except Exception as err: # in case error is made
    raise Exception("You done fucked up: ", err)

# actually runs the asynchronous function
asyncio.run(main())