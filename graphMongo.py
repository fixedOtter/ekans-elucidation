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

    # query of objects with string value higher than "900"
    query900 = {
      "$and": [
        {"ssnamenr": {"$gt": "900"}},
        {"periods.numberObservations": {"$gt": 5}}
      ]
      }

    gungunObjects = gunguntestCol.find(query900) # runs query on gungun collection
    currentObjects = realCol.find(query900) # runs query on "real" collection
    print("I found %d objects in gunguntest!" % gungunCount) # prints how many was found
    print("I found %d objects in current!" % currentCount) # prints how many was found
    

    someObject = await gungunObjects.next()
    print(someObject["periods"]["periods"][1]["period"])

    # getting data 
    gungunObjData = [[],[]]
    currentObjData = [[],[]]
    async for object in gungunObjects: # loops through all objects in what was found
      if (object["periods"] != 0):
        per = object["periods"]["periods"][1]["period"]
        ssnamenr = object["ssnamenr"]
        gungunObjData[0].append(ssnamenr)
        gungunObjData[1].append(per)
    
    async for object in currentObjects:
      if (object["periods"] != 0):
        per = object["periods"]["periods"][1]["period"]
        currentObjData[0].append(ssnamenr)
        currentObjData[1].append(per)
    
    # sort arrays (should sort both of them based on ssnamenr?)
    gungunObjData[0], gungunObjData[1] = zip(*sorted(zip(gungunObjData[0], gungunObjData[1])))
    currentObjData[0], currentObjData[1] = zip(*sorted(zip(currentObjData[0], currentObjData[1])))


    # makes some stuff for plotting
    # x1 = np.arange(0,len(gungunObjData),1)
    # x2 = np.arange(0,len(currentObjData),1)
    # y1 = np.array(gungunObjData)
    # y2 = np.array(currentObjData)

    # print(x1)

    pyplot.plot(gungunObjData[0], gungunObjData[1])
    pyplot.plot(currentObjData[0], currentObjData[1])
    pyplot.show()


    print("Closing connection...") # tells us
    await client.close() # it actually closes connection to mongo (best practice)
    print("\nDonesies! It only took %ss\n" % (time.time() - start_time)) # prints how long it took
  except Exception as err: # in case error is made
    raise Exception("You done fucked up: ", err)

# actually runs the asynchronous function
asyncio.run(main())