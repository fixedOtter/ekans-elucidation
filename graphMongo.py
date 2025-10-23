#
# made by gunnar 10.20.2025
#
import os # pulled in for environmen variables
import time # pulled in for calc time taken
import asyncio # pulled in to run asynchronously
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

    # query of all objects with more than 2 periods in period array
    # gt2perQuery = { 
    #   {"periods": {"periods": {"$gt": 2}}}
    # }
    
    # query of all objects with ssnamenr: >900 & <101731
    ssnamenrQuery = { 
      "$and": [
        {"ssnamenr": {"$gt": 900}},
        {"ssnamenr": {"$lte": 101731}}
      ]
    }

    gungunObjects = gungunCount.find(ssnamenrQuery) # runs query on gungun collection
    currentObjects = currentCount.find(ssnamenrQuery) # runs query on "real" collection
    print("I found %d objects" %gungunObjects) # prints how many was found
    for object in gungunObjects: # loops through all objects in what was found
      # pyplot.plot(object.periods.periods.1.period,currentObjects)
      print(object)


    print("Closing connection...") # tells us
    await client.close() # it actually closes connection to mongo (best practice)
    print("\nDonesies! It only took %s \n" % (time.time() - start_time)) # prints how long it took
  except Exception as err: # in case error is made
    raise Exception("You done fucked up: ", err)

# actually runs the asynchronous function
asyncio.run(main())