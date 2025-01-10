'''
Microservice Name: 
    ETL2 = ETL 2nd Level
Description:
    A microservice that read the data from the Kafka Batch Topic in a JSON
    format. Afterwards, it sends JSON data to two MONGODB collections:
        A) SIMPLE COLLECTION: 
            The data fetched from the Batch topic in a JSON format will be 
            saved in this collection without any change. 
            It is important to eliminate a message if this is a duplicate 
            in the MongoDB collection.

        B) ELABORATED COLLECTION: 
            The data fecthed from the Batch topic in JSON format will be 
            elaborated furthermore. Two field will be substituted by fields:
                1) DeltaOdometer: 
                        calculated from Odometer as difference between the last 
                        two values of Odometer for the same VIN/Vehicle.

                2) DeltaLifeConsumption:
                        calculated from LifeConsumption as difference between 
                        the last two values of LifeConsumption for the same 
                        VIN/Vehicle.
'''

import os
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient, DESCENDING
from json import loads
import itertools
import datetime

topic = "batch"

# Consumer to read all messages from topic batch.
etl2_consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='etl-2',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
    )

# Connection to MongoDB server and creation of collections.
client = MongoClient('localhost:27017')
db = client["AI4FleetManagement"]
simple_coll = db["Simple"]
elaborated_coll = db["Elaborated"]

# Start iteration over batch messages to be elaborated and pubblished on the two collections
# on MongoDB server.
start = datetime.datetime.now()
counter = 1
for message_ in etl2_consumer:
    message = message_.value
    VIN_key = dict(itertools.islice(message.items(), 1))
    last_vin_item = simple_coll.find_one(VIN_key, sort=[("Timestamp", DESCENDING)])
    keys = dict(itertools.islice(message.items(), 2))
    simple_coll.update(keys, message, upsert=True)
    print("Message n° {} inserted correctly into simple collection.".format(counter))
    
    if last_vin_item != None:
        DeltaOdometer = message["Odometer"] - last_vin_item["Odometer"]
        DeltaLifeConsumption = message["LifeConsumption"] - last_vin_item["LifeConsumption"]
    else:
        DeltaOdometer = 0
        DeltaLifeConsumption = 0
    del message["Odometer"]
    del message["LifeConsumption"]
    message["DeltaOdometer"] = DeltaOdometer
    message["DeltaLifeConsumption"] = DeltaLifeConsumption
    elaborated_coll.update(keys, message, upsert=True)
    print("Message n° {} inserted correctly into elaborated collection.".format(counter))
    print("#################################################################################")
    print()
    counter += 1
etl2_consumer.close()

# Final report.
stop = datetime.datetime.now()
time_required = stop - start
print("A total of n° {} messages have been processed in {}".format(counter-1, time_required))
print("#####################################################################################")
print("######## etl2 microservice published all the data retrieved from batch topic ########")
print("######## into the two simple and elaborated collections on MongoDB database. ########")
print("#####################################################################################")