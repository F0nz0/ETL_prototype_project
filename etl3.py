'''
Microservice Name: 
    ETL3 = ETL 3rd Level
Description:
    A microservice that read the data from the Kafka Real-Time Topic in a JSON
    format. Afterwards, it sends JSON data to Redis dataset.
'''

import os
import pandas as pd
from kafka import KafkaConsumer
import redis
from json import loads
import itertools
import datetime

topic = "real-time"

# Consumer to read all messages from topic real-time.
etl3_consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='etl-3',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
    )

# Connection to redis server.
r = redis.Redis(host='localhost', port=6379, db=0)

# Start iteration over real-time topic messages to be on Redis.
start = datetime.datetime.now()
counter = 1
for message_ in etl3_consumer:
    message = message_.value
    key = message["VinVehicle"] + "-" + str(message["Timestamp"])
    value = dict(itertools.islice(message.items(), 2,5))
    print(key, value)
    r.set(key, str(value))
    print("Message n° {} inserted correctly into Redis.".format(counter))
    counter += 1
etl3_consumer.close()

# Final report.
stop = datetime.datetime.now()
time_required = stop - start
print("A total of n° {} messages have been processed in {}".format(counter-1, time_required))
print("#####################################################################################")
print("##### etl3 microservice published all the data retrieved from real-time topic #######")
print("############################# into the Redis database. ##############################")
print("#####################################################################################")
