'''
Microservice Name: 
    ETL1 = ETL 1st Level
Description:
    A microservice that read the raw data from the n Kafka VIN topics, 
    where n is the number of different VIN numbers/vehicles and make some
    transformations and afterwards, sends JSON data to two branch of the 
    dataflow on two different topics:
        A) Topic Batch's data transformations:
            1)  Create a new string field, called "MessageDate", containing
                the date of the message expressed as ISO-8601 format.

            2)  If number of satellites in the field Position.satellites is < 3
                it's requested to invalidate the record by setting it to -1.

            3)  If the value of the field for Position.altitude is < 0, it's 
                requested to set it to 0.

            3)  Data type's for publishing:
                    VinVehicle = string
                    Timestamp = long
                    Driver = string
                    Odometer = long
                    LifeConsumption = long
                    Position.lon = double
                    Position.lat = double
                    Position.altitude = float
                    Position.heading = float
                    Position.speed = int
                    Position.satellites = int
            
        B) Topic Real-Time's data transformations:
            1)  The message published on this topic have to contain only
                the position of the vehicle (Position.lat and .long) and, 
                also in this case, if the number of satellite is < 3, the 
                position must be invalited to -1.
'''

# Import needed modules.
import os
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from json import dumps
from datetime import datetime

# Definition of principal function for data elaboration.
def from_raw_row_to_dict(raw_row_string):
    '''
    Function to transform the raw bite string of kafka to dictionary with correct datatype and
    to elaborate data related to Satellites number, Latitude, Longitude and Altitude.
    '''
    row_str_decod = raw_row_string
    row_str_decod = row_str_decod.replace("Pandas(", "")
    row_str_decod = row_str_decod.replace(")", "")
    row_str_decod = row_str_decod.replace("=", ":")
    row_str_decod = row_str_decod.split(",")
    row_dict = {}
    for i in row_str_decod:
        row_dict[i.split(":")[0].replace(" ", "")] = i.split(":")[1].replace("'", "").replace(" ", "")
    
    # Data-Type / Format Adjustments
    row_dict["Timestamp"] = int(row_dict["Timestamp"])
    row_dict["Odometer"] = int(row_dict["Odometer"])
    row_dict["LifeConsumption"] = int(row_dict["LifeConsumption"])
    row_dict["Position_lon"] = float(row_dict["Position_lon"])
    row_dict["Position_lat"] = float(row_dict["Position_lat"])
    row_dict["Position_altitude"] = float(row_dict["Position_altitude"])
    row_dict["Position_heading"] = float(row_dict["Position_heading"])
    row_dict["Position_speed"] = int(float(row_dict["Position_speed"]))
    row_dict["Position_satellites"] = int(float(row_dict["Position_satellites"]))
    
    # Data Elaborations
    row_dict["MessageDate"] = datetime.fromtimestamp(row_dict["Timestamp"]).isoformat()
    if row_dict["Position_satellites"] < 3:
        print("Satellite are ", row_dict["Position_satellites"], "\nLat and Long will be set to -1!")
        row_dict["Position_lon"] = -1
        row_dict["Position_lat"] = -1
    if row_dict["Position_altitude"] < 0:
        print("Position Altitude is", row_dict["Position_altitude"], "\nPosition Altitude will be set to 0!")
        row_dict["Position_altitude"] = 0
    return row_dict

# Consumer to gets the topics names.
first_consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='etl1_alpha',
     value_deserializer=lambda x: x.decode('utf-8'),
     consumer_timeout_ms=1000)

# Get VIN topics names.
VINs = list(first_consumer.topics())
if "batch" in VINs:
    VINs.remove("batch")
if "real-time" in VINs:
    VINs.remove("real-time")
print(VINs)

# Dictionary with n consumers (one for VINs topic.) 
vins_consumers = {}
for VIN in VINs:
    vins_consumers[VIN] = KafkaConsumer(
        VIN,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='etl1_{}'.format(VIN),
        value_deserializer=lambda x: x.decode('utf-8'),
        consumer_timeout_ms=2500)


# Load for each VIN, all the message and elaborate it. 
# After elaboration, data are published on the batch and real-time topics.
start = datetime.now()
counter = 1
for VIN in VINs:
    # Instantiate the Kafka Producer for the two topic branches
    etl1_producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                                    value_serializer = lambda x: dumps(x).encode('utf-8'))
                                    
    print("Publishing data of VIN:", VIN, ".....")
    for message in vins_consumers[VIN]:
        row_dict = from_raw_row_to_dict(message.value)

        # Publish on Batch branch.
        etl1_producer.send("batch", value=row_dict)
        print("Data of Message n° {} have been published on Batch topic.".format(counter))

        # Publish on Real-Time branch.
        realtime_dict = {}
        realtime_dict["VinVehicle"] = row_dict["VinVehicle"]
        realtime_dict["Timestamp"] = row_dict["Timestamp"]
        realtime_dict["Position_lon"] = row_dict["Position_lon"]
        realtime_dict["Position_lat"] = row_dict["Position_lat"]
        etl1_producer.send("real-time", value=realtime_dict)
        print("Data of Message n° {} have been published on real time topic.".format(counter))
        counter += 1
        print("####################################################################\n")
    vins_consumers[VIN].close()
    print("Publishing of data for VIN {} by etl1 microservice has been done.\n\n".format(VIN))

# Final report.
stop = datetime.now()
time_required = stop - start
print("A total of n° {} messages have been processed in {}".format(counter-1, time_required))
print("########################################################################")
print("# etl1 microservice published all the data retrieved from VINs topics! #")
print("########################################################################")