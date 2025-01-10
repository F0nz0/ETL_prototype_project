'''
Microservice Name: 
    INJECTOR
Description:
    A microservice that read data from the source (csv dataset) and send, by a 
    Kafka Producer, to n topics, where n is the number of different VIN numbers/
    vehicles. The messages will contain the raw data of each row.
'''

# Import needed modules.
import os
import pandas as pd
from kafka import KafkaProducer
import datetime

# Load whole dataset and elaborate it.
df = pd.read_csv("dati_centraline.csv")
df = df.dropna(axis=0)
df.drop_duplicates(subset=["VinVehicle", "Timestamp", "Driver"], inplace=True)
df.sort_values(by=["VinVehicle", "Timestamp"], inplace=True)
print("Dataset dimension after dropping rows with NaN values and duplicates:", df.shape)

# Eliminate dots in columns' name to avoid problem during string conversion.
df.columns = [i.replace(".", "_") for i in df.columns]
print(df.columns)

# Extract VINs from vehicles
VINs = df.VinVehicle.unique()
print(VINs)

# Instantiate a producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Produce messages for each row in the df, but putting then raw in N different
# topics (one for VIN number/ vehicle)
start = datetime.datetime.now()
counter = 1
print("Sending raw data to Kafka Broker.........")
for row in df.itertuples(index=False):
    topic = row.VinVehicle
    value = str(row).encode('utf-8')
    print(value)
    producer.send(topic, value=value)
    print("Message n° {} has been processed.\n".format(counter))
    counter += 1

# Final report.
stop = datetime.datetime.now()
time_required = stop - start
print("A total of n° {} message have been processed in {}".format(counter-1, str(time_required)))
print("############################################################")
print("# All data have been published on the corresponding topic. #")
print("############################################################")
