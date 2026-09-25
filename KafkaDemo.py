# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Kafka Demo — Lab 3
#
# ### Connect to Kafka Broker Server
# Open an SSH tunnel in your terminal and leave it running while you use this notebook.
# Replace `<NetID>` with your UIC NetID:
# ```
# ssh -o ServerAliveInterval=60 -L 9092:localhost:9092 <NetID>@cs544-f26.cs.uic.edu -NTf
# ```
#
# ### To kill connection
# ```
# lsof -ti:9092 | xargs kill -9
# ```
#
# ### Setup
# ```
# python -m pip install kafka-python
# ```
#
# See [bug_list.md](./bug_list.md) for frequent bugs and solutions.

# %%
import os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
from kafka import KafkaConsumer, KafkaProducer

# Update this for your own recitation section :)
topic = 'recitation-vipul' # replace x with your recitation section

# %% [markdown]
# ### Producer Mode -> Writes Data to Broker

# %%
# Create a producer to write data to kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

# [TODO]: Replace '...' with the address of your Kafka bootstrap server
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

# [TODO]: Add cities of your choice
cities = ['Chicago', 'Ahmedabad', 'Tokyo']

# Write data via the producer
print("Writing to Kafka Broker")
for i in range(10):
    data = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")},{cities[randint(0,len(cities)-1)]},{randint(18, 32)}ºC'
    print(f"Writing: {data}")
    producer.send(topic=topic, value=data)
    sleep(1)

# %% [markdown]
# ### Consumer Mode -> Reads Data from Broker

# %%
# Create a consumer to read data from kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html

# [TODO]: Complete the missing ... parameters/arguments using the Kafka documentation
consumer = KafkaConsumer(
    'my-city-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', #Experiment with different values
    # Commit that an offset has been read
    enable_auto_commit=True,
    # How often to tell Kafka, an offset has been read
    auto_commit_interval_ms=1000
)

print('Reading Kafka Broker')
for message in consumer:
    message = message.value.decode()
    # Default message.value type is bytes!
    print(loads(message))
    os.system(f"echo {message} >> kafka_log.csv")

# %% [markdown]
# # Use kcat!
# It's a CLI (Command Line Interface). Previously known as kafkacat
#
#
# Ref: https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html

# %%
#kcat command: connect to local Kafka broker, specify a topic, and consume messages from the earliest offset
