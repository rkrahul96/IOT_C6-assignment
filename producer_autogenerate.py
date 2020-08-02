from pykafka import KafkaClient
import csv
import random
import datetime
import time
import json

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['customer_data']
producer = topic.get_sync_producer()
"""
{'Spending Score (1-100)': '83', 'Gender': 'Male', 'Age': '30', 'Annual Income (k$)': '137', 'CustomerID': '200'}
"""
while True:
    i=1
    with open('./customer_data.csv','r') as file1:
        reader = csv.DictReader(file1)
        dict={}
        for value in reader:
            cd_gender = (value["Gender"])
            cd_age = int(value["Age"])
            cd_customerid = int(value["CustomerID"])
            cd_income = int(value["Annual Income (k$)"])
            dict["Gender"]=cd_gender
            dict["Age"] = cd_age
            dict["Annual Income (k$)"] =cd_income
            dict["customerid"] = cd_customerid
            dict["Distance"] = random.randint(0,150)
            dict["timestamp"]  = datetime.datetime.now().strftime("%Y-%m-%d Time:%H:%M:%S")
            dict["sensorid"] = "Sid_"+str(i)
            #print (dict)
            message = json.dumps(dict)
            print(message)
            producer.produce(message.encode('ascii'))
            i+=1
            time.sleep(2)













