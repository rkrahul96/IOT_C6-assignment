# -*- coding: utf-8 -*-
import sys
import json
import time
import pandas as pd
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode
import customer_analysis as ca
import pyspark.sql

from pykafka import KafkaClient

#varibles
INFILE="./mall_customers.csv"
cust_df=ca.create_cust_df(INFILE)
clf=ca.generate_spend(cust_df)
cstdf,k_means = ca.create_cluster(cust_df)

cust_info=pd.DataFrame([[  0,  34,  78], [  0,  65,  63], [  1,  35,  19]])
spend=clf.predict(cust_info.values)
print("spend info : ", spend)

clsinput=pd.DataFrame([[0,64,19,3],[1,20,16,6]])
pred_cluster=k_means.predict(clsinput)
print("predicted cluster : ", pred_cluster)

spark_context = SparkContext(appName="kafka-spark-Streaming")
spark_context.setLogLevel("WARN")

spark_streaming_context = StreamingContext(spark_context, 30)
"""
{'Gender': 'Female', 'Age': 21, 'Annual Income (k$)': 33, 'customerid': 36, 'Distance': 51, 'timestamp': '2020-08-02 Time:13:47:43', 'sensorid': 'Sid_36'}
{"Gender": "Female", "Age": 21, "Annual Income (k$)": 33, "customerid": 36, "Distance": 51, "timestamp": "2020-08-02 Time:13:47:43", "sensorid": "Sid_36"}
{'Gender': 'Female', 'Age': 42, 'Annual Income (k$)': 34, 'customerid': 37, 'Distance': 67, 'timestamp': '2020-08-02 Time:13:47:45', 'sensorid': 'Sid_37'}
"""
kafka_stream = KafkaUtils.createStream(spark_streaming_context, 'localhost:2181', 'spark-streaming', {'customer_data':1})

parsed_data = kafka_stream.map(lambda v: json.loads(v[1]))
parsed_data.count().map(lambda x:'Records in this batch B1: %s' % x).pprint()
#print(parsed_data.pprint())
#print(type(parsed_data))

pot_cust_stream=parsed_data.filter(lambda line: int(line['Distance']) <=100)
print(pot_cust_stream.pprint())
"""
keys_dstream = parsed_data.map(lambda line: line['key'])
keys_counts = keys_dstream.countByValue()
keys_counts.pprint()
"""
"""
busline_dstream = parsed_data.map(lambda line: line['busline'])
bus_counts = busline_dstream.countByValue()
bus_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))
bus_counts.pprint()

print(type(parsed_data), type(bus_counts))
"""
#status_dstream = parsed_data.map(lambda line: line['status'])
#statuswise_bus_counts = status_dstream.countByValue()
#broken_buses_streams = statuswise_bus_counts.filter(lambda x: x[0].upper().startswith('BROKEN'))
#broken_buses_streams.pprint()

def send_to_kafka_matched(partition):
	client = KafkaClient(hosts="localhost:9092")
	topic = client.topics['t2']
	producer = topic.get_sync_producer()
	for record in partition:
		message = json.dumps(str(record))
		producer.produce(message.encode('ascii'))

def get_customer_data(partition):
	#counter=0
	for record in partition: #this is only 1 record.
		### record is a dict, partition is a json structure.
	
		print(record['Gender'],record['Age'],record['Annual Income (k$)'])

	#	counter+=1 
	#print(counter)

pot_cust_data=pot_cust_stream.foreachRDD(lambda rdd: rdd.foreachPartition(get_customer_data)) #this gets called for each record individually.
print(type(pot_cust_data),pot_cust_data)
#parsed_data.foreachRDD()
#statuswise_bus_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))

spark_streaming_context.start()
spark_streaming_context.awaitTermination()

#spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar k1.py >> spark_demo.logs