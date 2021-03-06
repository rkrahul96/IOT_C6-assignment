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
INFILE="./customer_data.csv"
cust_df=ca.create_cust_df(INFILE)
clf=ca.generate_spend(cust_df)
cstdf,k_means = ca.create_cluster(cust_df)

spark_context = SparkContext(appName="kafka-spark-Streaming")
spark_context.setLogLevel("WARN")

spark_streaming_context = StreamingContext(spark_context, 30)
kafka_stream = KafkaUtils.createStream(spark_streaming_context, 'localhost:2181', 'spark-streaming', {'customer_data':1})

parsed_data = kafka_stream.map(lambda v: json.loads(v[1]))
parsed_data.count().map(lambda x:'Records in this batch B1: %s' % x).pprint()

pot_cust_stream=parsed_data.filter(lambda line: int(line['Distance']) <=100)
print(pot_cust_stream.pprint())

def get_offer(customer_segment):
	if customer_segment == 0:
		return ("offer1")
	elif customer_segment == 1:
		return ("offer2")
	elif customer_segment == 2:
		return ("offer3")
	elif customer_segment == 3:
		return ("offer4")
	elif customer_segment == 4:
		return ("offer5")
	else:
		return ("offer6")

def get_customer_data(partition):
	client = KafkaClient(hosts="localhost:9092")
	#create a topic for customer offer.
	topic = client.topics['customer_offer']
	producer = topic.get_sync_producer()

	for record in partition: #this is only 1 record.
		### record is a dict, partition is a json structure.
		# use clf.predict(ndarray) to predict the spending
		# use k_means.predict(dataframe) to predict the cluster
		if(record['Gender'] == 'Male'):
			record['Gender']=0
		else:
			record['Gender']=1
		incustdata=pd.DataFrame([record])
		incustdata['Spending Score (1-100)']=0
		incustdata_features=incustdata[['Gender', 'Age', 'Annual Income (k$)']] #Features
		spend = clf.predict(incustdata_features.values)
		print("predicted spend : ", spend)

		# Next step to calculate the customer segment
		incustdata["Spending Score (1-100)"]=spend
		incustdata_elements=incustdata[['Gender', 'Age', 'Annual Income (k$)' ,'Spending Score (1-100)']]
		#print("customer data with predicted spend : ", incustdata_elements)
		pred_cus_segment=k_means.predict(incustdata_elements)
		print("predicted customer segment : ", pred_cus_segment)
		offer=get_offer(pred_cus_segment)
		print("offer to release : ", offer)
		record['Spending Score (1-100)']=spend[0]
		record['segment']=pred_cus_segment[0]
		record['offer']=offer
		print(type(spend))
		print(record)
		# Writing the record offer data to a topic
		message = json.dumps(str(record))
		producer.produce(message.encode('ascii'))  


pot_cust_stream.foreachRDD(lambda rdd: rdd.foreachPartition(get_customer_data)) #this gets called for each record individually.

spark_streaming_context.start()
spark_streaming_context.awaitTermination()
