# -*- coding: utf-8 -*-
import sys
import json
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pykafka import KafkaClient
'''
{'Distance': 77, 'timestamp': '2020-08-02 Time:06:30:58', 'age': 30, 'sex': 'Male', 'sensorid': 'Sid_200', 'customerid': 200}
'''
spark_context = SparkContext(appName="kafka-spark-Streaming")
spark_context.setLogLevel("WARN")

spark_streaming_context = StreamingContext(spark_context, 20)

kafka_stream = KafkaUtils.createStream(spark_streaming_context, 'localhost:2181', 'spark-streaming', {'customer_data':1})
parsed_data = kafka_stream.map(lambda v: json.loads(v[1]))
parsed_data.count().map(lambda x:'Records in this batch : %s' % x).pprint()
#parsed_data.pprint()
distance_dstream = parsed_data.map(lambda line: line['Distance'])
#TransformedDStream to RDD
#rdd=distance_dstream.foreachRDD(lambda rdd: rdd.foreachPartition(result))
rdd=distance_dstream.foreachRDD(lambda rdd: rdd.foreachPartition(result))
#filtering data less than 100 distance 
stream=parsed_data.filter(lambda line: int(line['Distance']) <=100)
stream.pprint()
def result(partiton):
    for values in partiton:
        #print("afterloop")
        print (values)

'''
distance_dstream.foreachRDD(lambda rdd: rdd.foreachPartition(data_conv))
# converting TransformedDstream datatype to str/int
if data_conv(partition):
    for record in partition:
        message = json.dumps(int(record))
        #print(message)
        #print(type(message))
        if message <= 100:
            parsed_data.pprint()
        else:
            print("dumped")
#print(str(type(parsed_data))+'type')
'''
spark_streaming_context.start()
spark_streaming_context.awaitTermination()

#spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar k1.py >> spark_demo.logs
