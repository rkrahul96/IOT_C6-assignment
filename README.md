# IOT_C6-assignment

#step 1:run producer_autogenerate.py
python3 producer_autogenerate.py

#step 2: run customer_segmenting_spark.py
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar customer_segmenting_spark.py >> spark.logs

#step 3: to view topic offer 
run kafka consumer 
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic offer --from-beginning
