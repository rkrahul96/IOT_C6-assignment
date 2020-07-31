from kafka import KafkaProducer
import csv
producer = KafkaProducer(bootstrap_servers='localhost:9092')
with open('/home/rahul/IOT/kafka/PROJECT/customer_track.csv','r') as file:
    reader = csv.DictReader(file)
    with open('/home/rahul/IOT/kafka/PROJECT/customer_data.csv','r') as file1:
        reader1 = csv.DictReader(file1)
        for value1 in reader1:
            for value in reader:
                if (value1['CustomerID']==value['CustomerID']):
                    value.update(value1)
                    producer.send('customer_data',bytes(value))
                else:
                    break












