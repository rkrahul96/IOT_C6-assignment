#code for distance sensor!!!
import time
import json
import random
import datetime
import csv
count =1
sensor_id = 'sd_id_'
time_first = datetime.datetime.now()
csv_columns = ['CustomerID','sensor_id','timestamp','distance']
csv_file = "/home/rahul/IOT/kafka/PROJECT/customer_track.csv"
with open(csv_file, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    while count <=200:
        data = {}
        data['sensor_id']=sensor_id+str(count)
        data['CustomerID']=count
        time_now=time_first
        for i in range(60):
            data['timestamp'] = time_now.strftime("%Y-%m-%d Time:%H:%M:%S")
            initial = random.randint(0, 150)
            subset = random.randint(0, 10)
            dist = initial + subset
            data['distance'] = dist
            #dataJson = json.dumps(data)
            writer.writerow(data)
        #print(dataJson)
            time_now = time_now + datetime.timedelta(seconds=10)
        #time.sleep(10)
        count = count+1





