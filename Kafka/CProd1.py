from confluent_kafka import Producer
import socket
import boto3
from datetime import datetime as dt
from pytz import timezone

p = Producer({'bootstrap.servers': '**********:9092'})

client = boto3.client('s3')

##
while True:
    ## connect to S3
    obj = client.get_object(Bucket='******', Key='streaming/sp1.csv')
    
    ## stream out record one by one
    for line in obj['Body'].read().split():
        line = line.decode("utf-8").split(",")
        
        line[2] = dt.strftime(dt.now(timezone('UTC')), "%H:%M:%S") # attach real-time time stamp
        key = line[0] # Key: ip
        line = ','.join(line) # to string
        
        p.produce(topic = 'streaming', value = line.encode('utf-8'), key = key)
        p.flush()
    
    print("************************ 180000 records done.************************")


