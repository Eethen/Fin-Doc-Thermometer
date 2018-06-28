from confluent_kafka import Producer
import socket
import boto3
from datetime import datetime as dt
from pytz import timezone

p = Producer({'bootstrap.servers': '**********:9092'})

client = boto3.client('s3')


while True:
    obj = client.get_object(Bucket='******', Key='streaming/sp1.csv')
    
    for line in obj['Body'].read().split():
        line = line.decode("utf-8").split(",") # to list the string
        
        line[2] = dt.strftime(dt.now(timezone('UTC')), "%H:%M:%S")
        key = line[0] # Key: ip
        line = ','.join(line) # to string
        
        p.produce(topic = 'streaming', value = line.encode('utf-8'), key = key)
        p.flush()
    
    print("************************ 180000 records done.************************")


