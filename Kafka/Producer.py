from   datetime        import datetime as dt
from   confluent_kafka import Producer
from   pytz            import timezone
import socket
import boto3


##################################################################
class Producer:
    """
        class that reads data from S3 bucket, updates it with
        real-time time stamp and produces it to Kafka cluster
    """

    def __init__(self, broker, topic, bucket, key):
        """
            class constructor that initializes an instance with
            the configurations of S3 bucket and Kakfa cluster
            
            broker:    str    broker_ip:port
            topic :    str    Kafka topic name
            bucket:    str    S3 bucket name
            key   :    str    path to S3 data
        """
        self.broker    = broker
        self.topic     = topic
        
        self.bucket    = bucket
        self.key       = key

        self.s3_client = boto3.client('s3')
        self.s3_object = self.s3_client.get_object(Bucket = bucket, Key = key)
    
        self.producer  = Producer({'bootstrap.servers': self.broker})
    
    
    def produce(self):
        """
            updates each line with real-time time stamp and produces by line
            to the specified topic of the Kafka cluster broker
        """
        while True:
            for line in obj['Body'].read().split():
            
                line    = line.decode("utf-8").split(",")
    
                line[2] = dt.strftime(dt.now(timezone('UTC')), "%H:%M:%S")
                key     = line[0] # Key: ip
                line    = ','.join(line) # cast to string
                line    = line.encode('utf-8') # cast to bytes
            
                self.producer.produce(topic = self.topic,
                                      value = line,
                                      key   = key)
        
                self.producer.flush()


##################################################################
if __name__ == "__main__":
    
    broker   = '**********:9092'
    topic    = 'streaming'
    
    bucket   = '******'
    key      = 'streaming/sp1.csv'
    
    producer = Producer(broker = broker,
                        topic  = topic
                        bucket = bucket,
                        key    = key)
    producer.produce()
