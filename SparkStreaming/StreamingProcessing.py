from pyspark.streaming       import StreamingContext
from pyspark                 import SparkContext
from pyspark.streaming.kafka import KafkaUtils



##################################################################
class StreamingProcessing:
    """
        class that consumes data from Kafka cluster, calculates total
        visits by time and by company, and saves the totals to Redis
    """
    def __init__(self, broker, topic):
        """
            class constructor that initializes an instance with
            Kafka cluster info
            
            broker:    str    Kafka-broker-ip:port
            topic :    str    Kafka broker topic name
        """
        self.broker        = broker
        self.topic         = topic
        
        self.sc            = SparkContext(appName = "StreamingProcessing")
        self.scc           = StreamingContext(self.sc, 2)

        self.Kafka_connect = KafkaUtils.createDirectStream(ssc,
                                                           [topic],
                                                           {"metadata.broker.list": brokers})

        self.sc.setLogLevel('ERROR')
    
    
    def get_totals(self):
        """
            calculates total visits by time and by company, and
            saves the totals to Redis
        """
        def save_totalBycompany(iterator):
            """
                saves total visits by company to Redis
            """
            redis_connect = redis.StrictRedis(host = '******', port = 6379)
            
            for x in iterator:
                redis_connect.zincrby('all', x[0], x[1])
                redis_connect.zincrby('now', x[0], x[1])
            
            
        def save_totalBytime(iterator):
            """
                saves total visits by time to Redis
            """
                redis_connect = redis.StrictRedis(host = '******', port = 6379) # Key 1: visible to node

                for x in iterator:
                    redis_connect.zincrby('time', x[0], x[1])
            
            
        raw = self.Kafka_connect.map(lambda x: x[1])\
                                .map(lambda line: line.split(","))\
                                .map(lambda line: [line[2], line[4]])\
                                .cache()
        """
           .map: [..., 'record1, record2, ...', ...] -> ['record1, record2, ...']
           .map: ['record1, record2, ...']           -> [['record1'], ['record2'], ...]
           .map: [['record1'], ['record2'], ...]     -> [['time', 'company'], ...]
        """
        
        totalBytimes   = raw.map(lambda x: (x[0], 1))\
                            .reduceByKey(lambda x, y: x + y)
        """
          .map        : [['time', 'company'], ...] -> [('time', 1), ...]
          .reduceByKey: [('time', 1), ...]         -> [(time1, total1), (time2, total2), ...]
        """
        
        totalBycompany = raw.map(lambda x: (x[1], 1))\
                            .reduceByKey(lambda x, y: x + y)
        """
          .map        : [['time', 'company'], ...] -> [('company', 1), ...]
          .reduceByKey: [('company', 1), ...]      -> [('company1', total1), ('company2', total2), ...]
        """
        
        totalBytimes.foreachRDD(lambda x: save_totalBytime(x.collect()))
        totalBycompany.foreachRDD(lambda x: save_totalBycompany(x.collect()))
                                     
        self.ssc.start()
        self.ssc.awaitTermination()
                                     
                                     
##################################################################
if __name__ == "__main__":
    broker              = "******:9092"
    topic               = "streaming"

    streamingProcessing = StreamingProcessing(broker = broker, topic = topic)
    streamingProcessing.get_totals()
                                     
                                     
