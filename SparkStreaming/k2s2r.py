from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis

if __name__ == "__main__":
    
    ## fn: write cik data to Redis
    def cikRedis(iterator):
        r = redis.StrictRedis(host='ec2-*******.compute-1.amazonaws.com', port=6379) # Key 1: visible to node
        for x in iterator:
            r.zincrby('top10', x[0], x[1])

    def timeRedis(iterator):
        r = redis.StrictRedis(host='******', port=6379) # Key 1: visible to node
        for x in iterator:
            r.zincrby('time', x[0], x[1])

    ## fn: print the total record
    def counts(iterator):
        print(iterator.count())
    
    ## connect to kakfa
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = [ '******:9092', 'streaming']
    sc.setLogLevel('ERROR')
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

    ## get the records
    raw = kvs.map(lambda x: x[1]) \  # kvs.value()
             .map(lambda line: line.split(",")) \  # parse by fields
             .map(lambda line: [line[2],line[4]]).cache()  # [time, cik]

    ## count by cik
    times = raw.map(lambda ciktime: (ciktime[0], 1)) \
               .reduceByKey(lambda a, b: a+b)

    ## count by cik
    ciks = raw.map(lambda ciktime: (ciktime[1],1)) \
              .reduceByKey(lambda a, b: a+b)
    
    ## write to Redis
    ciks.foreachRDD(lambda x: cikRedis(x.collect()))
    times.foreachRDD(lambda x: timeRedis(x.collect()))

    ## print/ track the performance
    raw.foreachRDD(counts)
    ciks.pprint()

ssc.start()
ssc.awaitTermination()
