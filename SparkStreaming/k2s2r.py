from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis

## function: save by-cik visit total to redis
def cikRedis(iterator):
    r = redis.StrictRedis(host='ec2-*******.compute-1.amazonaws.com', port=6379) # Key 1: visible to node
    for x in iterator:
        r.zincrby('top10', x[0], x[1])

## function: save by-second visit total to redis
def timeRedis(iterator):
        r = redis.StrictRedis(host='******', port=6379) # Key 1: visible to node
        for x in iterator:
            r.zincrby('time', x[0], x[1])

## function: print the total of records  -- track performance
def counts(iterator):
    print(iterator.count())




if __name__ == "__main__":
    
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    brokers, topic = [ '******:9092', 'streaming']
    sc.setLogLevel('ERROR')
    
    ## connect to kakfa
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

    ## get the records: [cik, time]
    raw = kvs.map(lambda x: x[1]) \
             .map(lambda line: line.split(",")) \
             .map(lambda line: [line[2],line[4]]).cache()

    ## count by cik
    times = raw.map(lambda timeCik: (timeCik[0], 1)) \
               .reduceByKey(lambda a, b: a+b)

    ## count by cik
    ciks = raw.map(lambda timeCik: (timeCik[1], 1)) \
              .reduceByKey(lambda a, b: a+b)
    
    ## write to Redis
    ciks.foreachRDD(lambda x: cikRedis(x.collect()))
    times.foreachRDD(lambda x: timeRedis(x.collect()))

    ## print/ track the performance
    raw.foreachRDD(counts)
    ciks.pprint()

ssc.start()
ssc.awaitTermination()
