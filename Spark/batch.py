import numpy as np
import psycopg2
from psycopg2 import extras
from pyspark import SparkConf, SparkContext

## function: to batch insert
def insert(row_list):
    ## build connections for each partition
    conn = psycopg2.connect(host='*******',\
                            database="rawdata", \
                            user="*****", \
                            password="******")
                            
    cur = conn.cursor()

    ## prepartion statement & batch insert
    cur.execute("PREPARE stmt AS INSERT INTO QUANTILES ( time, sevfiv, ninfiv) VALUES ($1, $2, $3);")
    extras.execute_batch(cur, "EXECUTE stmt (%s, %s, %s)", row_list)
    cur.execute("DEALLOCATE stmt")

    conn.commit()
    cur.close()
    conn.close()


print("************************* starting *************************")
sc = SparkContext().getOrCreate()

data = sc.textFile("s3a://*****/*.csv")\
    .map(lambda x: (str(x)).split(','))\
    .map(lambda x: [x[1],x[2]])\
    .map(lambda x: (','.join(x), 1))\
    .reduceByKey(lambda a,b: a+b)\
    .map(lambda x: (( (x[0]).split(',') )[1] , x[1]) )\
    .groupByKey().mapValues(list)\
    .map(lambda x: [x[0], np.percentile(x[1], 75), np.percentile(x[1], 95)])\
    .foreachPartition(insert)

print("************************* finished *************************")
