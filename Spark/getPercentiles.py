from   pyspark  import SparkConf, SparkContext
from   psycopg2 import extras
import numpy    as     np
import psycopg2





##################################################################
class Percentiles:
    """
        class that reads data from S3 bucket, calculates 25 percentiles,
        75 percentiles and 95 percentiles by second, and saves them to PostgreSQL
    """
    def __init__(self, s3_path):
        """
            class constructor that initializes an instance with path to
            data in S3 and loads the data
            
            s3_path:    str    path to the data in S3
        """
        self.sc   = SparkContext().getOrCreate()
        self.data = sc.textFile(s3_path)
    
    
    def get_percentiles(self):
        """
            generates the percentiles, and saves them to PostgreSQL
        """
        def insert(row_list):
            """
                saves the percentiles to PostgreSQL
            """
            database_connect = psycopg2.connect(host     = "*******",
                                                database = "s3a://*****/*.csv",
                                                user     = "*******",
                                                password = password = "******"
                
                                                cursor   = database_connect.cursor()
                                                
                                                cursor.execute("PREPARE stmt AS INSERT INTO QUANTILES (time, sevfiv, ninfiv) VALUES ($1, $2, $3, $4);")
                                                extras.execute_batch(cur, "EXECUTE stmt (%s, %s, %s, %s)", row_list)
                                                cursor.execute("DEALLOCATE stmt")
                                                
                                                database_connect.commit()
                                                cursor.close()
                                                database_connect.close()
                                                
        self.data.map(lambda x: (str(x)).split(','))\
                 .map(lambda x: [x[1], x[2]])\
                 .map(lambda x: (','.join(x), 1))\
                 .reduceByKey(lambda a, b: a + b)\
                 .map(lambda x: (((x[0]).split(','))[1], x[1]))\
                 .groupByKey()\
                 .mapValues(list)\
                 .map(lambda x: [x[0],
                                 np.percentile(x[1], 25),
                                 np.percentile(x[1], 75),
                                 np.percentile(x[1], 95)])\
                 .foreachPartition(self.insert)
        """
           .map:        ['record, record, ...']        -> ['record', 'record', ...]
           .map:        ['record', 'record', ...]      -> [['date', 'time'], ...]
           .map:        [['date', 'time'], ...]        -> [('date,time', 1), ...]
           .map:        [('date,time', 1), ...]        -> [('date,time', N), ...]
           .map:        [('date,time', N), ...]        -> [('time', N), ...]
           .groupByKey: [('time', N), ...]             -> [('time', (N1, N2,..)), ...]
           .mapyValues: [('time', (N1, N2, ...)), ...] -> [('time', [N1, N2,..]), ...]
           .map:        [('time', [N1, N2, ...]), ...] -> [['time', 25th percentile, 25th percentile, 25th percentile], ...]
        """
                                                
##################################################################
if __name__ == "__main__":
                                                
    s3_path     = "s3a://*****/*.csv"

    percentiles = Percentiles(s3_path)
    percentiles.get_percentiles()
