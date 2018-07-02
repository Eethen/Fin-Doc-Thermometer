from flask import render_template
from flaskexample import app
from datetime import datetime as dt
from datetime import timedelta as td
from pytz import timezone
import redis
import psycopg2
import matplotlib.pyplot as plt



user = 'Katie' #add your username here (same as previous postgreSQL)
r = redis.StrictRedis(host='ec2-35-171-246-105.compute-1.amazonaws.com', port=6379)

@app.route('/')
@app.route('/index')
def index():
    user = { 'nickname': 'Miguel' } # fake user
    return render_template("index.html", title = 'Home', user = user)

@app.route('/top10')
def top10():
    cikCount_list_All = r.zrevrange('All', 0, 9, withscores=True) # [('cik', '#')]
    cikCount_list_Now = r.zrevrange('Now', 0, 9, withscores=True)

# reset
# r.zremrangebyrank('Now', 0, -1)

    ranksAll={}
    ranksNow={}
    ##########
    ## check if there are not 10 companies
    
#    r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 = list( map(lambda y: " ~ ".join([(y[0].decode('utf-8')).replace(".0", "", 1), str(int(y[1]))]),
#                                                        cikCount_list_All) )

    r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 = list( map(lambda y: [(y[0].decode('utf-8')).replace(".0", "", 1), str(int(y[1]))],
                                                        cikCount_list_All))

    ranksAll['r1']=r1
    ranksAll['r2']=r2
    ranksAll['r3']=r3
    ranksAll['r4']=r4
    ranksAll['r5']=r5
    ranksAll['r6']=r6
    ranksAll['r7']=r7
    ranksAll['r8']=r8
    ranksAll['r9']=r9
    ranksAll['r10']=r10

    #################
#    r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 = list( map(lambda y: " ~ ".join([(y[0].decode('utf-8')).replace(".0", "", 1), str(int(y[1]))]),
#                                                    cikCount_list_Now) )

    r1, r2, r3, r4, r5, r6, r7, r8, r9, r10 = list( map(lambda y: [(y[0].decode('utf-8')).replace(".0", "", 1), str(int(y[1]))],
                                                        cikCount_list_Now) )
    
    ranksNow['r1']=r1
    ranksNow['r2']=r2
    ranksNow['r3']=r3
    ranksNow['r4']=r4
    ranksNow['r5']=r5
    ranksNow['r6']=r6
    ranksNow['r7']=r7
    ranksNow['r8']=r8
    ranksNow['r9']=r9
    ranksNow['r10']=r10
    
    return render_template("top10.html", title = 'top10', ranksNow = ranksNow, ranksAll=ranksAll)



time_time=[]
time_count=[]
url2 = "dbname=rawdata user=YiLiu_RDS password=Cd2017Job host=post20g.cvvi4duxl8pw.us-east-1.rds.amazonaws.com"
conn = psycopg2.connect(host='post20g.cvvi4duxl8pw.us-east-1.rds.amazonaws.com',\
                        database="rawdata", \
                        user="YiLiu_RDS", \
                        password="Cd2017Job")
cur = conn.cursor()
cur.execute("""
    SELECT * FROM Quantiles;
    """,
        )
quant = cur.fetchall()

cur.close()
conn.close()

quant_list = sorted(quant, key = lambda x: x[0])
quan25 = list(map(lambda x: x[1], quant_list))
quan75 = list(map(lambda x: x[2]*10, quant_list))
quan95 = list(map(lambda x: x[3]*10, quant_list))

@app.route('/seconds')
def seconds():
#    now = dt.now(timezone('UTC'))
#    then = now - td(seconds = 10)
#    now =dt.strftime(now, "%H:%M:%S")
#    then =dt.strftime(then, "%H:%M:%S")

    timeCount_list = r.zrange('time',0, -1, withscores = True)
    
    last = len(timeCount_list) + 1 # number of records; 1 step forward
    
    timeCount_list = sorted(timeCount_list, key=lambda x: x[0])
    timeCount_list = list(map(lambda x: x[1], timeCount_list))
    
    data_redis = ['Real-time'] + timeCount_list
    data_25 = ['25th percentile'] + quan25[0:last]
    data_75 = ['75th percentile'] + quan75[0:last]
    data_95 = ['95th percentile'] + quan95[0:last]

    return render_template("seconds.html", title = 'seconds', data_redis = data_redis, data_25 = data_25, data_75 = data_75, data_95 = data_95)
#    return str(quant_list)




