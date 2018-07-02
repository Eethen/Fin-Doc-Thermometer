# Fin-Doc-Thermometer


## Brief Introduction
This project has built a real-time monitoring system to track visits to electronic financial documents available in The Division of Economic and Risk Analysis (DERA). 

The system consists of a streaming pipeline and a batch pipeline. The streaming pipeline deals with real-time data, tracking what companies attract people's most attention and the total of documents that have been visited by second. The batch pipeline generates statistics from historical data to help describe how active the public are in checking documents. 


## Pipeline
The whole system is illustrated as follows:
![Pipeline Image](https://github.com/Eethen/Fin-Doc-Thermometer/blob/master/Images/Screen%20Shot%202018-07-02%20at%202.35.16%20PM.png)


## Folder Map
**DataDownload**  : a script that downloads, unzips and uploads to AWS S3 bucket, the historical data.
**Images**        : a pipeline .png file as shown above.
**Kafka**         : a python script that updates the historical data in S3 with real-time time stamp and publishes them to the Kafka cluster.
**Spark**         : a bash script and a python script that generate, and store to PostgreSQL, the 25th, the 75th and the 95th percentiles by second.
**SparkStreming** : a bash script and a python script that track, and store to Redis, the total visits by company ID and by time.
**Flask**         : Python and HTML scripts that establish the Graphical User Interface.


## Background and Motivation
US Securities and Exchange Commission require companies to file registration statements and periodic reports. All those information are available to the public and are well studied by researchers and investors. This system enables users, such as data analysts and quantitative analysts, to track what the most visited companies are in real-time manner and 


## Streaming Data Processing


## Historical Data Batch Analysis


## GUI
