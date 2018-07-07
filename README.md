# Fin-Doc-Thermometer


## Brief Introduction
This project has built a real-time monitoring system to track visits to electronic financial documents available in The Division of Economic and Risk Analysis (DERA). 

The system consists of a streaming pipeline and a batch pipeline. The streaming pipeline deals with real-time data, tracking what companies attract people's most attention and the total of documents that have been visited by second. The batch pipeline generates statistics from historical data to help describe how active the public are in checking documents. 


## Pipeline
The whole system is illustrated as follows:
![Pipeline Image](https://github.com/Eethen/Fin-Doc-Thermometer/blob/master/Images/Screen%20Shot%202018-07-02%20at%202.35.16%20PM.png)

**Kafka:**  receives and buffers real-time data from S3
**Spark Streaming:** consumes real-time data from Kafka and calculates total visits by second and total visits by second and company
**Redis:** receives from Spark Streaming and holds total visits by second and total visits by second and company
**Spark:** loads historical data from S3, generates statistics and save them to PostgreSQL
**PostgreSQL:** saves raw data and the calculated statistics
**Flask:** displays real-time result from Redis and calculated statistics from PostgreSQL


## Github Repo Structure
**./DataDownload/**  : a script that downloads, unzips and uploads to AWS S3 bucket, the historical data.

**./Images/**        : a pipeline .png file as shown above.

**./Kafka/**         : a python script that updates the historical data in S3 with real-time time stamp and publishes them to the Kafka cluster.

**./Spark/**         : a bash script and a python script that generate, and store to PostgreSQL, the 25th, the 75th and the 95th percentiles by second.

**./SparkStreming/** : a bash script and a python script that track, and store to Redis, the total visits by company ID and by time.

**./Flask/**         : Python and HTML scripts that establish the Graphical User Interface.


## Background, Data and Motivation

### Background:
US Securities and Exchange Commission require companies to file registration statements and periodic reports. All those information are available to the public and are well studied by researchers and investors. 

### Data:
Historical data from June 2017 to January 2003 are available in CSV files from US Securities and Exchange Commission website. The data are stored by day and come with fields, such as IP, date, time, CIK, accession and extension. While IP, date and time are self-evident, CIK is an unique ID to each filer, and accession along with extension can uniquely determine a specific document given a CIK. 

### Motivation
This system is intended to enable users, such as data analysts and quantitative analysts, to track the most visited companies in a real-time manner and to leverage historical information to help describe the visitors' activeness in general.
