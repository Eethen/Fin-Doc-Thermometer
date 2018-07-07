#!/bin/bash

spark-submit     --master spark://ip-10-0-0-5:7077\
                 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0\
                 --executor-memory 4G\
                 getPercentiles.py
