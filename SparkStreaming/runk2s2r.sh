#!/bin/bash

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --master spark://ip-****:7077 --executor-memory 5G k2s2r.py
