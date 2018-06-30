#!/bin/bash

## calculating Quarter
if (( $(($2 % 3)) == 0 ))
then
    qtr=$(($2 / 3))
else
    qtr=$(($(($2 / 3)) + 1))
fi

## standardize month as MM
if (($2 < 10))
then
    mth=0$2
else
    mth=$2
fi

## generate a range of dates
for i in $(seq $3 $4)
do
    if ((i < 10))
    then
        day=0$i
    else
        day=$i
    fi

## download to .
    wget  "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/${1}/Qtr${qtr}/log${1}${mth}${day}.zip"
done

## unzip *.zip
yes yes | unzip "*.zip"

## upload to S3 bucket
for y in log*.csv; do aws s3 cp $y s3://*****/data/$y; done
