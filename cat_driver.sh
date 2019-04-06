#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./tag_driver.sh [input_location] [output_location]"
    exit 1
fi

hadoop jar /usr/lib/hadoop/hadoop-streaming-2.8.5-amzn-2.jar \
-D mapreduce.job.reduces=3 \
-D mapreduce.job.name='Video category analysis' \
-file cat_mapper.py \
-mapper cat_mapper.py \
-file cat_reducer.py \
-reducer cat_reducer.py \
-input $1 \
-output $2
