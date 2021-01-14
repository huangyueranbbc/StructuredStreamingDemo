#!/bin/bash

export SPARK_KAFKA_VERSION=0.10

spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --num-executors 2 \
 --driver-memory 2g \
 --executor-memory 2g \
 --executor-cores 2 \
 --conf spark.executor.memoryOverhead=4096 \
 --class com.hyr.structured.streaming.sink.StructuredStreamingForeachSink \
 --conf spark.dynamicAllocation.enabled=false \
 --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails" \
 --principal xxxx \
 --keytab xxxx.keytab \
StructuredStreamingDemo-1.0-SNAPSHOT-jar-with-dependencies.jar