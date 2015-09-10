#!/usr/bin/env bash

export SPARK_LOCAL_DIRS="/mnt/spark"

# Standalone cluster options
export SPARK_MASTER_OPTS=""
export SPARK_WORKER_INSTANCES=1
export SPARK_WORKER_CORES=2

export HADOOP_HOME="/root/persistent-hdfs"
export SPARK_MASTER_IP=ec2-54-237-113-55.compute-1.amazonaws.com
export MASTER=`cat /root/spark-ec2/cluster-url`

export SPARK_SUBMIT_LIBRARY_PATH="$SPARK_SUBMIT_LIBRARY_PATH:/root/persistent-hdfs/lib/native/"
export SPARK_SUBMIT_CLASSPATH="$SPARK_CLASSPATH:$SPARK_SUBMIT_CLASSPATH:/root/persistent-hdfs/conf"

# Bind Spark's web UIs to this machine's public EC2 hostname:
export SPARK_PUBLIC_DNS=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`

# Set a high ulimit for large shuffles
ulimit -n 1000000
