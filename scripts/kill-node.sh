#!/bin/bash
SPARK_HOME=/root/spark

$SPARK_HOME/sbin/stop-this-slave.sh
#HDFS stop-dfs.sh
echo 3 > /proc/sys/vm/drop_caches

echo "______________ CLEARING ON-DISK TMP_______________"

#rm -rf /mnt/spark/* #For tmp results?

#rm -rf /tmp/*



#HDFS


