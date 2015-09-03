#!/bin/sh

git clone https://github.com/prateek-s/sspark

mv spark spark-back

cd sspark

./build/sbt -Dhadoop.version=1.0.4  clean assembly

cd /root

mv sspark spark

cp -a spark-back/conf spark

pssh -h spark/conf/slaves "rm -rf spark"

spark-ec2/copy-dir spark

echo "DONE"
