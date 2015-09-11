#!/bin/bash
# ./start_expt.sh -b pagerank -c none -k 0 -r full
origargs=$@ 
SPARK_HOME=/root/spark


while [[ $# > 1 ]]
do
key="$1"

case $key in
    -b|--benchmark)
    BENCHMARK="$2"
    shift # past argument
    ;;
    -c|--checkpoint)
    CKPT="$2"
    shift # past argument
    ;;
    -k|--tokill)
    TOKILL="$2"
    shift # past argument
    ;;
    -r|--replenish)
    REPLENISH="$2"
    shift # past argument
    ;;
    --default)
    DEFAULT=YES
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done


resultshome="/root/results"
mkdir $resultshome
progdir="$BENCHMARK"_"$CKPT"_"$TOKILL"_"$REPLENISH"
resultsdir=$resultshome/$progdir
mkdir $resultsdir

echo "Clean Disks on Spark"

pssh -h $SPARK_HOME/conf/slaves "rm /root/spark/work/* -rf"
pssh -h $SPARK_HOME/conf/slaves "rm /mnt/spark/* -rf"

echo "start all slaves"

$SPARK_HOME/sbin/start-all.sh

echo "-------------- Created Directories : $resultsdir"

date >> "$resultsdir"/start
echo "$origargs" >> "$resultsdir"/args
echo "$origargs" >> /root/latest
cp $SPARK_HOME/scripts/start_expt.sh "$resultsdir"/start_expt.sh

outputfile=$resultsdir/time

echo "--------------- Spark Config --------------------------"

sparkconfig=""
if [ "$CKPT" == "opt" ];
then
    $sparkconfig="spark.checkpointing.policy    opt 
spark.checkpointing.tau      0.2
spark.checkpointing.dir     /root/ckpts"

    echo $sparkconfig >> $SPARK_HOME/conf/spark-defaults.conf
    echo "COPYING NEW CONF DIR"
    /root/spark-ec2/copy-dir /spark/conf/
    exit

elif [ "$CKPT" == "none" ];
then
    $sparkconfig=""
    echo "Default conf is good conf, nothing to do"
fi

echo "-------------------- Spark --------------------------"
starttime=`date +%s`

if [ "$BENCHMARK" == "pagerank" ];
then
    echo "$BENCHMARK !"
    #programname="graphx.LiveJournalPageRank"
    #params="s3n://prtk1/sparkdata/part-r-0000[1-7] --numEPart=20"
    programname="SparkPageRank"
    params="s3n://prtk1/sparkdata/part-r-0000[1-2] 10"

elif [ "$BENCHMARK" == "als" ];
then
    echo "$BENCHMARK !"
    programname="mllib.MovieLensALS"
    params="s3n://lassALS/movielens00[6-7][0-9].txt --rank 5"

elif [ "$BENCHMARK" == "kmeans" ];
then
    echo "$BENCHMARK !"
    programname="mllib.DenseKMeans"
    params="s3n://lassKmeans/kmp_[1-30].txt -k 500 --numIterations 100"

fi
echo "run-example $programname $params"

nohup time $SPARK_HOME/bin/run-example $programname $params > $outputfile 2>&1 &

echo "job should be running. Now sleeping...?"

if [ "$tokill" != 0 ];
then
    ### Expt has started. 
    sleeptime=900 #15 minutes default
#
    sleep $sleeptime

#kill nodes 

    echo "------------- Wake up to kill nodes -------------"
    slavestokill=`cat $SPARK_HOME/conf/slaves | head -n $TOKILL`

    pssh -H "$slavestokill" "$SPARK_HOME/scripts/kill-node.sh"
fi
#This kills spark worker AND hdfs

if [ "$tokill" != 0 ] && [ "$REPLENISH" == "full" ];
then
    sleep 100
    $SPARK_HOME/sbin/start-all.sh
fi

#

echo ">>>>>>>>>> NOW WAIT FOR EXPERIMENT TO FINISH >>>>>>>>>>>>>> "


while true 
do
    sleep 10
    wget -q http://localhost:8080/json -O json
    appstate=`cat json | jq '.activeapps|.[0].state'`
    numrunning=`cat json | jq '.activeapps|length'`
    if [ "$appstate" != "\"RUNNING\"" ] && [ "$numrunning" == 0 ];
    then
	sparkrunning=`jps | grep -c SparkSubmit`
	if [ "$sparkrunning" != 0 ]; 
	then
	    echo "sparkprocess still running. Exit anyway"
	fi
	
	echo "EXPT DONE!!!!"
	endtime=`date +%s`
	td=$(($endtime-$starttime))
	echo $td >> $resultsdir/timediff
	exit 
    fi
    rm json
done

