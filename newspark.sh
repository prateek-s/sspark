#!/bin/bash

/root/spark/sbin/stop-all.sh

#pssh -h /root/spark/conf/slaves "rm /root/spark -rf"   

prsync -h /root/spark/conf/slaves -av /root/spark/assembly /root/spark/
prsync -h /root/spark/conf/slaves -av /root/spark/bin /root/spark/
prsync -h /root/spark/conf/slaves -av /root/spark/examples /root/spark/
prsync -h /root/spark/conf/slaves -av /root/spark/lib_managed /root/spark/
prsync -h /root/spark/conf/slaves -av /root/spark/sbin /root/spark/
prsync -h /root/spark/conf/slaves -av /root/spark/scripts /root/spark/

/root/spark/sbin/start-all.sh