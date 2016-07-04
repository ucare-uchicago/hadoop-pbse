#!/bin/bash

if [[ "$#" -ne 2 ]]
  then
    echo "Insufficient argument"
    exit
fi

echo "Limping node-$1:$2"

myhome=`readlink -e ~/`

fastnode $1
ssh -t node-17 "clstop;"
clcleanlogs
sleep 5

cp pbse_conf/readconf/* $HADOOP_CONF_DIR/
sed -i "s/pc001/$2/" $HADOOP_CONF_DIR/mapred-site.xml

# change default heartbeat interval
sed -i "s/3000/375/" $HADOOP_CONF_DIR/mapred-site.xml

ssh -t node-17 "clstart;"
sleep 10
hc
hdfs dfs -rm -r -f "$myhome/workGenOutputTest*"
sleep 10
#slownode $1
ssh -t node-0 "cd $TESTDIR; ./expStart.sh & echo $! > ~/swim.pid;"

sleep 5
cd $TESTDIR/workGenLogs/
cp $PSBIN/genstats.py ./
sed -i "s/^SLOWNODE=.*/SLOWNODE=$1/" genstats.py
sed -i "s/^SLOWHOST=.*/SLOWHOST="\""$2"\""/" genstats.py
