#!/bin/bash

if [[ "$#" -ne 2 ]]
  then
    echo "Insufficient argument"
    exit
fi

. cluster_topology.sh

echo "Limping node-$1:$2"

myhome=`readlink -e ~/`

fastnode $1
ssh -t $YARN_RM "clstop;"
clcleanlogs
sleep 5

cp ucare_se_conf/readconf/* $HADOOP_CONF_DIR/

# change slow down node
sed -i "s/pc001/$2/" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i "s/pc001/$2/" $HADOOP_CONF_DIR/mapred-site.xml


# change default heartbeat interval
# sed -i "s/3000/375/" $HADOOP_CONF_DIR/mapred-site.xml

ssh -t $YARN_RM "clstart;"
sleep 10
hc
hdfs dfs -rm -r -f "$myhome/workGenOutputTest*"
sleep 10
#slownode $1
ssh -t $CLIENT_NODE "cd $TESTDIR; ./expStart.sh & echo $! > ~/swim.pid;"

sleep 5
cd $TESTDIR/workGenLogs/
cp $PSBIN/genstats.py ./
sed -i "s/^SLOWNODE=.*/SLOWNODE=$1/" genstats.py
sed -i "s/^SLOWHOST=.*/SLOWHOST="\""$2"\""/" genstats.py
