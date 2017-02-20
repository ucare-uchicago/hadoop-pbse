#!/bin/bash

programname=$0

function usage {
    echo "usage: $programname <nodenumber> <ip> <identifier> "
    echo "  nodenumber    Number id of node to be slowed down"
    echo "  ip            The real IP of the slownode"
    echo "  identifier    String that genstats.py detect to identify the slownode."
    echo "                Usually set as the slow node hostname."
    exit 1
}

if [[ "$#" -ne 3 ]]
  then
    usage
fi

. cluster_topology.sh

myhome="/user/$USER"
slownode="${HOSTNAME_PREFIX}$1"

echo "Limping $slownode :: $2 :: $3"

fastnode $1 $2
clstop
clcleanlogs
sleep 5

#cp $PSBIN/ucare_se_conf/writeconf/* $HADOOP_CONF_DIR/
cp $PSBIN/ucare_se_conf/readconf/* $HADOOP_CONF_DIR/
sed_replaceconf.sh

# change slow down node
sed -i "s|<value>pc...</value>|<value>$3</value>|" $HADOOP_CONF_DIR/hdfs-site.xml
sed -i "s|<value>pc...</value>|<value>$3</value>|" $HADOOP_CONF_DIR/mapred-site.xml


# change default heartbeat interval
# sed -i "s/3000/375/" $HADOOP_CONF_DIR/mapred-site.xml

clstart
sleep 10
hc
sleep 10
hdfs dfs -rm -r -f "$myhome/workGenOutputTest*"
slownode $1 $2
ssh -t $CLIENT_NODE "cd $TESTDIR; ./expStart.sh > /tmp/expLog.log & "

sleep 5
cd $TESTDIR/workGenLogs/
cp $PSBIN/genstats.py ./
sed -i "s|^SLOWNODE=.*|SLOWNODE=$1|" genstats.py
sed -i "s|^SLOWHOST=.*|SLOWHOST='$3'|" genstats.py
sed -i "s|^SLOWIP=.*|SLOWIP='$2'|" genstats.py
