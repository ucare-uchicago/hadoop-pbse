#!/bin/bash

. cluster_topology.sh

ssh -t $YARN_RM "clstop;"
cp ucare_se_conf/writeconf/* $HADOOP_CONF_DIR/
sed_replaceconf.sh
clcleanlogs
clreset
clsetup
ssh -t $HDFS_NN "dfsformat;"

ssh -t $YARN_RM "clstart;sleep 10;"
ssh -t $CLIENT_NODE "prep_run.sh > /tmp/writeinput.log &;"
