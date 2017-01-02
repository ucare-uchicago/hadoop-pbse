#!/bin/bash

. cluster_topology.sh

sed -i "s/HDFS_NN/$HDFS_NN/" $HADOOP_CONF_DIR/core-site.xml;
sed -i "s/HDFS_NN_IP/$HDFS_NN_IP/" $HADOOP_CONF_DIR/hdfs-site.xml;
sed -i "s|SLAVES_FILE|$SLAVES_FILE|" $HADOOP_CONF_DIR/hdfs-site.xml;
sed -i "s/YARN_RM_IP/$YARN_RM_IP/" $HADOOP_CONF_DIR/mapred-site.xml;
sed -i "s/YARN_RM_HOSTNAME/$YARN_RM_HOSTNAME/" $HADOOP_CONF_DIR/yarn-site.xml;

