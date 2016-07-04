#!/bin/bash

hdfs dfsadmin -setBalancerBandwidth 1000000000
hdfs balancer -threshold 5
