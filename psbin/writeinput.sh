#!/bin/bash

ssh -t node-17 "clstop;"
clcleanlogs
clreset
clsetup
ssh -t node-16 "dfsformat;"
cp pbse_conf/writeconf/* $HADOOP_CONF_DIR/

ssh -t node-17 "clstart;sleep 10;"
ssh -t node-0 "prep_run.sh > /tmp/writeinput.log &;"
