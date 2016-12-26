#!/bin/bash

hdfs dfsadmin -report > /tmp/$USER-dfsreport.txt
yarn node -list -states RUNNING > /tmp/$USER-yarnreport.txt
grep "^DFS Used:\|^Name:\|^Live datanodes" /tmp/$USER-dfsreport.txt > /tmp/$USER-dfsused.txt
cat /tmp/$USER-dfsused.txt > /tmp/$USER-healthreport.txt
cat /tmp/$USER-yarnreport.txt >> /tmp/$USER-healthreport.txt
#cat /tmp/$USER-dfsreport >> /tmp/$USER-healthreport.txt
cat /tmp/$USER-healthreport.txt
