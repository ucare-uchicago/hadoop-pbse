#!/bin/bash

if [[ "$#" -ne 1 ]]
  then
    echo "Insufficient argument"
    exit
fi

echo "slowing node-$1"

. cluster_topology.sh

nodeid=$1
ip="10.1.1.$((nodeid+2))"

for i in `seq 0 $MAX_NODE`;
do
  if [ $i -eq $nodeid ]
  then
    echo "node-$i limiting incoming bw"
    ssh -t node-$i "sudo $PSBIN/limit_incoming.sh"
  else
    echo "node-$i limiting outgoing bw to $ip"
    ssh -t node-$i "sudo $PSBIN/limit_outgoing.sh $ip"
  fi

done
