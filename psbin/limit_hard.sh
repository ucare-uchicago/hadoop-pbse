#!/bin/bash

if [[ "$#" -ne 2 ]]
  then
    echo "Insufficient argument"
    exit
fi

. cluster_topology.sh

echo "slowing ${HOSTNAME_PREFIX}$1 :: $2"

nodeid=$1
ip=$2

for i in `seq 0 $MAX_NODE`;
do
  if [ $i -eq 8 -o $i -eq 9 ]
  then
    echo "${HOSTNAME_PREFIX}$i limiting incoming bw"
    ssh -t ${HOSTNAME_PREFIX}$i "sudo $PSBIN/limit_incoming.sh"
  else
    echo "${HOSTNAME_PREFIX}$i limiting outgoing bw to $ip"
    ssh -t ${HOSTNAME_PREFIX}$i "sudo $PSBIN/limit_out_hard.sh $ip"
  fi

done
