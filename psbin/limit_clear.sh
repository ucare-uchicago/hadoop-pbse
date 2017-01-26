#!/bin/bash

. cluster_topology.sh

for i in `seq 0 $MAX_NODE`;
do
  ssh -t $HOSTNAME_PREFIX$i "sudo $PSBIN/fast_machine.sh"
done
