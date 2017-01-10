#!/bin/bash

. cluster_topology.sh

for i in `seq 0 $MAX_NODE`;
do
  ssh -t node-$i "sudo $PSBIN/fast_machine.sh"
done
