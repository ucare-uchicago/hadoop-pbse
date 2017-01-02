#!/bin/bash
# ===============================================================================================================
# PARAMETERS
# ===============================================================================================================
# prelude from remote devices, like eth0, eth1, ...
REMOTE_DEVICE_PRELUDE="eth"
# asume only one local device per machine
LOCAL_DEVICE_PRELUDE="lo"
# number of remote devices. eth0...eth${NUM_REMOTE_DEVICES} will be slowed down
NUM_REMOTE_DEVICES=6
# ===============================================================================================================
# FUNCTIONS
# ===============================================================================================================
# Input:
#	$1 device name
fastDevice(){
	# clear device
	echo "============================================================"
	echo "Clearing device $1"
	echo "============================================================"
	wondershaper remove $1
}
# ===============================================================================================================
# MAIN
# ===============================================================================================================
# call the function for each device
for (( deviceId=0; deviceId<=$NUM_REMOTE_DEVICES; deviceId++ ))
do
	fastDevice ${REMOTE_DEVICE_PRELUDE}${deviceId}
done
# also clean the local interface
# fastDevice  $LOCAL_DEVICE_PRELUDE
# done
