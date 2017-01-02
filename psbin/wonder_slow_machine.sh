#!/bin/bash
# ===============================================================================================================
# PARAMETERS
# ===============================================================================================================
# notice that all rates are in kbit per sec
#########################################
# 0.1 MB SLOW DOWN
#########################################
# DOWN_RATE=100 
# UP_RATE=100
#########################################
# 1 MB SLOW DOWN
#########################################
DOWN_RATE=1000 
UP_RATE=1000
#########################################
# 10 MB SLOW DOWN
#########################################
# DOWN_RATE=10000 
# UP_RATE=10000
#########################################
# 100 MB SLOW DOWN
#########################################
# DOWN_RATE=100000 
# UP_RATE=100000
#########################################
# prelude from remote devices, like eth0, eth1, ...
REMOTE_DEVICE_PRELUDE="eth"
# asume only one local device per machine
LOCAL_DEVICE_PRELUDE="lo"
# number of remote devices. eth0...eth${NUM_REMOTE_DEVICES} will be slowed down
NUM_REMOTE_DEVICES=6
# ===============================================================================================================
# FUNCTIONS
# ===============================================================================================================
# slows down a device using wonder shaper (sudo apt-get install wondershaper)
# Input:
#	$1 target device name
#	$2 download speed
#	$3 upload speed
# All speed MUST BE IN KBITS PER SEC
slowDevice(){
	wondershaper $1 $2 $3
    # done
}
# ===============================================================================================================
# MAIN
# ===============================================================================================================
# call the function for each device
for (( deviceId=0; deviceId<=$NUM_REMOTE_DEVICES; deviceId++ ))
do
	slowDevice ${REMOTE_DEVICE_PRELUDE}${deviceId} $DOWN_RATE $UP_RATE
done
# also limp the local interface, dont want local communication to be fast
# slowDevice  $LOCAL_DEVICE_PRELUDE $DOWN_RATE $UP_RATE
# done
