#!/bin/bash
# ===============================================================================================================
# PARAMETERS
# ===============================================================================================================
# notice that all rates should include the unit, something like 512kbit or 1mbit

slowRate=$1
let "subclassRate=1000-slowRate"

SLOW_RATE=${slowRate}mbit
MAX_TOTAL_RATE=1000mbit
MAX_SUBCLASS_RATE=${subclassRate}mbit

# prelude from remote devices, like eth0, eth1, ...
REMOTE_DEVICE_PRELUDE="eth"
# asume only one local device per machine
LOCAL_DEVICE_PRELUDE="lo"
# number of remote devices. eth0...eth${NUM_REMOTE_DEVICES} will be slowed down
NUM_REMOTE_DEVICES=6
# ===============================================================================================================
# FUNCTIONS
# ===============================================================================================================
# slows down a device. Check the parent 1:0 classid 1:1 htb rate ${MAX_TOTAL_RATE}mbit ceil ${MAX_TOTAL_RATE}mbit
# and 1:1 classid 1:2 htb rate ${MAX_SUBCLASS_RATE}mbit ceil ${MAX_SUBCLASS_RATE}mbit.
#This should match a realistic configuration for the network card (yes, when i set really
#large nums i have problems, so i adjust this. The problems are that this script basically does
# not work or it works but slows down all ports)
# a reference for a catch all filter (at http://serverfault.com/questions/320608/traffic-shaping-tc-filter-catch-all-filter)
# is tc filter add dev $IF_LAN parent 1: protocol ip prio 7 u32 match ip dst 0.0.0.0/0 flowid 1:190
# Input: 
#	$1 target device name
#	$2 rate for the parent queue
#	$3 rate for the fast child queue
#	$4 rate for the slow child queue
slowDevice(){
	# clear device (dont show message)
	tc qdisc del dev $1 root > /dev/null 2>&1
	echo "============================================================"
	echo "Slowing down interface $1 to $4"
	echo "============================================================"
	# add handling classes
	tc qdisc add dev $1 root handle 1:0 htb default 2
	tc class add dev $1 parent 1:0 classid 1:1 htb rate $2 ceil $2
	tc class add dev $1 parent 1:1 classid 1:2 htb rate $3 ceil $3
	tc class add dev $1 parent 1:1 classid 1:3 htb rate $4 ceil $4
	tc qdisc add dev $1 parent 1:2 sfq
	tc qdisc add dev $1 parent 1:3 sfq
	# filter by ip
	tc filter add dev $1 parent 1:0 protocol ip u32 match ip dst 0.0.0.0/0 flowid 1:3
	tc filter add dev $1 parent 1:0 protocol ip u32 match ip src 0.0.0.0/0 flowid 1:3
    # done
}
# ===============================================================================================================
# MAIN
# ===============================================================================================================
# call the function for each device
for (( deviceId=0; deviceId<=$NUM_REMOTE_DEVICES; deviceId++ ))
do
	slowDevice ${REMOTE_DEVICE_PRELUDE}${deviceId} $MAX_TOTAL_RATE $MAX_SUBCLASS_RATE $SLOW_RATE
done
# also limp the local interface, dont want local communication to be fast
# slowDevice  $LOCAL_DEVICE_PRELUDE $MAX_TOTAL_RATE $MAX_SUBCLASS_RATE $SLOW_RATE
# done
