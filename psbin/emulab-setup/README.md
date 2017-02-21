

This is a walkthrough to setup Hadoop and UCARE_SE scripts in emulab
environment.


# Layout

This walktrough will set you up with vanilla hadoop-2.7.1 using 4
node.  We will dedicate node-0 to host HDFS and YARN manager (ie,
NameNode, ResourceManager, and HistoryServer), and the other 3 node as
worker node (host DataNode and NodeManager). In some experiment, you
might also want to dedicate 1 other node as client node. But in this
walkthrough, we will use node-0 as client node also.

Couple of directories that we will use are:

* `/proj/ucare/auto-hadoop/` - This will be our project directory where
  we put hadoop binaries and UCARE_SE scripts. This directory will be
  accessible later through $PR environment variables. Please make
  yourself a different project dir, like /proj/ucare/your-name. Please
  note that, similar as your emulab home directory, /proj/ucare is
  also a Network File System (NFS) directory that visible from every
  node. We prefer putting hadoop and ucare_se scripts here in NFS so
  that we have single common place that accessible by all nodes.

* `/mnt/extra/dfs-ucare/` - This is the directory where we store the HDFS
  data blocks. In this walkthrough, we will request 20GB additional
  blockstore for each node to be mounted in path /mnt/extra through
  emulab NS file.

* `/tmp/hadoop-ucare/` - This is the directory where we put all hadoop
  logs.

Please make sure you use bash as your shell choice in your emulab profile.



# Allocating machines in Emulab

Emulab allocate machines and set network topology using instruction
from ns file. We will allocate 4 emulab nodes of type d430. In each of
this node, we will also allocate additional 20GB blockstore, mounted
on /mnt/extra to store HDFS data blocks. The OS image that we will use
in this NS File.

Let say you create a new experiment named "auto-hadoop". Put ns script
bellow as auto-hadoop ns file.

```
set ns [new Simulator]
source tb_compat.tcl

set maxnodes 4
set lanstr ""

# Create LAN including node from 1 to N
for { set i 0} { $i < $maxnodes } {incr i} {
    set node($i) [$ns node]
    append lanstr "$node($i) "

    # set machine type
    tb-set-hardware $node($i) d430

    # set OS for machine
    tb-set-node-os $node($i) UBUNTU14-64-JAVA

    # add additional block storage
    set bs($i) [$ns blockstore]
    $bs($i) set-class "local"
    $bs($i) set-size "20GB"
    $bs($i) set-placement "nonsysvol"
    $bs($i) set-mount-point "/mnt/extra"
    $bs($i) set-node $node($i)
}

# make lan connection for all nodes
set lan0 [$ns make-lan "$lanstr" 1000Mb 0ms]

# use static routing
$ns rtproto Static
$ns run
```

This ns file will allocate node-0, node-1, node-2, and node-3, each
with ip from 10.1.1.2 - 10.1.1.5 respectively.

*IMPORTANT*: always use this naming convention (node-$i) and always
 start $i from 0, or the scripts below will break.



# Login to Emulab

Make sure you have paswordless ssh set up, and the login to node-0 of
auto-hadoop experiment:

```
ssh users.emulab.net
ssh node-0.auto-hadoop.ucare.emulab.net
```

# Automatically Install Hadoop

Go to your project directory, download and run the setup script

```
cd /proj/ucare/auto-hadoop
wget https://raw.githubusercontent.com/ucare-uchicago/hadoop/ucare_se/psbin/emulab-setup/setup-hadoop.sh
chmod 774 setup-hadoop
./setup-hadoop
```

This script will download hadoop-2.7.1, hadoop-ucare psbin directory,
and SWIM benchmark (you don't need SWIM for now, but let it be).

```
riza@node-0:/proj/ucare/auto-hadoop$ ls -1
hadoop-2.7.1
hadoop-ucare
setup-nfs.sh
SWIM
```

Source `hadoop-ucare/psbin/ucare_se-rc.sh` from inside .bashrc and
.bash_profile

```
echo "source /proj/ucare/auto-hadoop/hadoop-ucare/psbin/ucare_se-rc.sh" >> .bashrc
echo "source /proj/ucare/auto-hadoop/hadoop-ucare/psbin/ucare_se-rc.sh" >> .bash_profile
```

Log out from node-0, and then log in again. Now you should have every
environment variables point to correct destination. Test it by running

```
printenv | egrep "(HADOOP)|(YARN)|(PSBIN)|(PR)"
```

You should get output similar like:

```
HADOOP_LOG_DIR=/tmp/hadoop-ucare/logs/hadoop
HADOOP_HOME=/proj/ucare/auto-hadoop/hadoop-ucare/psbin/../../hadoop-2.7.1
HADOOP_PREFIX=/proj/ucare/auto-hadoop/hadoop-ucare/psbin/../../hadoop-2.7.1
PSBIN=/proj/ucare/auto-hadoop/hadoop-ucare/psbin
PR=/proj/ucare/auto-hadoop/hadoop-ucare/psbin/../..
HADOOP_CLASSPATH=/usr/lib/jvm/java-7-openjdk-amd64lib/tools.jar
HADOOP_CONF_DIR=/proj/ucare/auto-hadoop/hadoop-ucare/psbin/ucare_se_conf/hadoop-etc/hadoop-2.7.1
YARN_LOG_DIR=/tmp/hadoop-ucare/logs/yarn
HADOOP_MAPRED_LOG_DIR=/tmp/hadoop-ucare/logs/mapred
```

Try ssh to other node and test it too from that node. You should see
exactly the same output.



# Configure Hadoop

Next step we need to do is configure the hadoop installation. The
$PSBIN directory contains customizable config template and useful
scripts to control hadoop nodes. To utilize the $PSBIN scripts, first
we need edit the values in the $PSBIN/cluster_topology.sh

We will dedicate node-0 (having ip 10.1.1.2) to host NameNode,
ResourceManager, and also as client node. So edit
$PSBIN/cluster_topology.sh like this:

```
MAX_NODE=3
HOSTNAME_PREFIX="node-"

YARN_RM="${HOSTNAME_PREFIX}0"
YARN_RM_IP=10.1.1.2
YARN_RM_HOSTNAME="${HOSTNAME_PREFIX}0"
HDFS_NN="${HOSTNAME_PREFIX}0"
HDFS_NN_IP=10.1.1.2
CLIENT_NODE="${HOSTNAME_PREFIX}0"
SLAVES_FILE="$PSBIN/ucare_se_conf/hadoop-etc/hadoop-2.7.1/slaves"
```

Edit SLAVES_FILE to list our worker nodes
```
node-1
node-2
node-3
```

Next, from $PSBIN directory, format HDFS NameNode by running
`clformat`, followed by `clstart` to start up your hadoop cluster.

Thats it! You have your Hadoop cluster up and running!

# Swap In/Out

Note that the installation part is one time setup only. If you ever
swap out your experiment, and swap in with different ns file (like
using more nodes), just do the configuration part again.

If you want to restart everything from scratch, swap out your
experiment, rm -rf your project dir (in this example,
`/proj/ucare/auto-hadoop`), and redo the installation and
configuration part again.

*WARNING!!* Never touch other directories under `/proj/ucare`. That
 directories belong to other group members and research project. You
 have been warned!
