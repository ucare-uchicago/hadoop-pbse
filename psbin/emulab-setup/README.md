

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

* /proj/ucare/auto-hadoop/ - This will be our project directory where
  we put hadoop binaries and UCARE_SE scripts. This directory will be
  accessible later through $PR environment variables. Please make
  yourself a different project dir, like /proj/ucare/your-name. Please
  note that, similar as your emulab home directory, /proj/ucare is
  also a Network File System (NFS) directory that visible from every
  node. We prefer putting hadoop and ucare_se scripts here in NFS so
  that we have single common place that accessible by all nodes.

* /mnt/extra/dfs-ucare/ - This is the directory where we store the HDFS
  data blocks. In this walkthrough, we will request 20GB additional
  blockstore for each node to be mounted in path /mnt/extra through
  emulab NS file.

* /tmp/hadoop-ucare/ - This is the directory where we put all hadoop
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



# Login to Emulab

Make sure you have paswordless ssh set up, and the login to node-0 of
auto-hadoop experiment:

```
ssh users.emulab.net
ssh node-0.auto-hadoop.ucare.emulab.net
```

# Automatically Install Hadoop

