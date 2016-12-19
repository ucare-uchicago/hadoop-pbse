

UCARE_SE Scripts

First, copy the entire of psbin folder to the hadoop installation
folder, such as:

/proj/ucare/riza/hadoop-2.7.1.faread/psbin


SETUP ENV

1. Setup Hadoop and UCARE_SE environtment vars
   Check out riza_cshrc and pad these vars to your ~/.cshrc.
   Update the values according to your directory setup.

2. Setup your Hadoop cluster
   - slaves
   - hdfs-site.xml
   - mapred-site
   - yarn-site.xml
   - capacity-scheduler.xml
   (example at psbin/ucare_se_conf/hadoop-etc/hadoop-2.7.1/)

3. MAKE SURE to sync your mapred-site.xml and yarn-site.xml with the
   one at ucare_se_conf/writeconf and ucare_se_conf/readconf

   writeconf : configuration for SWIM input write
   readconf  : configuration for SWIM benchmark

   WARNING: the actual config files at $HADOOP_CONF_DIR will be
   replaced with these one on input write or benchmark phase.



SETUP EXPERIMENT

1. Generate your SWIM benchmark and note the input sizes.
   Update files ucare_se_conf/*.xsl according to the input size you just
   generate.

   Refer to https://github.com/SWIMProjectUCB/SWIM/wiki/Performance-measurement-by-executing-synthetic-or-historical-workloads

2. We usually running just the first 150 of job of SWIM benchmark. Thus:

   cd $SWIMDIR
   cp run-jobs-all.sh expStart.sh

   In expStart, remove job script >= run-job-150.sh (first job is run-job-0.sh)



RUNNING EXPERIMENT

1. Writing SWIM Input

   cd $PSBIN
   ./writeinput.sh

2. Running benchmark
   Note node number and physical id of node that you want to slowdown on experiment
   example, for node-8 with node id pc712:

   cd $PSBIN
   ./runbenchmark.sh 8 pc712

   If you don't wish to slowdown node, run with undefined cluster value, such as:

   ./runbenchmark.sh 20 VOID

3. Examining result

   cd $TESTDIR/workGenLogs/
   oc

4. Saving Hadoop logs

   cd $PSBIN
   ./savelogs.sh log_desc


MAKING GRAPH FROM ONE/MULTIPLE EXPERIMENT RUN

1. Copy your saved-compressed logs to /tmp/ or any safe directory, and
   untar it.

2. cd to the folder. Inside you will have script genstats.py from
   runbenchmark script. Run it from this derectory to create
   experiment graph and summarized json data.

   ./genstats.py

3. Rename data.json to new name reflecting the experiment where it
   came from. Save it.

4. To merge data from multiple experiment into single graph, copy the
   json from each experiment and script $PSBIN/combinestats.py into
   single directory.
   Run combinestats.py from that directory.

   ./combinestats.py

5. Line style of series from specific json file can be directly edited
   from the json file. Inside the json file, search "conf". Inside
   conf, edit the "line_style" and "linewidth" as desired.
