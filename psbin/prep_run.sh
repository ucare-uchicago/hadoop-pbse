#!/bin/sh


hadoop jar $SWIMDIR/workloadSuite/HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf $PSBIN/pbse_conf/randomwriter_conf.xsl workGenInput &
