#!/bin/sh


hadoop jar $SWIMDIR/workloadSuite/HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf pbse_conf/randomwriter_conf.xsl workGenInput &
