package org.apache.hadoop.mapred;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.InputStreamFinder;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;

public class InputStreamInterceptor implements InputStreamFinder {
  
  private static final Log LOG = LogFactory.getLog(InputStreamInterceptor.class.getName());
  
  private MapTask task;
  private TaskReporter reporter;
  private TaskSplitIndex splitMetaInfo;
  
  private boolean sendDatanodeInfo;
  private InputStream inputStream;
  
  private long startTime;
  private long stopTime;
  
  public InputStreamInterceptor(MapTask task, TaskReporter reporter, TaskSplitIndex splitMetaInfo) {
    this.startTime = System.currentTimeMillis();
    this.task = task;
    this.reporter = reporter;
    this.splitMetaInfo = splitMetaInfo;

    this.sendDatanodeInfo =
        task.getConf().getBoolean(MRJobConfig.PBSE_MAP_DATANODE_SEND_REPORT,
            MRJobConfig.DEFAULT_PBSE_MAP_DATANODE_SEND_REPORT);
  }
  
  @Override
  public void setInputStream(InputStream istream) {
    stopTime = System.currentTimeMillis();
    LOG.info("riza: InputStream lookup took " + (stopTime - startTime) + " ms");
    inputStream = istream;
    if ((inputStream instanceof HdfsDataInputStream) && sendDatanodeInfo) {
      LOG.info("Initiating PBSE datanode reporting");
      HdfsDataInputStream hdis = (HdfsDataInputStream) inputStream;
      switchDatanode(hdis, reporter);
      reporter.setInputStream(hdis);
    } else {
      LOG.warn("no faread or input stream is not instance of HdfsDataInputStream "
          + inputStream.toString());
    }
  }
  
  /**
   * riza: switch datanode with one obtained from splitMetaInfo.
   * {@code TaskSplitIndex#getSlowShufflingMap()} from PBSE-Slow-Shuffle-1 is
   * prioritized over {@code TaskSplitIndex#getLastDatanodeID()}, means that if
   * both is not empty, it will ignore slow shuffling map rather than
   * lastDatanodeID, <b>EXCEPT</b> if this task is the first attempt.
   * 
   * @param hdis
   */
  private void switchDatanode(HdfsDataInputStream hdis, TaskReporter reporter){
    try{
      if ((hdis.getCurrentOrChoosenDatanode() != null) &&
          !DatanodeID.nullDatanodeID.equals(hdis.getCurrentOrChoosenDatanode())) 
        LOG.info("riza: was trying to read from " + hdis.getCurrentOrChoosenDatanode().getXferAddr()
            + " probing new datanode for pos=" + hdis.getPos());
      else
        LOG.warn("riza: HdfsDataInputStream datanode is null or not choosen yet");

      DatanodeID initial = hdis.getCurrentOrChoosenDatanode();

      if (task.getTaskID().getId() > 0 && !splitMetaInfo.getSlowShufflingMap().isEmpty()) {
        LOG.debug("riza: ignoring datanode by hostname: " + splitMetaInfo.getSlowShufflingMap());
        hdis.switchDatanode(splitMetaInfo.getSlowShufflingMap());
      } else {
        LOG.debug("riza: ignoring datanode by DatanodeID: " + splitMetaInfo.getLastDatanodeID());
        hdis.switchDatanode(splitMetaInfo.getLastDatanodeID());
      }
      
      if (hdis.getIgnoredDatanode() != null
          && !hdis.getIgnoredDatanode().equals(DatanodeID.nullDatanodeID)) {
        LOG.info("PBSE-Read-1: ignored " + hdis.getIgnoredDatanode()
            + " current " + hdis.getCurrentOrChoosenDatanode());
      }

//      if (reporter != null && !initial.equals(hdis.getCurrentOrChoosenDatanode())) {
//        reporter.setInputStream(hdis);
//      }
    } catch (Exception e){
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      LOG.error(sw.toString());
    }
  }

}
