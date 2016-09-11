/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.AMtoReduceTask;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

@SuppressWarnings({ "rawtypes" })
public class ReduceTaskImpl extends TaskImpl {
  //huanke
  private static final Log LOG = LogFactory.getLog(ReduceTaskImpl.class);
  
  private final int numMapTasks;
  private final AMtoReduceTask AMtoReduce;

  private List<String> badPipe = null;
  private String badHost = null;
  
  public ReduceTaskImpl(JobId jobId, int partition,
      EventHandler eventHandler, Path jobFile, JobConf conf,
      int numMapTasks, TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext, AMtoReduceTask AMtoReduce) {
    super(jobId, TaskType.REDUCE, partition, eventHandler, jobFile, conf,
        taskAttemptListener, jobToken, credentials, clock,
        appAttemptId, metrics, appContext);
    this.numMapTasks = numMapTasks;
    this.AMtoReduce=AMtoReduce;
  }
  
  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 4);
  }

  @Override
  protected TaskAttemptImpl createAttempt() {
    return new ReduceTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, numMapTasks, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext, AMtoReduce);
  }

  // @Cesar: Do not override, not necessary
  protected TaskAttemptImpl createAttempt(List<String> badPipe, String badHost) {
    return new ReduceTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, numMapTasks, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext, badPipe, badHost);
  }
  
  // @Cesar: Same as default
  @Override
  protected TaskAttemptImpl createAttempt(String slowMapHost) {
    return this.createAttempt();
  }
  
  @Override
  public TaskType getType() {
    return TaskType.REDUCE;
  }

  protected void setBadPipe(List<String> badPipe){
	  this.badPipe = badPipe;
  }
  
  protected void setBadHost(String badHost){
	  this.badHost = badHost;
  }
  
  public List<String> getBadPipe() {
	return badPipe;
  }

  public String getBadHost() {
	return badHost;
  }

//huanke
  protected void updateTaskOutputDN() {

    TaskAttempt lastAttempt = getAttempt(this.lastAttemptId);

    LOG.info("@huanke lastAttempt _h_r "+lastAttempt);
    if(this.lastAttemptId==null){
      LOG.info("@huanke you are null");
    }else{
      LOG.info("@huanke you are last"+this.lastAttemptId.getId());
//      huanke you are last0
//      huanke you are last1
    }
    /*
    * huanke lastAttempt _h_r null
      huanke lastAttempt _h_r null
      huanke lastAttempt _h_r org.apache.hadoop.mapred.ReduceTaskAttemptImpl
    * */
    if (lastAttempt != null) {
      TaskAttemptImpl att = (TaskAttemptImpl) lastAttempt;
      LOG.info("@huanke this.ignoreNode == "+this.ignoreNode);
      if(this.ignoreNode == null){
    	  LOG.info("@huanke: AMToReduce is " + 
    			  (AMtoReduce == null? "null" : "not null") + " and ignore node is " + 
    			  (this.ignoreNode == null? "null" : "not null"));
    	  
      }
      else{
    	  AMtoReduce.setAMtoTaskInfo(this.ignoreNode.getHostName());
      }
//      AMtoReduce.monitor();
//      att.getInfo();
      LOG.info("@huanke piggyback info : "+att.getID()+this.lastAttemptId.getTaskId().getTaskType()+AMtoReduce.getAMtaskInfo());
      /**
       huanke piggyback info : 0REDUCEMaMaMaMa
       huanke piggyback info : 1REDUCEMaMaMaMa
       */

    }
  }

  }
