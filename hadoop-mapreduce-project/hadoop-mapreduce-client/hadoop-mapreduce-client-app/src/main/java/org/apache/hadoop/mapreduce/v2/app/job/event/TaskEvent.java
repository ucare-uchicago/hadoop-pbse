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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

import java.util.List;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * this class encapsulates task related events.
 *
 */
public class TaskEvent extends AbstractEvent<TaskEventType> {

  private TaskId taskID;
  // @Cesar: This mapper is slow during shuffle
  private String slowMapper = null; 
  // @Cesar: This the task attempt id
  private TaskAttemptId slowMapperAttemptId = null;
  // huanke
  private DatanodeInfo ignoreNode;
  // @Cesar
  private List<String> badPipe = null;
  private String badHost = null;
  private boolean writeDiversity = false;
  private TaskAttemptId targetAttempt = null;
  
  public TaskEvent(TaskId taskID, TaskEventType type) {
    super(type);
    this.taskID = taskID;
  }

  // @Cesar: Added for support
  public TaskEvent(TaskId taskID, TaskEventType type, String slowMapper, TaskAttemptId attemptId) {
    super(type);
    this.taskID = taskID;
    this.slowMapper = slowMapper;
    this.slowMapperAttemptId = attemptId;
  }

  //@Cesar: construct a new TaskEvent with bad pipeline
  public TaskEvent(TaskId taskID, TaskEventType type, 
		  			List<String> badPipe, String badHost,
		  			boolean isWriteDiversity) {
    super(type);
    this.taskID = taskID;
    this.badPipe = badPipe;
    this.badHost = badHost;
    this.writeDiversity = isWriteDiversity;
  }
  
  //@Cesar: construct a new TaskEvent with bad pipeline and target attempt
  public TaskEvent(TaskId taskID, TaskEventType type, 
		  			List<String> badPipe, String badHost,
		  			boolean isWriteDiversity,
		  			TaskAttemptId targetAttempt) {
    super(type);
    this.taskID = taskID;
    this.badPipe = badPipe;
    this.badHost = badHost;
    this.writeDiversity = isWriteDiversity;
    this.targetAttempt = targetAttempt;
  }
  
  //huanke construct a new TaskEvent with ignore Datanode
  public TaskEvent(TaskId taskID, TaskEventType type, DatanodeInfo ignoreNode) {
    super(type);
    this.taskID = taskID;
    this.ignoreNode=ignoreNode;
  }

  public DatanodeInfo getIgnoreNode(){
    return this.ignoreNode;
  }
  
  public TaskId getTaskID() {
    return taskID;
  }
  
  public String getSlowMapper(){
	  return slowMapper;
  }

  public TaskAttemptId getSlowMapperAttemptId() {
	  return slowMapperAttemptId;
  }

	public List<String> getBadPipe() {
		return badPipe;
	}
	
	public void setBadPipe(List<String> badPipe) {
		this.badPipe = badPipe;
	}

	public String getBadHost() {
		return badHost;
	}

	public void setBadHost(String badHost) {
		this.badHost = badHost;
	}

	public boolean isWriteDiversity() {
		return writeDiversity;
	}

	public TaskAttemptId getTargetAttempt() {
		return targetAttempt;
	}
	
}
