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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.task.reduce.FetchRateReport;
import org.apache.hadoop.mapreduce.task.reduce.PBSEShuffleMessage;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleData;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;

public class DefaultSpeculator extends AbstractService implements
    Speculator {

  private static final long ON_SCHEDULE = Long.MIN_VALUE;
  private static final long ALREADY_SPECULATING = Long.MIN_VALUE + 1;
  private static final long TOO_NEW = Long.MIN_VALUE + 2;
  private static final long PROGRESS_IS_GOOD = Long.MIN_VALUE + 3;
  private static final long NOT_RUNNING = Long.MIN_VALUE + 4;
  private static final long TOO_LATE_TO_SPECULATE = Long.MIN_VALUE + 5;

  private long soonestRetryAfterNoSpeculate;
  private long soonestRetryAfterSpeculate;
  private double proportionRunningTasksSpeculatable;
  private double proportionTotalTasksSpeculatable;
  private int  minimumAllowedSpeculativeTasks;
  
  private static final Log LOG = LogFactory.getLog(DefaultSpeculator.class);

  private final ConcurrentMap<TaskId, Boolean> runningTasks
      = new ConcurrentHashMap<TaskId, Boolean>();

  // Used to track any TaskAttempts that aren't heart-beating for a while, so
  // that we can aggressively speculate instead of waiting for task-timeout.
  private final ConcurrentMap<TaskAttemptId, TaskAttemptHistoryStatistics>
      runningTaskAttemptStatistics = new ConcurrentHashMap<TaskAttemptId,
          TaskAttemptHistoryStatistics>();
  
  //huanke
  private Thread speculationIntersectionThread=null;
  private boolean PBSEenabled=false;
  
  // @Cesar: To hold the info about the reported fetch rates
  private ShuffleTable shuffleTable = new ShuffleTable();
  private boolean fetchRateSpeculationEnabled = false;
  private double fetchRateSpeculationSlowNodeThresshold = 0.0;
  private double fetchRateSpeculationSlowProgressThresshold = 0.0;
  // @Cesar: I will keep events in here
  private Map<ShuffleHost, List<ShuffleRateInfo>> fetchRateUpdateEvents = new HashMap<>();
  
  // Regular heartbeat from tasks is every 3 secs. So if we don't get a
  // heartbeat in 9 secs (3 heartbeats), we simulate a heartbeat with no change
  // in progress.
  private static final long MAX_WAITTING_TIME_FOR_HEARTBEAT = 9 * 1000;

  // These are the current needs, not the initial needs.  For each job, these
  //  record the number of attempts that exist and that are actively
  //  waiting for a container [as opposed to running or finished]
  private final ConcurrentMap<JobId, AtomicInteger> mapContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();
  private final ConcurrentMap<JobId, AtomicInteger> reduceContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();

  private final Set<TaskId> mayHaveSpeculated = new HashSet<TaskId>();
  
  private final Configuration conf;
  private AppContext context;
  private Thread speculationBackgroundThread = null;
  // @Cesar: This is my speculation background thread
  private Thread speculationFetchRateThread = null;
  private volatile boolean stopped = false;
  private BlockingQueue<SpeculatorEvent> eventQueue
      = new LinkedBlockingQueue<SpeculatorEvent>();
  private TaskRuntimeEstimator estimator;

  private BlockingQueue<Object> scanControl = new LinkedBlockingQueue<Object>();
  // @Cesar
  private BlockingQueue<Object> fetchRateScanControl = new LinkedBlockingQueue<Object>();
  // huanke
  private BlockingQueue<Object> scanControl1 = new LinkedBlockingQueue<Object>();
  
  private final Clock clock;

  private final EventHandler<TaskEvent> eventHandler;

  //huanke just launch one reduce back up task! using counter1
  private int counter1=0;
  private DatanodeInfo ignoreNode;
  private List<ArrayList<DatanodeInfo>> lists = new ArrayList<ArrayList<DatanodeInfo>>();
  private Map<TaskId, List<DatanodeInfo>> TaskAndPipeline=new ConcurrentHashMap<>();
  private Set<TaskId> TaskSet=new HashSet<>();
  
  // riza
  private int maxSpeculationDelay = 0;
  private boolean everDelaySpeculation = false;
  private boolean hasDelayThisIteration = false;

  public DefaultSpeculator(Configuration conf, AppContext context) {
    this(conf, context, context.getClock());
  }

  public DefaultSpeculator(Configuration conf, AppContext context, Clock clock) {
    this(conf, context, getEstimator(conf, context), clock);
  }
  
  static private TaskRuntimeEstimator getEstimator
      (Configuration conf, AppContext context) {
    TaskRuntimeEstimator estimator;
    
    try {
      // "yarn.mapreduce.job.task.runtime.estimator.class"
      Class<? extends TaskRuntimeEstimator> estimatorClass
          = conf.getClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
                          LegacyTaskRuntimeEstimator.class,
                          TaskRuntimeEstimator.class);

      Constructor<? extends TaskRuntimeEstimator> estimatorConstructor
          = estimatorClass.getConstructor();

      estimator = estimatorConstructor.newInstance();

      estimator.contextualize(conf, context);
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    }
    
  return estimator;
  }

  // This constructor is designed to be called by other constructors.
  //  However, it's public because we do use it in the test cases.
  // Normally we figure out our own estimator.
  public DefaultSpeculator
      (Configuration conf, AppContext context,
       TaskRuntimeEstimator estimator, Clock clock) {
    super(DefaultSpeculator.class.getName());

    this.conf = conf;
    this.context = context;
    this.estimator = estimator;
    this.clock = clock;
    this.eventHandler = context.getEventHandler();
    this.soonestRetryAfterNoSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_NO_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_NO_SPECULATE);
    this.soonestRetryAfterSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_SPECULATE);
    this.proportionRunningTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_RUNNING_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_RUNNING_TASKS);
    this.proportionTotalTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_TOTAL_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_TOTAL_TASKS);
    this.minimumAllowedSpeculativeTasks =
        conf.getInt(MRJobConfig.SPECULATIVE_MINIMUM_ALLOWED_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS);
    // @Cesar: read the properties
    this.fetchRateSpeculationEnabled = conf.getBoolean("mapreduce.experiment.enable_fetch_rate_speculation", false);
    this.fetchRateSpeculationSlowNodeThresshold = conf.getDouble("mapreduce.experiment.fetch_rate_speculation_slow_thresshold", Double.MAX_VALUE);
    this.fetchRateSpeculationSlowProgressThresshold = conf.getDouble("mapreduce.experiment.fetch_rate_speculation_progress_thresshold", Double.MAX_VALUE);
    // huanke
    this.PBSEenabled=conf.getBoolean("pbse.enable.for.reduce.pipeline", false);
    // riza
    this.maxSpeculationDelay = conf.getInt("mapreduce.policy.faread.maximum_speculation_delay", 0) / (int) this.soonestRetryAfterNoSpeculate;
    this.everDelaySpeculation = false;
  }
  
/*   *************************************************************    */

  // This is the task-mongering that creates the two new threads -- one for
  //  processing events from the event queue and one for periodically
  //  looking for speculation opportunities

  @Override
  protected void serviceStart() throws Exception {
	//huanke create my own thread to launch a backup reduce task
    LOG.info("@huanke PBSEenabled "+PBSEenabled);
    if (PBSEenabled){
      Runnable speculationIntersection
              = new Runnable() {
        @Override
        public void run() {
          while (!stopped && !Thread.currentThread().isInterrupted()) {
            long backgroundRunStartTime = clock.getTime();
            try {
              LOG.info("@huanke MyThread is running!" + TaskAndPipeline + "TaskSet000: " + TaskSet);
              DatanodeInfo ignoreNode = checkIntersection();
              if (ignoreNode == null) {
                LOG.info("@huanke checkIntersection returns null now");
                //just test to launch a backup reduce task
              } else {
                LOG.info("@huanke checkIntersection returns ignoreNode :" + ignoreNode);
                LOG.info("PBSE-Write-Diversity-1 taskId " + TaskAndPipeline.keySet() + " choose-datanode " + TaskAndPipeline+" IntersectedNode "+ignoreNode);
                relauchReduceTask(ignoreNode);
              }
              long mininumRecomp
                      = ignoreNode != null ? soonestRetryAfterSpeculate
                      : soonestRetryAfterNoSpeculate;
              long wait = Math.max(mininumRecomp,
                      clock.getTime() - backgroundRunStartTime);
              LOG.info("@huanke MyThread is waiting for Pipeline info" + wait);

              Object pollResult
                      = scanControl1.poll(wait, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
              if (!stopped) {
                LOG.error("Background thread returning, interrupted", e);
              }
              return;
            }
          }
        }
      };
      speculationIntersectionThread = new Thread(speculationIntersection, " DefaultSpeculator for intersection");
      speculationIntersectionThread.start();
    }
    Runnable speculationBackgroundCore
    = new Runnable() {
        @Override
        public void run() {
          while (!stopped && !Thread.currentThread().isInterrupted()) {
            hasDelayThisIteration = false;
            long backgroundRunStartTime = clock.getTime();
            try {
              int speculations = computeSpeculations();
              long mininumRecomp
                  = speculations > 0 ? soonestRetryAfterSpeculate
                                     : soonestRetryAfterNoSpeculate;
              long wait = Math.max(mininumRecomp,
                    clock.getTime() - backgroundRunStartTime);

              if (speculations > 0) {
                LOG.info("We launched " + speculations
                    + " speculations.  Sleeping " + wait + " milliseconds.");
              }
              Object pollResult
                  = scanControl.poll(wait, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              if (!stopped) {
                LOG.error("Background thread returning, interrupted", e);
              }
              return;
            }
          }
        }
      };
      speculationBackgroundThread = new Thread
    		  (speculationBackgroundCore, "DefaultSpeculator background processing");
      speculationBackgroundThread.start();
    if(fetchRateSpeculationEnabled){
    	// @Cesar: Create a new thread to speculate using the fetch rate
        Runnable speculationFetchRate = new Runnable(){
    		@Override
    		public void run() {
    			// @Cesar: In here, we check the table and speculate
    			// if necessary
    			while (!stopped && !Thread.currentThread().isInterrupted()){
    				try{
    					long backgroundRunStartTime = clock.getTime();
    					// @Cesar: Log some info
    					LOG.debug("@Cesar: Starting fetch rate speculation check");
    					int numSpeculations = checkFetchRateTable();
    	    			// @Cesar: So, after speculation we are going to
    					// sleep only for soonestRetryAfterNoSpeculate seconds, the same when
    					// we do not speculate. Why? Well, i have seen cases where we relaunch task x
    					// and then task x + 1 is slow, so we would have to wait to detect that slow shuffle
    	    			long mininumRecomp = soonestRetryAfterNoSpeculate;
    	                long wait = Math.max(mininumRecomp,
    	                        clock.getTime() - backgroundRunStartTime);
    	                if (numSpeculations > 0) {
    	                    LOG.debug("@Cesar: We launched " + numSpeculations
    	                        + " speculations.  Sleeping " + wait + " milliseconds.");
    	                }
    	                LOG.debug("@Cesar: Finished checking for speculations");
    	                // @Cesar: Sleep a while
    	                Object dummy = fetchRateScanControl.poll(wait, TimeUnit.MILLISECONDS);
    				}
    				catch(InterruptedException ie){
    					if(!stopped){
    						LOG.error("@Cesar: Background thread returning, interrupted", ie);
    					}
    					// @Cesar: Get out
    					return;
    				}
	    			
    			}
    		}
        };
    	speculationFetchRateThread = new Thread(speculationFetchRate, "@Cesar: Fetch rate "
    			+ " speculation background processing");
    	speculationFetchRateThread.start();
    }
    super.serviceStart();
  }

  
  //huanke
  private DatanodeInfo checkIntersection() {
    //huanke--------------------------------------------------------------
    TaskType type=TaskType.REDUCE;
    LOG.info("@huanke TaskSetSize :"+TaskSet+TaskAndPipeline);
    if(TaskSet.size()!=0) {
      LOG.info("@huanke TaskSet1 :" + TaskSet);
      //huanke TaskSet1 :[task_1471318623508_0001_r_000001]
      //huanke TaskSet1 :[task_1471318623508_0001_r_000001, task_1471318623508_0001_r_000000]
      Iterator iter = TaskSet.iterator();
      TaskId Ttmp = (TaskId) iter.next();
      Job job = context.getJob(Ttmp.getJobId());
      Map<TaskId, Task> tasks = job.getTasks(type);
      LOG.info("@huanke taskSize "+tasks.size()+tasks);
      for (Map.Entry<TaskId, Task> taskEntry : tasks.entrySet()) {
        if(TaskAndPipeline!=null && TaskAndPipeline.size()==tasks.size()) {
          List<DatanodeInfo> tmp = TaskAndPipeline.get(taskEntry.getKey());
          LOG.info("@huanke TaskAndPipeline for each task" + tmp);
//          huanke TaskAndPipeline for each task[10.1.1.4:50010, 10.1.1.7:50010]
//          huanke TaskAndPipeline for each task[10.1.1.6:50010, 10.1.1.4:50010]
          if (tmp != null) {
            lists.add((ArrayList<DatanodeInfo>) tmp);
          } else {
            LOG.info("@huanke TaskAndPipeline is not ready for task " + taskEntry.getKey());
          }
        }
        else{
          LOG.info("@huanke TaskAndPipeline is still empty");
        }
      }
    }else{
      LOG.info("@huanke TaskSet2 is empty now :" + TaskSet);
    }
    if(lists.isEmpty()){
      LOG.info("@huanke listsa is empty now");
      return null;
    }else{
      LOG.info("@huanke lists1: " + lists);
      //huanke lists1: [[10.1.1.4:50010, 10.1.1.7:50010], [10.1.1.6:50010, 10.1.1.4:50010]]
      List<DatanodeInfo> common = new ArrayList<DatanodeInfo>();
      LOG.info("@huanke common00: " + common);
//      huanke common00: []
      common.addAll(lists.get(0));
      LOG.info("@huanke common01: " + common);
//      huanke common01: [10.1.1.4:50010, 10.1.1.7:50010]
      for (ListIterator<ArrayList<DatanodeInfo>> iter = lists.listIterator(); iter.hasNext(); ) {
        ArrayList<DatanodeInfo> ttt=iter.next();
        LOG.info("@huanke iter.next(): " + ttt );
//        huanke iter.next(): [10.1.1.4:50010, 10.1.1.7:50010]
//        huanke iter.next(): [10.1.1.6:50010, 10.1.1.4:50010]
        common.retainAll(ttt);
        LOG.info("@huanke commonIter: " + common);
//        huanke commonIter: [10.1.1.4:50010, 10.1.1.7:50010]
//        huanke commonIter: [10.1.1.4:50010]
      }
      if (common == null) {
        LOG.info("@huanke common is null");
        return null;
      } else {
        LOG.info("@huanke common: " + common);
//        huanke common: [10.1.1.4:50010]
        return common.get(0);
      }
    }
  }

  //huanke
  private void relauchReduceTask(DatanodeInfo ignoreNode) {
    TaskId taskID=TaskAndPipeline.keySet().iterator().next();
    LOG.info("@huanke relauchRedueTask" + taskID);
    eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_ADD_SPEC_ATTEMPT, ignoreNode));
    mayHaveSpeculated.add(taskID);
  }
  
  @Override
  protected void serviceStop()throws Exception {
      stopped = true;
    // this could be called before background thread is established
    if (speculationBackgroundThread != null) {
      speculationBackgroundThread.interrupt();
    }
    // @Cesar: Also interrupt this one
    if(speculationFetchRateThread != null){
    	speculationFetchRateThread.interrupt();
    }
    // huanke
    if (speculationIntersectionThread != null) {
        speculationIntersectionThread.interrupt();
    }
    super.serviceStop();
  }
  
  // @Cesar: Check the table, return num speculations
  private int checkFetchRateTable(){
	  try{
		  // @Cesar: Start by pulling all reports
		  synchronized(fetchRateUpdateEvents){ 
			  for(Entry<ShuffleHost, List<ShuffleRateInfo>> events : fetchRateUpdateEvents.entrySet()){
				  for(ShuffleRateInfo rate : events.getValue()){
					  shuffleTable.reportRate(events.getKey(), rate);
				  }
			  }
			  fetchRateUpdateEvents.clear();
		  }
		  // @Cesar: This will be the return value
		  int numSpeculatedMapTasks = 0;
		  // @Cesar: This is the class that selects wich nodes
		  // are slow
		  HarmonicAverageSlowShuffleEstimator estimator = new HarmonicAverageSlowShuffleEstimator();
		  // @Cesar: In here, we have to check the shuffle rate for
		  // one mapper, and choose if we are going to speculate 
		  // or not
		  // @Cesar: So, iterate the fetch rate table
		  LOG.info("@Cesar: Starting fetch rate speculation check");
		  Map<ShuffleHost, Set<ShuffleRateInfo>> allReports = shuffleTable.getReports();
		  // @Cesar: Done released object, now go
		  // Lets iterate
		  if(allReports != null){
			  // @Cesar: Mark this hosts to be checked to delete if no entries
			  LOG.info("@Cesar: We have " + allReports.size() + " map hosts to check");
			  Iterator<Entry<ShuffleHost, Set<ShuffleRateInfo>>> fetchRateTableIterator = allReports.entrySet().iterator();
			  while(fetchRateTableIterator.hasNext()){
				  Entry<ShuffleHost, Set<ShuffleRateInfo>> nextEntry = fetchRateTableIterator.next();
				  // @Cesar: Do we have enough reports to speculate something??
				  if(!shuffleTable.canSpeculate(nextEntry.getKey().getMapHost())){
					  // @Cesar: Continue loop
					  LOG.info("@Cesar: No speculation possible for host " + nextEntry.getKey().getMapHost() + 
							  	" since it does not have enough reports");
					  continue;
				  }
				  // @Cesar: So, in this row we have one map host.
				  // This map host can have multiple map task associated, so if we detect
				  // that this node is slow, then we will relaunch all tasks in here
				  if(estimator.isSlow(nextEntry.getKey().getMapHost(), 
						  			  nextEntry.getValue(), 
						  			  fetchRateSpeculationSlowNodeThresshold, 
						  			  fetchRateSpeculationSlowProgressThresshold)){
					  // @Cesar: So, this node is slow. Get all its map tasks
					  Set<TaskAttemptId> allMapTasksInHost = shuffleTable.getAllSuccessfullMapTaskAttemptsFromHost(nextEntry.getKey().getMapHost());
					  LOG.info("@Cesar: " + allMapTasksInHost.size() + " map tasks will be speculated at " + nextEntry.getKey());
					  Iterator<TaskAttemptId> mapIterator = allMapTasksInHost.iterator();
					  while(mapIterator.hasNext()){
						  TaskAttemptId next = mapIterator.next(); 
						  if(!shuffleTable.wasSpeculated(next.getTaskId())){
							  // @Cesar: Only speculate if i havent done it already
							  LOG.info("@Cesar: Relaunching attempt " + next + " of task " + next.getTaskId() + 
									  	" at host " + nextEntry.getKey().getMapHost());
							  relaunchTask(next.getTaskId(), nextEntry.getKey().getMapHost(), next);
							  // @Cesar: also, add to the list of tasks that may have been speculated already
							  shuffleTable.bannMapTask(next.getTaskId());
							  // @Cesar: Mark this attempt as relaunched (killed)
							  shuffleTable.unsucceedTaskAtHost(nextEntry.getKey().getMapHost(), next.getTaskId());
							  shuffleTable.bannMapTaskAttempt(next);
							  // @Cesar: This is the real number of speculated map tasks
							  ++numSpeculatedMapTasks;
						  }
						  else{
							  LOG.info("@Cesar: Not going to relaunch " + next + " since task " + next.getTaskId() + " was speculated already");
						  }
						  // @Cesar: Clean host
						  shuffleTable.cleanHost(nextEntry.getKey().getMapHost());
					  }
				  }
				  else{
					  LOG.info("Estimator established that " + nextEntry.getKey().getMapHost() + " is not slow, so no speculations for this host");
				  }
			  }
		  }
		  LOG.info("@Cesar: Finished fetch rate speculation check");
		  return numSpeculatedMapTasks;
	  }
	  catch(Exception exc){
		  LOG.error("@Cesar: Catching dangerous exception: " + exc.getMessage() + " : " + exc.getCause());
		  return 0;
	  }
  }
  
  @Override
  public void handleAttempt(TaskAttemptStatus status) {
    long timestamp = clock.getTime();
    statusUpdate(status, timestamp);
  }

  // This section is not part of the Speculator interface; it's used only for
  //  testing
  public boolean eventQueueEmpty() {
    return eventQueue.isEmpty();
  }

  // This interface is intended to be used only for test cases.
  public void scanForSpeculations() {
    LOG.info("We got asked to run a debug speculation scan.");
    // debug
    System.out.println("We got asked to run a debug speculation scan.");
    System.out.println("There are " + scanControl.size()
        + " events stacked already.");
    scanControl.add(new Object());
    Thread.yield();
  }


/*   *************************************************************    */

  // This section contains the code that gets run for a SpeculatorEvent

  private AtomicInteger containerNeed(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    TaskType taskType = taskID.getTaskType();

    ConcurrentMap<JobId, AtomicInteger> relevantMap
        = taskType == TaskType.MAP ? mapContainerNeeds : reduceContainerNeeds;

    AtomicInteger result = relevantMap.get(jobID);

    if (result == null) {
      relevantMap.putIfAbsent(jobID, new AtomicInteger(0));
      result = relevantMap.get(jobID);
    }

    return result;
  }

  private synchronized void processSpeculatorEvent(SpeculatorEvent event) {
    switch (event.getType()) {
      case ATTEMPT_STATUS_UPDATE:
        statusUpdate(event.getReportedStatus(), event.getTimestamp());
        // @Cesar: Is this a success event? is this a map task? is fetch rate spec enabled?
        if(event.isSuccedded() && event.getReportedStatus().id.getTaskId().getTaskType() == TaskType.MAP && 
           fetchRateSpeculationEnabled){
        	// @Cesar: Report attempt as finished
        	shuffleTable.reportSuccessfullAttempt(event.getMapperHost(), event.getReportedStatus().id);
        	LOG.info("@Cesar: Reported successfull map at " + event.getMapperHost()  + " : " + event.getReportedStatus().id);
        }
        break;
      case TASK_CONTAINER_NEED_UPDATE:
      {
        AtomicInteger need = containerNeed(event.getTaskID());
        need.addAndGet(event.containersNeededChange());
        break;
      }

      case ATTEMPT_START:
      {
        LOG.info("ATTEMPT_START " + event.getTaskID());
        estimator.enrollAttempt
            (event.getReportedStatus(), event.getTimestamp());
        // @Cesar: Enroll also in here
        if(event.getTaskID().getTaskType() == TaskType.MAP && fetchRateSpeculationEnabled){
        	// @Cesar: An attempt started for a given map task
        	shuffleTable.reportStartedTask(event.getMapperHost(), event.getTaskID());
        	LOG.info("@Cesar: Map task reported at node " + event.getMapperHost() + " : " + event.getTaskID());
        }
        break;
      }
      
      case JOB_CREATE:
      {
        LOG.info("JOB_CREATE " + event.getJobID());
        estimator.contextualize(getConfig(), context);
        break;
      }
      // @Cesar: Handle fetch rate update
      case ATTEMPT_FETCH_RATE_UPDATE:
      {
    	FetchRateReport report = event.getReport();
    	String reduceHost = ShuffleTable.parseHost(event.getReducerNode());
    	if(reduceHost != null && fetchRateSpeculationEnabled){
    		// @Cesar: If we are here, report is not null
    		for(Entry<String, ShuffleData> reportEntry : report.getFetchRateReport().entrySet()){
    			// @Cesar: This is the report for one mapper
    			String mapHost = ShuffleTable.parseHost(reportEntry.getKey());
    			ShuffleRateInfo info = new ShuffleRateInfo();
    			info.setFetchRate(reportEntry.getValue().getTransferRate());
    			info.setMapHost(mapHost);
    			info.setMapTaskAttempId(TypeConverter.toYarn(reportEntry.getValue().getMapTaskId()));
    			info.setReduceHost(reduceHost);
    			info.setReduceTaskAttempId(event.getReduceTaskId());
    			info.setTransferProgress(reportEntry.getValue().getBytes() / (reportEntry.getValue().getTotalBytes() != 0?
    																		  reportEntry.getValue().getTotalBytes() : 1.0));
    			info.setTotalBytes(reportEntry.getValue().getTotalBytes());
    			info.setShuffledBytes(reportEntry.getValue().getBytes());
    			info.setUnit(reportEntry.getValue().getTransferRateUnit().toString());
    			// @Cesar: Lets save this report in queue
    			synchronized(fetchRateUpdateEvents){
    				if(fetchRateUpdateEvents.containsKey(new ShuffleHost(mapHost))){
    					fetchRateUpdateEvents.get(new ShuffleHost(mapHost)).add(info);
    				}
    				else{
    					List<ShuffleRateInfo> rates = new ArrayList<>();
    					rates.add(info);
    					fetchRateUpdateEvents.put(new ShuffleHost(mapHost), rates);
    				}
    				
    			}
    			// @Cesar: Done
    			LOG.info("@Cesar: Stored report with " + info);
    		}
    	}
    	else{
    		// @Cesar: Small error, just log it. It should never happen
    		LOG.error("@Cesar: Trying to update fetch rate report with null reducer. Data is " + 
    				  report);
    	}
        // @Cesar: Log that this event happened
    	LOG.info("ATTEMPT_FETCH_RATE_UPDATE " + event.getReduceTaskId());
    	break;
      }
      case ATTEMPT_PIPELINE_UPDATE:
      {
          LOG.info("@huanke ATTEMPT_PIPELINE_UPDATE");
          PipelineUpdate(event.getReportedStatus(), event.getDNpath());

          break;
      }
    }
  }

  private void PipelineUpdate(TaskAttemptStatus reportedStatus, ArrayList<DatanodeInfo> dNpath) {
	    String stateString = reportedStatus.taskState.toString();

	    LOG.info("@huanke at the beginning"+TaskAndPipeline+" size: "+TaskAndPipeline.size()+dNpath+reportedStatus.Pipeline);

	    TaskAttemptId attemptID = reportedStatus.id;
	    TaskId taskID = attemptID.getTaskId();
	    Job job = context.getJob(taskID.getJobId());

	    synchronized (TaskAndPipeline) {
	      if (taskID.getTaskType() == TaskType.REDUCE) {
	        if (dNpath.size() == 0) {
	          LOG.info("@huanke TaskAndPipeline is empty at this moment");
	        }
	        else {
	          if (TaskSet.add(taskID)) {
	            TaskAndPipeline.put(taskID, dNpath);
	          }
	        }
	      }
	    }
	    LOG.info("@huanke DefaultSpeculator TaskAndPipeline"+TaskAndPipeline+TaskAndPipeline.size());
	    //DefaultSpeculator TaskAndPipeline{task_1471222182002_0001_r_000000=[10.1.1.2:50010, 10.1.1.7:50010], task_1471222182002_0001_r_000001=[10.1.1.7:50010, 10.1.1.4:50010]}2


	    if (job == null) {
	      return;
	    }

	    Task task = job.getTask(taskID);

	    if (task == null) {
	      return;
	    }
	//
//	    estimator.updateAttempt(reportedStatus, timestamp);

	    if (stateString.equals(TaskAttemptState.RUNNING.name())) {
	      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
	    } else {
	      runningTasks.remove(taskID, Boolean.TRUE);
	      if (!stateString.equals(TaskAttemptState.STARTING.name())) {
	        runningTaskAttemptStatistics.remove(attemptID);
	      }
	    }
	  }
  
  /**
   * Absorbs one TaskAttemptStatus
   *
   * @param reportedStatus the status report that we got from a task attempt
   *        that we want to fold into the speculation data for this job
   * @param timestamp the time this status corresponds to.  This matters
   *        because statuses contain progress.
   */
  protected void statusUpdate(TaskAttemptStatus reportedStatus, long timestamp) {

    String stateString = reportedStatus.taskState.toString();

    TaskAttemptId attemptID = reportedStatus.id;
    TaskId taskID = attemptID.getTaskId();
    Job job = context.getJob(taskID.getJobId());

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }
    
    estimator.updateAttempt(reportedStatus, timestamp);

    if (stateString.equals(TaskAttemptState.RUNNING.name())) {
      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
    } else {
      runningTasks.remove(taskID, Boolean.TRUE);
      if (!stateString.equals(TaskAttemptState.STARTING.name())) {
        runningTaskAttemptStatistics.remove(attemptID);
      }
    }
  }
  
   

  /* ************************************************************* */

  // This is the code section that runs periodically and adds speculations for
  // those jobs that need them.

  // This can return a few magic values for tasks that shouldn't speculate:
  // returns ON_SCHEDULE if thresholdRuntime(taskID) says that we should not
  // considering speculating this task
  // returns ALREADY_SPECULATING if that is true. This has priority.
  // returns TOO_NEW if our companion task hasn't gotten any information
  // returns PROGRESS_IS_GOOD if the task is sailing through
  // returns NOT_RUNNING if the task is not running
  //
  // All of these values are negative. Any value that should be allowed to
  // speculate is 0 or positive.
  private long speculationValue(TaskId taskID, long now) {
    Job job = context.getJob(taskID.getJobId());
    Task task = job.getTask(taskID);
    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
    long acceptableRuntime = Long.MIN_VALUE;
    long result = Long.MIN_VALUE;
    
    if (!mayHaveSpeculated.contains(taskID)) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
    	  return ON_SCHEDULE;
      }
    }

   // @Cesar: Ignore tasks relaunched by slow shuffle
   synchronized(shuffleTable){
	   if(shuffleTable.wasSpeculated(taskID)){
		   // @Cesar: Return in here, this wont be speculated again
		   LOG.info("@Cesar: Task " + taskID + " wont be speculated again.");
		   return ON_SCHEDULE;
	   }
   }
    
    TaskAttemptId runningTaskAttemptID = null;

    int numberRunningAttempts = 0;
    
    for (TaskAttempt taskAttempt : attempts.values()) {
    	
      if (taskAttempt.getState() == TaskAttemptState.RUNNING
          || taskAttempt.getState() == TaskAttemptState.STARTING) {
        if (++numberRunningAttempts > 1) {
          return ALREADY_SPECULATING;
        }
        runningTaskAttemptID = taskAttempt.getID();

        long estimatedRunTime = estimator.estimatedRuntime(runningTaskAttemptID);

        long taskAttemptStartTime
            = estimator.attemptEnrolledTime(runningTaskAttemptID);
        if (taskAttemptStartTime > now) {
          // This background process ran before we could process the task
          //  attempt status change that chronicles the attempt start
          return TOO_NEW;
        }

        // riza: do not speculate task that has not sent its first status update
        MapTaskAttemptImpl mapTaskAttempt = (taskAttempt instanceof MapTaskAttemptImpl) ?
            (MapTaskAttemptImpl) taskAttempt : null;
        if (mapTaskAttempt != null) {
          if (maxSpeculationDelay > 0 && DatanodeID.nullDatanodeID.equals(mapTaskAttempt.getLastDatanodeID())) {
            if (!everDelaySpeculation)
              LOG.info("PBSE-Read-2: " + runningTaskAttemptID + " speculation delayed");
            everDelaySpeculation = true;

            if (!hasDelayThisIteration) {
              maxSpeculationDelay--;
              hasDelayThisIteration = true;
            }

            LOG.info(runningTaskAttemptID + " has not report its datanode, speculator return TOO_NEW, "
              + maxSpeculationDelay + " speculation delay left");
            return TOO_NEW;
          }
        }

        long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;

        long estimatedReplacementEndTime
            = now + estimator.estimatedNewAttemptRuntime(taskID);

        float progress = taskAttempt.getProgress();
        TaskAttemptHistoryStatistics data =
            runningTaskAttemptStatistics.get(runningTaskAttemptID);
        if (data == null) {
          runningTaskAttemptStatistics.put(runningTaskAttemptID,
            new TaskAttemptHistoryStatistics(estimatedRunTime, progress, now));
        } else {
          if (estimatedRunTime == data.getEstimatedRunTime()
              && progress == data.getProgress()) {
            // Previous stats are same as same stats
            if (data.notHeartbeatedInAWhile(now)) {
              // Stats have stagnated for a while, simulate heart-beat.
              TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
              taskAttemptStatus.id = runningTaskAttemptID;
              taskAttemptStatus.progress = progress;
              taskAttemptStatus.taskState = taskAttempt.getState();
              // Now simulate the heart-beat
              handleAttempt(taskAttemptStatus);
            }
          } else {
            // Stats have changed - update our data structure
            data.setEstimatedRunTime(estimatedRunTime);
            data.setProgress(progress);
            data.resetHeartBeatTime(now);
          }
        }

        if (estimatedEndTime < now) {
          return PROGRESS_IS_GOOD;
        }

        if (estimatedReplacementEndTime >= estimatedEndTime) {
          return TOO_LATE_TO_SPECULATE;
        }
        LOG.info("@huanke  ---estimatedEndTime: "+estimatedEndTime +"estimatedReplacementEndTime: "+estimatedReplacementEndTime+" progress: "+progress+" now: "+now);
        //huanke  ---estimatedEndTime: 1470079227747 estimatedReplacementEndTime: 1469999793485 progress: 0.0 now: 1469999785793
        result = estimatedEndTime - estimatedReplacementEndTime;
      
      }
    }
    
    // If we are here, there's at most one task attempt.
    if (numberRunningAttempts == 0) {
      return NOT_RUNNING;
    }

    
    if (acceptableRuntime == Long.MIN_VALUE) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    
    return result;
  }

  //huanke reduce task does not launch backup task as map task like T_ADD_SPEC_ATTEMPT
  protected void relaunchTask(TaskId taskID) {
    LOG.info
            ("DefaultSpeculator.@huanke-relaunchTask -- we are speculating a reduce task of id " + taskID);
    eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_ATTEMPT_KILLED));
    // @huanke: Add this as speculated
    mayHaveSpeculated.add(taskID);
  }
  
  //Add attempt to a given Task.
  protected void addSpeculativeAttempt(TaskId taskID) {
	// @Cesar: Do not re espculate if this have been launched by slow shuffle
	  LOG.info
      ("DefaultSpeculator.addSpeculativeAttempt -- we are speculating " + taskID);
	  eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_ADD_SPEC_ATTEMPT));
	  mayHaveSpeculated.add(taskID);
   
  }

  // @Cesar: I will use this to send my map speculative attempt for now
  // It can change if at the end of the day i discover that is the same
  // using addSpeculativeAttempt
  protected void relaunchTask(TaskId taskID, String mapperHost, TaskAttemptId mapId) {
    LOG.info
        ("DefaultSpeculator.relaunchTask.@cesar -- we are speculating a map task of id " + taskID);
    eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_ATTEMPT_KILLED, mapperHost, mapId));
    // @Cesar: Log
    LOG.info(PBSEShuffleMessage.createPBSESlowShuffleLogMessage(mapperHost));
    // @Cesar: Add this as speculated
	mayHaveSpeculated.add(taskID);
	
  }
  
  @Override
  public void handle(SpeculatorEvent event) {
    processSpeculatorEvent(event);
  }


  private int maybeScheduleAMapSpeculation() {
    return maybeScheduleASpeculation(TaskType.MAP);
  }

  private int maybeScheduleAReduceSpeculation() {
    return maybeScheduleASpeculation(TaskType.REDUCE);
  }

  private int maybeScheduleASpeculation(TaskType type) {
	int successes = 0;
    long now = clock.getTime();
    ConcurrentMap<JobId, AtomicInteger> containerNeeds
        = type == TaskType.MAP ? mapContainerNeeds : reduceContainerNeeds;

    for (ConcurrentMap.Entry<JobId, AtomicInteger> jobEntry : containerNeeds.entrySet()) {
      // This race conditon is okay.  If we skip a speculation attempt we
      //  should have tried because the event that lowers the number of
      //  containers needed to zero hasn't come through, it will next time.
      // Also, if we miss the fact that the number of containers needed was
      //  zero but increased due to a failure it's not too bad to launch one
      //  container prematurely.
      if (jobEntry.getValue().get() > 0) {
        continue;
      }

      int numberSpeculationsAlready = 0;
      int numberRunningTasks = 0;

      // loop through the tasks of the kind
      Job job = context.getJob(jobEntry.getKey());

      Map<TaskId, Task> tasks = job.getTasks(type);

      int numberAllowedSpeculativeTasks
          = (int) Math.max(minimumAllowedSpeculativeTasks,
              proportionTotalTasksSpeculatable * tasks.size());
     
      TaskId bestTaskID = null;
      long bestSpeculationValue = -1L;

      // this loop is potentially pricey.
      // TODO track the tasks that are potentially worth looking at
      for (Map.Entry<TaskId, Task> taskEntry : tasks.entrySet()) {
        long mySpeculationValue = speculationValue(taskEntry.getKey(), now);

        if (mySpeculationValue == ALREADY_SPECULATING) {
          ++numberSpeculationsAlready;
        }

        LOG.info("@huanke TaskId"+taskEntry.getKey()+taskEntry.getValue().getType()+" taskProgress: "+taskEntry.getValue().getProgress());
        
        if (mySpeculationValue != NOT_RUNNING) {
          ++numberRunningTasks;
        }

        LOG.info("@huanke mySpeculationValue: "+mySpeculationValue+" bestSpeculationValue: "+bestSpeculationValue + " taskEntry.getKey(): "+ taskEntry.getKey());
        //huanke mySpeculationValue: -9223372036854775808 bestSpeculationValue: -1 taskEntry.getKey(): task_1470000526915_0001_m_000000
        
        if (mySpeculationValue > bestSpeculationValue) {
          bestTaskID = taskEntry.getKey();
          bestSpeculationValue = mySpeculationValue;
        }
      }
      numberAllowedSpeculativeTasks
          = (int) Math.max(numberAllowedSpeculativeTasks,
              proportionRunningTasksSpeculatable * numberRunningTasks);

      // If we found a speculation target, fire it off
      if (bestTaskID != null
          && numberAllowedSpeculativeTasks > numberSpeculationsAlready) {
    	LOG.info("@huanke addSpeculativeAttempt: h_r ?"+bestTaskID.getTaskType()); 
        addSpeculativeAttempt(bestTaskID);
        ++successes;
      }
    }
    
    return successes;
  }

  private int computeSpeculations() {
    // We'll try to issue one map and one reduce speculation per job per run
    return maybeScheduleAMapSpeculation() + maybeScheduleAReduceSpeculation();
  }

  static class TaskAttemptHistoryStatistics {

    private long estimatedRunTime;
    private float progress;
    private long lastHeartBeatTime;

    public TaskAttemptHistoryStatistics(long estimatedRunTime, float progress,
        long nonProgressStartTime) {
      this.estimatedRunTime = estimatedRunTime;
      this.progress = progress;
      resetHeartBeatTime(nonProgressStartTime);
    }

    public long getEstimatedRunTime() {
      return this.estimatedRunTime;
    }

    public float getProgress() {
      return this.progress;
    }

    public void setEstimatedRunTime(long estimatedRunTime) {
      this.estimatedRunTime = estimatedRunTime;
    }

    public void setProgress(float progress) {
      this.progress = progress;
    }

    public boolean notHeartbeatedInAWhile(long now) {
      if (now - lastHeartBeatTime <= MAX_WAITTING_TIME_FOR_HEARTBEAT) {
        return false;
      } else {
        resetHeartBeatTime(now);
        return true;
      }
    }

    public void resetHeartBeatTime(long lastHeartBeatTime) {
      this.lastHeartBeatTime = lastHeartBeatTime;
    }
  }

  @VisibleForTesting
  public long getSoonestRetryAfterNoSpeculate() {
    return soonestRetryAfterNoSpeculate;
  }

  @VisibleForTesting
  public long getSoonestRetryAfterSpeculate() {
    return soonestRetryAfterSpeculate;
  }

  @VisibleForTesting
  public double getProportionRunningTasksSpeculatable() {
    return proportionRunningTasksSpeculatable;
  }

  @VisibleForTesting
  public double getProportionTotalTasksSpeculatable() {
    return proportionTotalTasksSpeculatable;
  }

  @VisibleForTesting
  public int getMinimumAllowedSpeculativeTasks() {
    return minimumAllowedSpeculativeTasks;
  }
}
