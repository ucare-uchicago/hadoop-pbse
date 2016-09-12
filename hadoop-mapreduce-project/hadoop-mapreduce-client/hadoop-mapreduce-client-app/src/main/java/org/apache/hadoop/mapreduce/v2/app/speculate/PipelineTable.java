package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.task.reduce.PipelineWriteRateReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;


public class PipelineTable {

	// @Cesar: Hard code this for now
	public static final long MIN_REPORT_COUNT = 2; 
	
	private static final Log LOG = LogFactory.getLog(PipelineTable.class);
	
	// @Cesar: We will keep the reports in here, this
	// is the central entity in this case
	private Map<HdfsWriteHost, PipelineWriteRateReport> reports = 
			new ConcurrentHashMap<>();
	// @Cesar: Reduce tasks that finished
	private Set<TaskId> finishedReduceTasks = 
			Collections.newSetFromMap(new ConcurrentHashMap<TaskId, Boolean>());
	// @Cesar: NOt going to receive more reports from this ones
	private Set<HdfsWriteHost> bannedReports = 
			Collections.newSetFromMap(new ConcurrentHashMap<HdfsWriteHost, Boolean>());
	
	// @Cesar: Count the reports for each reportes
	private Map<HdfsWriteHost, Long> reportCount = 
			new ConcurrentHashMap<>();
	
	private Set<TaskId> alreadySpeculated = 
			Collections.newSetFromMap(new ConcurrentHashMap<TaskId, Boolean>());
	
	public void markAsSpeculated(TaskId task){
		alreadySpeculated.add(task);
	}
	
	public boolean wasSpeculated(TaskId task){
		return alreadySpeculated.contains(task);
	}
	
	public void incrementReportCount(HdfsWriteHost reporter){
		if(reportCount.containsKey(reporter)){
			reportCount.put(reporter, 
					reportCount.get(reporter).longValue() + 1L);
		}
		else{
			reportCount.put(reporter, 1L);
		}
	}
	
	public Map<HdfsWriteHost, PipelineWriteRateReport> getReports() {
		return reports;
	}
	
	public void bannReporter(HdfsWriteHost reporter){
		bannedReports.add(reporter);
	}
	
	public boolean isFinished(TaskId task){
		return finishedReduceTasks.contains(task);
	}
	
	public void cleanReports(List<HdfsWriteHost> toBeCleaned){
		for(HdfsWriteHost toDelete : toBeCleaned){
			reports.remove(toDelete);
		}
	}
	
	public boolean storePipelineRateReport(HdfsWriteHost writeHost,
										   PipelineWriteRateReport report){
		
		// @Cesar: In case of any delayed report, i just ignore it
		if(finishedReduceTasks.contains(writeHost.getReduceTaskAttempt().getTaskId())) return false;
		// @Cesar: I do not receive more from the ones alreasy slow
		if(bannedReports.contains(writeHost)) return false;
		// @Cesar: Store
		reports.put(writeHost, report);
		incrementReportCount(writeHost);
		// @Cesar: Some logging
		LOG.info("@Cesar: Stored pipe report: " + writeHost + " ---> " + report);
		return true;
	}

	public void storeFromMap(Map<HdfsWriteHost, PipelineWriteRateReport> reports){
		for(Entry<HdfsWriteHost, PipelineWriteRateReport> entry : reports.entrySet()){
			storePipelineRateReport(entry.getKey(), entry.getValue());
		}
	}
	
	public void markAsFinished(TaskId finishedReduceTask){
		finishedReduceTasks.add(finishedReduceTask);
	}
	
	public boolean canSpeculate(HdfsWriteHost writeHost, long minNumberOfReports){
		return reportCount.get(writeHost).longValue() >= minNumberOfReports;
	}
	
}
