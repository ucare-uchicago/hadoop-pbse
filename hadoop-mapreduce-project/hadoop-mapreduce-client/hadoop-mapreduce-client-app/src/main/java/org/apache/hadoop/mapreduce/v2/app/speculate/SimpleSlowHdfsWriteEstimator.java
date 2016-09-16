package org.apache.hadoop.mapreduce.v2.app.speculate;import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsWriteData;
import org.apache.hadoop.mapreduce.task.reduce.PipelineWriteRateReport;

public class SimpleSlowHdfsWriteEstimator {
	
	private static final Log LOG = LogFactory.getLog(
			SimpleSlowHdfsWriteEstimator.class);
	
	public boolean isSlow(HdfsWriteHost targetTaskAttemptAndHost,
						  PipelineWriteRateReport report,
						  double slowPipeThresshold,
						  long reduceWriteStartTime,
						  double reducePipelineMaximumDelay){
		// @Cesar: First, lets check the max delay here
		double totalSeconds = ((double)(System.nanoTime() - reduceWriteStartTime)) 
									/ 1000000000.0;
		if(reducePipelineMaximumDelay > 0.0 
			&& reduceWriteStartTime > 0
		    && totalSeconds >= reducePipelineMaximumDelay
		    && report.getPipeTransferRates().size() == 0){
			// @Cesar: Yes, i have delay
			LOG.info("@Cesar: Reporter " + targetTaskAttemptAndHost + " with pipeline " + 
					 report.getPipeOrderedNodes() + " has been delayed more than " +
					 reducePipelineMaximumDelay + " seconds so its going to be speculated");
			return true;
		}
		// @Cesar: The approach is going to be simple: If one
		// of the nodes in the pipeline is slower than the thresshold,
		// then this is slow!
		boolean oneComplies = false;
		LOG.info("@Cesar: Analyzing " + targetTaskAttemptAndHost);
		for(Entry<String, HdfsWriteData> pipeReport : report.getPipeTransferRates().entrySet()){
			double mBitsTransferRate = ((double)pipeReport.getValue().getBytesWritten() // in bytes
											* 8.0 / 1024.0 / 1024.0) /
										(pipeReport.getValue().getElapsedTime() > 0?
										((double)pipeReport.getValue().getElapsedTime() // in millisec
											/ 1000) : 1.0) ;
			if(mBitsTransferRate <= slowPipeThresshold){
				oneComplies = true;
				LOG.info("@Cesar: Detected slow transfer rate on pipenode " + 
						 pipeReport.getKey() + " : " + mBitsTransferRate);
			}
			else{
				LOG.info("@Cesar: Pipenode " + pipeReport.getKey() + 
						" does not comply: " + mBitsTransferRate);
			}
		}
		// @Cesar: ready, simple
		return oneComplies;
	}
	
}
