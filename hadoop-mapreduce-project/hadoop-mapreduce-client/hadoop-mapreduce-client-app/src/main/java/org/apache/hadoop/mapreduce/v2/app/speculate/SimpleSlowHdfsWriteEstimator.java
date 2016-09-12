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
						  double slowPipeThresshold){
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
