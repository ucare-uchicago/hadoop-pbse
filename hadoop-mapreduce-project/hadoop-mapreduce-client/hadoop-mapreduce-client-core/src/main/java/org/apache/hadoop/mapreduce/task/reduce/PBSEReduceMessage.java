package org.apache.hadoop.mapreduce.task.reduce;

import java.util.List;

public class PBSEReduceMessage {
	
	private static final String MESSAGE_TYPE_SPECULATION = "SPECULATE_TASK";
	
	private static final String PBSE_VERSION = "PBSE-Slow-Reduce-Write-1";
	private static final String PBSE_MSG = "PBSE_SLOW_REDUCE_WRITE";
	
	// @Cesar: Create a message to be logged for pbse statistic purposes
	public static String createPBSEMessageReduceTaskSpeculated(String reduceHost,
															   String attemptId,
															   List<String> pipeline){
		StringBuilder bld = new StringBuilder();
		bld.append(PBSE_VERSION).append(": ")
		.append("{")
		.append("\"type\":")
		.append("\"").append(MESSAGE_TYPE_SPECULATION).append("\"")
		.append(",")
		.append("\"host\":")
		.append("\"").append(reduceHost).append("\"")
		.append(",")
		.append("\"attempt\":")
		.append("\"").append(attemptId).append("\"")
		.append(",")
		.append("\"pipe\":[");
			for(String node : pipeline)
				bld.append("\"").append(node).append("\"").append(",");
		// @Cesar: Remove last comma
		bld.delete(bld.length() - 1, bld.length());
		bld.append("]")
		.append("}");
		return bld.toString();
			     
	}
}
