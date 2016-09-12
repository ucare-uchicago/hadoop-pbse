package org.apache.hadoop.mapreduce.task.reduce;

import java.util.List;

public class PBSEReduceMessage {
	
	private static final String MESSAGE_TYPE_SPECULATION = "SPECULATE_TASK";
	
	private static final String PBSE_VERSION_WRITE = "PBSE-Slow-Reduce-Write-1";
	private static final String PBSE_VERSION_DIVERSITY = "PBSE-Write-Diversity-1";
	private static final String PBSE_MSG = "PBSE_SLOW_REDUCE_WRITE";
	
	// @Cesar: Create a message to be logged for pbse statistic purposes
	public static String createPBSEMessageReduceTaskSpeculated(String reduceHost,
															   String attemptId,
															   List<String> pipeline){
		StringBuilder bld = new StringBuilder();
		bld.append(PBSE_VERSION_WRITE).append(": ")
		.append("{")
		.append("\"type\":")
		.append("\"").append(MESSAGE_TYPE_SPECULATION).append("\"")
		.append(",")
		.append("\"slowHost\":")
		.append("\"").append(reduceHost).append("\"")
		.append(",")
		.append("\"attempt\":")
		.append("\"").append(attemptId).append("\"")
		.append(",")
		.append("\"slowPipe\":[");
			for(String node : pipeline)
				bld.append("\"").append(node).append("\"").append(",");
		// @Cesar: Remove last comma
		bld.delete(bld.length() - 1, bld.length());
		bld.append("]")
		.append("}");
		return bld.toString();
			     
	}
	
	// @Cesar: Create a message to be logged for pbse statistic purposes
		public static String createPBSEMessageReduceTaskSpeculatedDueToWriteDiversity
							(String reduceHost,
							 String attemptId,
							 List<String> pipeline){
			StringBuilder bld = new StringBuilder();
			bld.append(PBSE_VERSION_DIVERSITY).append(": ")
			.append("{")
			.append("\"type\":")
			.append("\"").append(MESSAGE_TYPE_SPECULATION).append("\"")
			.append(",")
			.append("\"ignoreHost\":")
			.append("\"").append(reduceHost).append("\"")
			.append(",")
			.append("\"attempt\":")
			.append("\"").append(attemptId).append("\"")
			.append(",")
			.append("\"ignorePipe\":[");
				for(String node : pipeline)
					bld.append("\"").append(node).append("\"").append(",");
			// @Cesar: Remove last comma
			bld.delete(bld.length() - 1, bld.length());
			bld.append("]")
			.append("}");
			return bld.toString();
				     
		}	
}
