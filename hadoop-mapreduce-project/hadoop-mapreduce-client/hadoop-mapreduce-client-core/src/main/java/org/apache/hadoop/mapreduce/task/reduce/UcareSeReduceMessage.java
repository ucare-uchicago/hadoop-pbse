package org.apache.hadoop.mapreduce.task.reduce;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class UcareSeReduceMessage {
	
	private static final String MESSAGE_TYPE_SPECULATION = "SPECULATE_TASK";
	
	private static final String UCARE_SE_VERSION_WRITE = "UCARE_SE-Slow-Reduce-Write-1";
	private static final String UCARE_SE_VERSION_DIVERSITY = "UCARE_SE-Write-Diversity-1";
	private static final String UCARE_SE_VERSION_SINGLE_REDUCE = "UCARE_SE-Single-Reduce-1";
	private static final String UCARE_SE_MSG = "UCARE_SE_SLOW_REDUCE_WRITE";
	
	// @Cesar: Create a message to be logged for ucare_se statistic purposes
	public static String createUcareSeMessageReduceTaskSpeculated(String reduceHost,
															   String attemptId,
															   List<String> pipeline){
		StringBuilder bld = new StringBuilder();
		bld.append(UCARE_SE_VERSION_WRITE).append(": ")
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
		if(pipeline.size() > 0)
			bld.delete(bld.length() - 1, bld.length());
		bld.append("]")
		.append("}");
		return bld.toString();
			     
	}
	
	// @Cesar: Create a message to be logged for ucare_se statistic purposes
	public static String createUcareSeMessageReduceTaskSpeculatedDueToSingleReduce
						(String reduceHost,
						 String attemptId,
						 List<String> pipeline){
		StringBuilder bld = new StringBuilder();
		bld.append(UCARE_SE_VERSION_SINGLE_REDUCE).append(": ")
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
		if(pipeline.size() > 0)
			bld.delete(bld.length() - 1, bld.length());
		bld.append("]")
		.append("}");
		return bld.toString();
			     
	}
	
	// @Cesar: Create a message to be logged for ucare_se statistic purposes
	public static String createUcareSeMessageReduceTaskSpeculatedDueToWriteDiversity
						(String reduceHost,
						 String attemptId,
						 List<String> pipeline){
		StringBuilder bld = new StringBuilder();
		bld.append(UCARE_SE_VERSION_DIVERSITY).append(": ")
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
		if(pipeline.size() > 0)
			bld.delete(bld.length() - 1, bld.length());
		bld.append("]")
		.append("}");
		return bld.toString();
			     
	}	
		
		public static void main(String... args){
			System.out.println(createUcareSeMessageReduceTaskSpeculatedDueToWriteDiversity
								("a", "b", (List)Lists.newArrayList()));
		}
}
