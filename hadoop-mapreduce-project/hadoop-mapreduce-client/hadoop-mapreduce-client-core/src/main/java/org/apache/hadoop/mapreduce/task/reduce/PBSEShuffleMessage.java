package org.apache.hadoop.mapreduce.task.reduce;

public class PBSEShuffleMessage {
	
	  private static final String MESSAGE_TYPE_SPECULATION = "RELAUNCH_TASK";
	  private static final String MESSAGE_TYPE_FETCHER_DATA = "FETCHER_INFO";
	  private static final String MESSAGE_TYPE_SHUFFLE_TIME = "SHUFFLE_TIME";
	  private static final String MESSAGE_TYPE_SORT_TIME = "SORT_TIME";
	  private static final String MESSAGE_TYPE_REDUCE_TIME = "REDUCE_TIME";
	  
	  private static final String PBSE_VERSION = "PBSE-Slow-Shuffle-1";
	  private static final String PBSE_MSG = "PBSE_SHUFFLE";
	  
	  
	  // @Cesar: Create a message to be logged for pbse statistic purposes
	  public static String createPBSEMessageMapTaskRelaunched(String mapperHost){
		  StringBuilder bld = new StringBuilder();
		  bld.append(PBSE_VERSION).append(": ")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_SPECULATION).append("\"")
		 .append(",")
		 .append("\"host\":")
		 .append("\"").append(mapperHost).append("\"")
		 .append("}");
		  return bld.toString();
		     
	  }
	  
	  // @Cesar: Create a message to be logged for pbse statistic purposes
	  public static String createPBSEMessageFetcherThreadAssigned(String mapperHost, int fetcherId){
		  StringBuilder bld = new StringBuilder();
		  bld.append(PBSE_MSG).append(":")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_FETCHER_DATA).append("\"")
		 .append(",")
		 .append("\"maphost\":")
		 .append("\"").append(mapperHost).append("\"")
		 .append(",")
		 .append("\"fetcherId\":")
		 .append(fetcherId)
		 .append("}");
		  return bld.toString();
		     
	  }

	  // @Cesar: Create a message to be logged for pbse statistic purposes
	  public static String createPBSEMessageShuffleFinished(long totalTime){
		  StringBuilder bld = new StringBuilder();
		  bld.append(PBSE_MSG).append(":")
		  .append("{")
		  .append("\"type\":")
		  .append("\"").append(MESSAGE_TYPE_SHUFFLE_TIME).append("\"")
		  .append(",")
		  .append("\"totalTime\":")
		  .append(totalTime)
		  .append("}");
		  return bld.toString();
		     
	  }
	
	// @Cesar: Create a message to be logged for pbse statistic purposes
	public static String createPBSEMessageSortFinished(){
		StringBuilder bld = new StringBuilder();
		bld.append(PBSE_MSG).append(":")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_SORT_TIME).append("\"")
		 .append("}");
		 return bld.toString();
		     
	 }
	  
	// @Cesar: Create a message to be logged for pbse statistic purposes
	public static String createPBSEMessageReduceFinished(){
		StringBuilder bld = new StringBuilder();
		bld.append(PBSE_MSG).append(":")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_REDUCE_TIME).append("\"")
		 .append("}");
		 return bld.toString();
		     
	  }
	
	  public static void main(String... args){
		  System.out.println(createPBSEMessageMapTaskRelaunched("xxx"));
		  System.out.println(createPBSEMessageFetcherThreadAssigned("xxx", 10));
		  System.out.println(createPBSEMessageShuffleFinished(1100000000L));
		  System.out.println(createPBSEMessageSortFinished());
	  }
	  
}
