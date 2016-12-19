package org.apache.hadoop.mapreduce.task.reduce;

public class UcareSeShuffleMessage {
	
	  private static final String MESSAGE_TYPE_SPECULATION = "RELAUNCH_TASK";
	  private static final String MESSAGE_TYPE_FETCHER_DATA = "FETCHER_INFO";
	  private static final String MESSAGE_TYPE_SHUFFLE_TIME = "SHUFFLE_TIME";
	  private static final String MESSAGE_TYPE_SORT_TIME = "SORT_TIME";
	  private static final String MESSAGE_TYPE_REDUCE_TIME = "REDUCE_TIME";
	  
	  private static final String UCARE_SE_VERSION = "UCARE_SE-Slow-Shuffle-1";
	  private static final String UCARE_SE_MSG = "UCARE_SE_SHUFFLE";
	  
	  
	  // @Cesar: Create a message to be logged for ucare_se statistic purposes
	  public static String createUcareSeMessageMapTaskRelaunched(String mapperHost){
		  StringBuilder bld = new StringBuilder();
		  bld.append(UCARE_SE_VERSION).append(": ")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_SPECULATION).append("\"")
		 .append(",")
		 .append("\"host\":")
		 .append("\"").append(mapperHost).append("\"")
		 .append("}");
		  return bld.toString();
		     
	  }
	  
	  // @Cesar: Create a message to be logged for ucare_se statistic purposes
	  public static String createUcareSeMessageFetcherThreadAssigned(String mapperHost, int fetcherId){
		  StringBuilder bld = new StringBuilder();
		  bld.append(UCARE_SE_MSG).append(":")
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

	  // @Cesar: Create a message to be logged for ucare_se statistic purposes
	  public static String createUcareSeMessageShuffleFinished(long totalTime){
		  StringBuilder bld = new StringBuilder();
		  bld.append(UCARE_SE_MSG).append(":")
		  .append("{")
		  .append("\"type\":")
		  .append("\"").append(MESSAGE_TYPE_SHUFFLE_TIME).append("\"")
		  .append(",")
		  .append("\"totalTime\":")
		  .append(totalTime)
		  .append("}");
		  return bld.toString();
		     
	  }
	
	// @Cesar: Create a message to be logged for ucare_se statistic purposes
	public static String createUcareSeMessageSortFinished(){
		StringBuilder bld = new StringBuilder();
		bld.append(UCARE_SE_MSG).append(":")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_SORT_TIME).append("\"")
		 .append("}");
		 return bld.toString();
		     
	 }
	  
	// @Cesar: Create a message to be logged for ucare_se statistic purposes
	public static String createUcareSeMessageReduceFinished(){
		StringBuilder bld = new StringBuilder();
		bld.append(UCARE_SE_MSG).append(":")
		 .append("{")
		 .append("\"type\":")
		 .append("\"").append(MESSAGE_TYPE_REDUCE_TIME).append("\"")
		 .append("}");
		 return bld.toString();
		     
	  }
	
	  public static void main(String... args){
		  System.out.println(createUcareSeMessageMapTaskRelaunched("xxx"));
		  System.out.println(createUcareSeMessageFetcherThreadAssigned("xxx", 10));
		  System.out.println(createUcareSeMessageShuffleFinished(1100000000L));
		  System.out.println(createUcareSeMessageSortFinished());
	  }
	  
}
