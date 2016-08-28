package org.apache.hadoop.mapreduce.task.reduce;

public class PBSEShuffleMessage {
	
	  private static final String MESSAGE_TYPE_SPECULATION = "RELAUNCH_TASK";
	  private static final String MESSAGE_TYPE_FETCHER_DATA = "FETCHER_INFO";
	  private static final String MESSAGE_TYPE_SHUFFLE_TIME = "SHUFFLE_TIME";
	  
	  private static final String PBSE_VERSION = "PBSE-Slow-Shuffle-1";
	  private static final String PBSE_MSG = "PBSE_SHUFFLE";
	  
	  
	  // @Cesar: Create a message to be logged for pbse statistic purposes
	  public static String createPBSESlowShuffleLogMessage(String mapperHost){
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
	  public static String createPBSESlowShuffleLogMessage(String mapperHost, int fetcherId){
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
	  public static String createPBSESlowShuffleLogMessage(long totalTime){
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
	  
	  public static void main(String... args){
		  System.out.println(createPBSESlowShuffleLogMessage("xxx"));
		  System.out.println(createPBSESlowShuffleLogMessage("xxx", 10));
		  System.out.println(createPBSESlowShuffleLogMessage(1100000000L));
	  }
	  
}
