package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.text.DecimalFormat;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

//@Cesar: Holds shuffle rate information
public class ShuffleRateInfo implements Comparable<ShuffleRateInfo>{
	
	private TaskAttemptId mapTaskAttempId = null; 
	private TaskAttemptId reduceTaskAttempId = null; 
	private double fetchRate = 0.0;
	private String mapHost = null;
	private String reduceHost = null;
	private double transferProgress = 0.0;
	private String unit = null;
	private long totalBytes = 0L;
	private long shuffledBytes =0L;
	
	public TaskAttemptId getMapTaskAttempId() {
		return mapTaskAttempId;
	}
	
	public void setMapTaskAttempId(TaskAttemptId mapTaskAttempId) {
		this.mapTaskAttempId = mapTaskAttempId;
	}
	
	public TaskAttemptId getReduceTaskAttempId() {
		return reduceTaskAttempId;
	}
	
	public void setReduceTaskAttempId(TaskAttemptId reduceTaskAttempId) {
		this.reduceTaskAttempId = reduceTaskAttempId;
	}
	
	public double getFetchRate() {
		return fetchRate;
	}
	
	public void setFetchRate(double fetchRate) {
		this.fetchRate = fetchRate;
	}
	
	public String getMapHost() {
		return mapHost;
	}
	
	public void setMapHost(String mapHost) {
		this.mapHost = mapHost;
	}
	
	public String getReduceHost() {
		return reduceHost;
	}
	
	public void setReduceHost(String reduceHost) {
		this.reduceHost = reduceHost;
	}
	
	public double getTransferProgress() {
		return transferProgress;
	}
	
	public void setTransferProgress(double transferProgress) {
		this.transferProgress = transferProgress;
	}
	
	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}
	
	public long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public long getShuffledBytes() {
		return shuffledBytes;
	}

	public void setShuffledBytes(long shuffledBytes) {
		this.shuffledBytes = shuffledBytes;
	}

	@Override
	public int compareTo(ShuffleRateInfo other) {
		if(mapTaskAttempId != null){
			int cmp = mapTaskAttempId.compareTo(other.mapTaskAttempId);
			if(cmp == 0){
				if(reduceTaskAttempId != null)
					return reduceTaskAttempId.compareTo(other.reduceTaskAttempId);
				else
					return cmp;
			}
			else{
				return cmp;
			}
		}
		else{
			if(reduceTaskAttempId != null)
				return reduceTaskAttempId.compareTo(other.reduceTaskAttempId);
			else
				return 0;
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapTaskAttempId == null) ? 0 : mapTaskAttempId.hashCode());
		result = prime * result + ((reduceTaskAttempId == null) ? 0 : reduceTaskAttempId.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ShuffleRateInfo other = (ShuffleRateInfo) obj;
		if (mapTaskAttempId == null) {
			if (other.mapTaskAttempId != null)
				return false;
		} else if (!mapTaskAttempId.equals(other.mapTaskAttempId))
			return false;
		if (reduceTaskAttempId == null) {
			if (other.reduceTaskAttempId != null)
				return false;
		} else if (!reduceTaskAttempId.equals(other.reduceTaskAttempId))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("###.###");
		StringBuilder builder = new StringBuilder();
		builder.append("ShuffleRateInfo [mapTaskAttempId=").append(mapTaskAttempId).append(", reduceTaskAttempId=")
				.append(reduceTaskAttempId).append(", fetchRate=").append(df.format(fetchRate)).append(", unit=").append(unit)
				.append(", mapHost=").append(mapHost).append(", reduceHost=")
				.append(reduceHost).append(", transferProgress=").append(df.format(transferProgress * 100))
				.append(", totalBytes=").append(totalBytes).append(", shuffleBytes=").append(shuffledBytes).append("]");
		return builder.toString();
	}
	
	
	
	

}
