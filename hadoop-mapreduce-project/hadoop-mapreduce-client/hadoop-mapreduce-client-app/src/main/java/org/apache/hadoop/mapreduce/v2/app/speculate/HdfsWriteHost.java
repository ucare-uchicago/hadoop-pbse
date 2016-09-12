package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class HdfsWriteHost{
	
	private String reduceHost = null;
	private TaskAttemptId reduceTaskAttempt = null;
	
	public HdfsWriteHost(String reduceHost, TaskAttemptId reduceTaskAttempt) {
		this.reduceHost = reduceHost;
		this.reduceTaskAttempt = reduceTaskAttempt;
	}

	public String getReduceHost() {
		return reduceHost;
	}

	public void setReduceHost(String reduceHost) {
		this.reduceHost = reduceHost;
	}

	public TaskAttemptId getReduceTaskAttempt() {
		return reduceTaskAttempt;
	}

	public void setReduceTaskAttempt(TaskAttemptId reduceTaskAttempt) {
		this.reduceTaskAttempt = reduceTaskAttempt;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((reduceHost == null) ? 0 : reduceHost.hashCode());
		result = prime * result + ((reduceTaskAttempt == null) ? 0 : reduceTaskAttempt.hashCode());
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
		HdfsWriteHost other = (HdfsWriteHost) obj;
		if (reduceHost == null) {
			if (other.reduceHost != null)
				return false;
		} else if (!reduceHost.equals(other.reduceHost))
			return false;
		if (reduceTaskAttempt == null) {
			if (other.reduceTaskAttempt != null)
				return false;
		} else if (!reduceTaskAttempt.equals(other.reduceTaskAttempt))
			return false;
		return true;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("HdfsWriteHost [reduceHost=").append(reduceHost).append(", reduceTaskAttempt=")
				.append(reduceTaskAttempt).append("]");
		return builder.toString();
	}
	
}
