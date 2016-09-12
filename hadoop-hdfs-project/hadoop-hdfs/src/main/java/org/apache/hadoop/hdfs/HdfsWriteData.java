package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


// @Cesar: Utility class to store transfer rate related info
public class HdfsWriteData {
	
	private long bytesWritten = 0L;
	private long elapsedTime = 0L;
	
	public long getBytesWritten() {
		return bytesWritten;
	}
	
	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}
	
	public long getElapsedTime() {
		return elapsedTime;
	}
	
	public void setElapsedTime(long elapsedNanos) {
		this.elapsedTime = elapsedNanos;
	}
	
	public void writeTo(DataOutput out) throws IOException{
		// @Cesar: Write in this order
		out.writeLong(bytesWritten);
		out.writeLong(elapsedTime);
	}

	public void readFrom(DataInput in) throws IOException{
		bytesWritten = in.readLong();
		elapsedTime = in.readLong();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("HdfsWriteData [bytesWritten=").append(bytesWritten).append(", elapsedNanos=")
				.append(elapsedTime).append("]");
		return builder.toString();
	}

	
}
