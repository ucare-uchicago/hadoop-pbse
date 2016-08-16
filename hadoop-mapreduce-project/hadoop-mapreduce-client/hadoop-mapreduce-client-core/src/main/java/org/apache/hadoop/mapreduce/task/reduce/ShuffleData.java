package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.mapred.TaskAttemptID;

// @Cesar: Hold shuffle info
public class ShuffleData implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public enum TransferRateUnit{
		MEGA_BIT_PER_SEC
	}
	
	public enum DataSource{
		IN_MEMORY,
		ON_DISK
	}
	
	private long bytes = 0L;
	private long nanos = 0L;
	private double transferRate = 0.0;
	private long totalBytes = 0L;
	private TransferRateUnit transferRateUnit = TransferRateUnit.MEGA_BIT_PER_SEC;
	private DataSource dataSource = DataSource.IN_MEMORY;
	private org.apache.hadoop.mapreduce.TaskAttemptID mapTaskId = new TaskAttemptID();
	
	public ShuffleData(){}
	
	public ShuffleData(long bytes, long nanos){
		this.bytes = bytes;
		this.nanos = nanos;
		transferRateUnit = TransferRateUnit.MEGA_BIT_PER_SEC;
		transferRate = 0.0;
		dataSource = DataSource.IN_MEMORY;
		totalBytes = 0L;
		mapTaskId = new TaskAttemptID();
		setTransferRate();
		
	}
	
	public void setTransferRate(){
		double seconds = nanos / 1000000000.0;
		double mbit = ((bytes * 8.0) / 1024.0) / 1024.0;
		this.transferRate = mbit / (seconds != 0? seconds : 1.0);
	}
	
	public double getTransferRate(){
		return transferRate;
	}
	
	public long getBytes() {
		return bytes;
	}
	
	public void setBytes(long bytes) {
		this.bytes = bytes;
	}
	
	public long getNanos() {
		return nanos;
	}
	
	public void setNanos(long nanos) {
		this.nanos = nanos;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}
	
	public TransferRateUnit getTransferRateUnit() {
		return transferRateUnit;
	}

	public void setTransferRateUnit(TransferRateUnit transferRateUnit) {
		this.transferRateUnit = transferRateUnit;
	}

	public org.apache.hadoop.mapreduce.TaskAttemptID getMapTaskId() {
		return mapTaskId;
	}

	public void setMapTaskId(org.apache.hadoop.mapreduce.TaskAttemptID mapTaskId) {
		this.mapTaskId = mapTaskId;
	}
	

	public void writeTo(DataOutput out) throws IOException{
		// @Cesar: Write in this order
		out.writeLong(bytes);
		out.writeLong(nanos);
		out.writeDouble(transferRate);
		out.writeInt(transferRateUnit.ordinal());
		out.writeInt(dataSource.ordinal());
		out.writeLong(totalBytes);
		mapTaskId.write(out);
	}
	
	public void readFrom(DataInput in) throws IOException{
		bytes = in.readLong();
		nanos = in.readLong();
		transferRate = in.readDouble();
		transferRateUnit = TransferRateUnit.values()[in.readInt()];
		dataSource = DataSource.values()[in.readInt()];
		totalBytes = in.readLong();
		mapTaskId = new TaskAttemptID();
		mapTaskId.readFields(in);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ShuffleData [bytes=").append(bytes).append(", nanos=").append(nanos).append(", transferRate=")
				.append(transferRate).append(", totalBytes=").append(totalBytes).append(", transferRateUnit=")
				.append(transferRateUnit).append(", dataSource=").append(dataSource).append(", mapTaskId=")
				.append(mapTaskId).append("]");
		return builder.toString();
	}

	

	
}
