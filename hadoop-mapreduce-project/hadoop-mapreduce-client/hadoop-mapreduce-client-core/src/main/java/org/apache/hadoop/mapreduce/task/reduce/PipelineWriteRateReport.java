package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.HdfsWriteData;

public class PipelineWriteRateReport {
	
	private Map<String, HdfsWriteData> pipeTransferRates 
		= new HashMap<>();
	
	private Map<Integer, String> orderedPipeNodes 
		= new HashMap<>();
	
	public void addPipeOrderedNode(int order, String host){
		orderedPipeNodes.put(order, host);
	}
	
	public void setPipeOrderedNodes(Map<Integer, String> newOnes){
		// @Cesar: Yes, this can be null
		if(newOnes != null) orderedPipeNodes.putAll(newOnes);
	}
	
	public void addPipeTransferRateReport(String host, HdfsWriteData data){
		pipeTransferRates.put(host, data);
	}
	
	public void setPipeTransferRates(Map<String, HdfsWriteData> newOnes){
		// @Cesar: Yes, this can be null
		if(newOnes != null) pipeTransferRates.putAll(newOnes);
	}
	
	public Map<String, HdfsWriteData> getPipeTransferRates(){
		return pipeTransferRates;
	}
	
	public Map<Integer, String> getPipeOrderedNodes(){
		return orderedPipeNodes;
	}
	
	public static PipelineWriteRateReport readFrom(DataInput in) throws IOException{
		PipelineWriteRateReport report = new PipelineWriteRateReport();
		int pipelineSize = in.readInt();
		for(int i = 0; i < pipelineSize; ++i){
			String host = in.readUTF();
			HdfsWriteData dt = new HdfsWriteData();
			dt.readFrom(in);
			report.addPipeTransferRateReport(host, dt);
		}
		int orderedPipeNodesSize = in.readInt();
		for(int i = 0; i < orderedPipeNodesSize; ++i){
			int order = in.readInt();
			String host = in.readUTF();
			report.addPipeOrderedNode(order, host);
		}
		return report;
	}
	
	public void writeTo(DataOutput out) throws IOException{
		out.writeInt(pipeTransferRates.size());
		for(Entry<String, HdfsWriteData> entries : pipeTransferRates.entrySet()){
			out.writeUTF(entries.getKey());
			entries.getValue().writeTo(out);
		}
		out.writeInt(orderedPipeNodes.size());
		for(Entry<Integer, String> entries : orderedPipeNodes.entrySet()){
			out.writeInt(entries.getKey());
			out.writeUTF(entries.getValue());
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PipelineWriteRateReport [pipeTransferRates=").append(pipeTransferRates)
				.append(", orderedPipeNodes=").append(orderedPipeNodes).append("]");
		return builder.toString();
	}

	
	
	
}
