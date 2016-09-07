package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

// @Cesar: Represents a fetch rate report
public class FetchRateReport implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final String TASK_SEPARATOR = "@taskRef=";
	
	// @Cesar: Remove the task ref from host name
	public static final String getHostName(String hostNameWithReference){
		if(hostNameWithReference != null && hostNameWithReference.contains(TASK_SEPARATOR)){
			return hostNameWithReference.split(TASK_SEPARATOR)[0];
		}
		return hostNameWithReference;
	}
	
	// @Cesar: mapperHost -> <bytes read, nanos took>
	Map<String, ShuffleData> fetchRateReport = null;	
	
	public FetchRateReport(){
		fetchRateReport = new ConcurrentHashMap<>();
	}
	
	public void addReport(String mapperHost, ShuffleData shuffleData){
		// Cesar: This is quick fix
		String mappedName = mapperHost + TASK_SEPARATOR + shuffleData.getMapTaskId();
		fetchRateReport.put(mappedName, shuffleData);
	}
		
	
	public Map<String, ShuffleData> getFetchRateReport() {
		return fetchRateReport;
	}


	public static FetchRateReport readFrom(DataInput in) throws IOException{
		FetchRateReport report = new FetchRateReport();
		int noReports = in.readInt();
		for(int i = 0; i < noReports; ++i){
			String host = in.readUTF();
			ShuffleData dt = new ShuffleData();
			dt.readFrom(in);
			report.addReport(host, dt);
		}
		return report;
	}
	
	public void writeTo(DataOutput out) throws IOException{
		out.writeInt(fetchRateReport.size());
		for(Entry<String, ShuffleData> entries : fetchRateReport.entrySet()){
			out.writeUTF(entries.getKey());
			entries.getValue().writeTo(out);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("FetchRateReport [fecthRateReport=").append(fetchRateReport).append("]");
		return builder.toString();
	}

}
