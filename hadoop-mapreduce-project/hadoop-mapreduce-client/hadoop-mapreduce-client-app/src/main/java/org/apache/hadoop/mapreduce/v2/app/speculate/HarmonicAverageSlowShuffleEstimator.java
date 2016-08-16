package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// @Cesar: This class estimates if a map node is slow
public class HarmonicAverageSlowShuffleEstimator extends ShuffleEstimator{

	private static final Log LOG = LogFactory.getLog(HarmonicAverageSlowShuffleEstimator.class);
	
	public boolean isSlow(String host, 
						  Set<ShuffleRateInfo> reportedRates,
						  double thressholdTransferRate,
						  double taskProgressLimit){
		// @Cesar: So, calculate in here. 
		LOG.info("@Cesar: Analyzing host " + host + " with " + reportedRates.size() + " reports");
		int numLocalReduceTasks = 0;
		int numRemoteReduceTasks = 0;
		double remoteTransferRateSum = 0.0;
		double localTransferRateSum = 0.0;
		double localTransferWeightSum = 0.0;
		double remoteTransferWeightSum = 0.0;
		boolean hasRemoteRates = false;
		boolean hasLocalRates = false;
		boolean enteredLoop = false;
		Iterator<ShuffleRateInfo> reportsIterator = reportedRates.iterator();
		// @Cesar: Weighted harmonic average WH = sum(wi) / sum(wi/xi)
		// In this case, wi is the amount of megabits to be transfered (weight)
		// and xi will be the transfer rate.
		while(reportsIterator.hasNext()){
			ShuffleRateInfo report = reportsIterator.next();
			// @Cesar: If we finished, we continue
			if(((double)report.getShuffledBytes() / (double)report.getTotalBytes()) >= taskProgressLimit){
				LOG.info("@Cesar: The progress of this transfer is "  + ((double)report.getShuffledBytes() / (double)report.getTotalBytes()) + 
						" >= " + taskProgressLimit + ", so it wont be counted [" + report.getMapTaskAttempId() + " <--- " + 
						report.getReduceTaskAttempId() + "]");
				continue;
			}
			// @Cesar: True if we entered the loop
			enteredLoop = true;
			// @Cesar: So, two things. First, average the remote reduces
			// I will use harmonic average for this
			// transfer rate is in megabit, change unit
			double numMegaBits = (double)report.getTotalBytes() * 8.0 / 1024.0 / 1024.0;
			if(report.getReduceHost().compareToIgnoreCase(host) == 0){
				// @Cesar: Both reduce and map on same node
				++numLocalReduceTasks;
				localTransferRateSum += ((report.getFetchRate()!= 0? numMegaBits : 0.0) / 
										 (report.getFetchRate()!= 0? (double)report.getFetchRate() : 1.0));
				localTransferWeightSum += numMegaBits;
				hasLocalRates = true;
			}
			else{
				// @Cesar: Running in different nodes
				++numRemoteReduceTasks;
				remoteTransferRateSum += ((report.getFetchRate()!= 0? numMegaBits : 0.0) / 
						 				  (report.getFetchRate()!= 0? (double)report.getFetchRate() : 1.0));
				remoteTransferWeightSum += numMegaBits;
				hasRemoteRates = true;
			}
		}
		if(LOG.isDebugEnabled()){
			LOG.debug("@Cesar: There are " + numLocalReduceTasks + " fetching from node " + host + " and " + 
					 numRemoteReduceTasks + " fetching from other nodes");
		}	
		// @Cesar: Now, harmonic weighted average to get average transfer rates
		double harmonicLocalRate = localTransferWeightSum / (localTransferRateSum != 0? localTransferRateSum : 1.0);
		double harmonicRemoteRate = remoteTransferWeightSum / (remoteTransferRateSum != 0? remoteTransferRateSum : 1.0);
		if(LOG.isDebugEnabled()){
			LOG.debug("@Cesar: weighted harmonic local average: "  + localTransferWeightSum + " / " + (localTransferRateSum != 0? localTransferRateSum : 1.0) + 
					" = " + harmonicLocalRate);
			LOG.debug("@Cesar: weighted harmonic remote average: "  + remoteTransferWeightSum + " / " + (remoteTransferRateSum != 0? remoteTransferRateSum : 1.0) + 
					" = " + harmonicRemoteRate);
		}
		// @Cesar: So, is this node slow? It is so if all remote task report it as slow or all local tasks 
		// do. Why? Well, if all remote tasks say its slow but local task say the opposite then its ok to
		// relaunch all its map tasks, since  remote tasks wont finish. The same applies for slow local and fast remote
		// since the important thing is that all reduce tasks can finish
		boolean result = enteredLoop && (hasLocalRates && harmonicLocalRate <= thressholdTransferRate ) || 
				(hasRemoteRates && harmonicRemoteRate <= thressholdTransferRate);
		LOG.info("@Cesar: Analisys for host " + host + ": " + "enteredLoop=" + enteredLoop  + " && (hasLocalRates=" + hasLocalRates + " && " + 
				harmonicLocalRate + " <= " + thressholdTransferRate + ") || (hasRemoteRates=" + hasRemoteRates + 
				" && " + harmonicRemoteRate + " <= " + thressholdTransferRate +") = " + result);
		return result;
	}
	
}
