package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.Set;

// @Cesar: Represents a shuffle estimator, which decides if 
// a map host is being slow
public abstract class ShuffleEstimator {
	
	public abstract boolean isSlow(String host, 
								   Set<ShuffleRateInfo> reportedRates,
								   double thressholdTransferRate,
								   double taskProgressLimit);
	
}
