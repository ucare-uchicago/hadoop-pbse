package org.apache.hadoop.mapreduce.v2.app.speculate;

// @Cesar: Holds the info of a map host
public class ShuffleHost implements Comparable<ShuffleHost>{
	
	private String mapHost = null;
	
	public ShuffleHost(String mapHost) {
		super();
		this.mapHost = mapHost;
	}

	public String getMapHost() {
		return mapHost;
	}

	public void setMapHost(String mapHost) {
		this.mapHost = mapHost;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapHost == null) ? 0 : mapHost.hashCode());
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
		ShuffleHost other = (ShuffleHost) obj;
		if (mapHost == null) {
			if (other.mapHost != null)
				return false;
		} else if (!mapHost.equals(other.mapHost))
			return false;
		return true;
	}

	@Override
	public int compareTo(ShuffleHost other) {
		if(mapHost != null)
			return mapHost.compareTo(other.mapHost);
		return other.mapHost == null? 0 : 1;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ShuffleHost [mapHost=").append(mapHost).append("]");
		return builder.toString();
	}
	
	
	
}
