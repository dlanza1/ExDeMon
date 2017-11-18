package ch.cern.spark.metrics;

import java.time.Instant;

@SuppressWarnings("unused")
public class JSONMetric {
	
	private String CLUSTER, HOSTNAME, METRIC, KEY_TO_REMOVE;
	
	private Instant TIMESTAMP;
	
	private float VALUE;
	
	public JSONMetric(Metric metric) {
		CLUSTER = metric.getIDs().get("CLUSTER");
		HOSTNAME = metric.getIDs().get("HOSTNAME");
		METRIC = metric.getIDs().get("METRIC");
		KEY_TO_REMOVE = metric.getIDs().get("KEY_TO_REMOVE");
		
		TIMESTAMP = metric.getInstant();
		
		VALUE = metric.getValue().getAsFloat().get();
	}

}
