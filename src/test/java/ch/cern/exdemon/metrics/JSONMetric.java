package ch.cern.exdemon.metrics;

import java.time.Instant;

@SuppressWarnings("unused")
public class JSONMetric {
	
	private String CLUSTER, HOSTNAME, METRIC, KEY_TO_REMOVE;
	
	private Instant TIMESTAMP;
	
	private float VALUE;
	
	public JSONMetric(Metric metric) {
		CLUSTER = metric.getAttributes().get("CLUSTER");
		HOSTNAME = metric.getAttributes().get("HOSTNAME");
		METRIC = metric.getAttributes().get("METRIC");
		KEY_TO_REMOVE = metric.getAttributes().get("KEY_TO_REMOVE");
		
		TIMESTAMP = metric.getTimestamp();
		
		VALUE = metric.getValue().getAsFloat().get();
	}

}
