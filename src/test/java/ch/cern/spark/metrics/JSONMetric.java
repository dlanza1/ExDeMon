package ch.cern.spark.metrics;

import java.time.Instant;

@SuppressWarnings("unused")
public class JSONMetric {
	
	private String CLUSTER, HOSTNAME, METRIC;
	
	private Instant TIMESTAMP;
	
	private float VALUE;
	
	public JSONMetric(Metric metric) {
		CLUSTER = metric.getIDs().get("CLUSTER");
		HOSTNAME = metric.getIDs().get("HOSTNAME");
		METRIC = metric.getIDs().get("METRIC");
		
		TIMESTAMP = metric.getInstant();
		
		VALUE = metric.getValue();
	}

}
