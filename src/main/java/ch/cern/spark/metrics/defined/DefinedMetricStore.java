package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DefinedMetricStore implements Serializable{
	
	private static final long serialVersionUID = 3020679839103994736L;
	
	private Map<String, Float> values; 
	
	public DefinedMetricStore() {
		values = new HashMap<>();
	}

	public void updateValue(String metricID, float value) {
		values.put(metricID, value);
	}
	
	public Map<String, Float> getValues() {
		return values;
	}

	@Override
	public String toString() {
		return "DefinedMetricStore [values=" + values + "]";
	}

}
