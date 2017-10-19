package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import ch.cern.spark.Pair;
import ch.cern.spark.metrics.DatedValue;

public class DefinedMetricStore implements Serializable{
	
	private static final long serialVersionUID = 3020679839103994736L;
	
	public static final int MAX_AGGREGATION_SIZE = 100;
	
	private Map<String, DatedValue> values;
	
	private Map<String, Map<Map<String, String>, DatedValue>> aggregateValues;
	
	public DefinedMetricStore() {
		values = new HashMap<>();
		aggregateValues = new HashMap<>();
	}

	public void updateValue(String metricID, float value, Instant instant) {
		values.put(metricID, new DatedValue(instant, value));
	}
	
	public Optional<Double> getValue(String name) {
		Float value = getValues().get(name);
		
		return value != null ? Optional.of(value.doubleValue()) : Optional.empty();
	}
	
	public void updateAggregatedValue(String variableName, Map<String, String> ids, float value, Instant instant) {
		if(!aggregateValues.containsKey(variableName))
			aggregateValues.put(variableName, new HashMap<>());
		
		if(aggregateValues.get(variableName).size() <= MAX_AGGREGATION_SIZE)
			aggregateValues.get(variableName).put(ids, new DatedValue(instant, value));
	}
	
	public Map<String, Float> getValues() {
		return values.entrySet().stream()
				.map(entry -> new Pair<String, Float>(entry.getKey(), entry.getValue().getValue()))
				.collect(Collectors.toMap(Pair::first, Pair::second));
	}
	
	public List<Float> getAggregatedValues(String variableID) {
		if(!aggregateValues.containsKey(variableID))
			return new LinkedList<>();
		
		return new LinkedList<>(aggregateValues.get(variableID).values().stream()
															.map(DatedValue::getValue)
															.collect(Collectors.toList()));
	}
	
	public void purge(String variableID, Optional<Instant> oldestUpdateOpt) {
		if(!oldestUpdateOpt.isPresent())
			return;
		
		Instant oldestUpdate = oldestUpdateOpt.get();
		
		values.entrySet().removeIf(pair -> pair.getValue().getInstant().isBefore(oldestUpdate));

		Map<Map<String, String>, DatedValue> aggregateValuesForVariable = aggregateValues.get(variableID);
		if(aggregateValuesForVariable != null)
			aggregateValuesForVariable.entrySet().removeIf(pair -> pair.getValue().getInstant().isBefore(oldestUpdate));
	}

	@Override
	public String toString() {
		return "DefinedMetricStore [values=" + values + "]";
	}

}
