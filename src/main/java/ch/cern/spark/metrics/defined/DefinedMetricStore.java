package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import ch.cern.spark.Pair;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.utils.TimeUtils;

public class DefinedMetricStore implements Serializable{
	
	private static final long serialVersionUID = 3020679839103994736L;
	
	public static final int MAX_AGGREGATION_SIZE = 100;
	
	private Map<String, DatedValue> values;
	
	private Map<String, Map<Integer, DatedValue>> aggregateValues;
	private Map<String, Map<Instant, Value>> aggregateValuesForEmptyAttributes;
	
	public DefinedMetricStore() {
		values = new HashMap<>();
		aggregateValues = new HashMap<>();
		aggregateValuesForEmptyAttributes = new HashMap<>();
	}

	public void updateValue(String variableName, Value value, Instant instant) {
		values.put(variableName, new DatedValue(instant, value));
	}
	
	public Value getValue(String name, Duration expirePeriod) {
		Value value = getValues().get(name);
		
		return value != null ? value : new ExceptionValue("no value" 
											+ (expirePeriod != null ? 
													" for the last " + TimeUtils.toString(expirePeriod) 
													: ""));
	}
	
	public void updateAggregatedValue(String variableName, int idHash, float value, Instant instant) {
		updateAggregatedValue(variableName, idHash, new FloatValue(value), instant);
	}
	
	public void updateAggregatedValue(String variableName, int idHash, Value value, Instant instant) {
		if(!emptyAttrbutes(idHash)){
		    if(!aggregateValues.containsKey(variableName))
	            aggregateValues.put(variableName, new HashMap<>());
		    
    		if(aggregateValues.get(variableName).size() <= MAX_AGGREGATION_SIZE)
    			aggregateValues.get(variableName).put(idHash, new DatedValue(instant, value));
		}else{
		    if(!aggregateValuesForEmptyAttributes.containsKey(variableName))
		        aggregateValuesForEmptyAttributes.put(variableName, new HashMap<>());
		    
		    if(aggregateValuesForEmptyAttributes.get(variableName).size() <= MAX_AGGREGATION_SIZE)
		        aggregateValuesForEmptyAttributes.get(variableName).put(instant, value);
		}
	}
	
	private boolean emptyAttrbutes(int idHash) {
        return idHash == 0;
    }

    public Map<String, Value> getValues() {
		return values.entrySet().stream()
				.map(entry -> new Pair<String, Value>(entry.getKey(), entry.getValue().getValue()))
				.collect(Collectors.toMap(Pair::first, Pair::second));
	}
	
	public List<Value> getAggregatedValues(String variableID) {
	    List<Value> values = new LinkedList<>();
	    
		if(aggregateValues.containsKey(variableID))
			values.addAll(aggregateValues.get(variableID).values().stream()
                                                    .map(DatedValue::getValue)
                                                    .collect(Collectors.toList()));
		
		if(aggregateValuesForEmptyAttributes.containsKey(variableID))
		    values.addAll(aggregateValuesForEmptyAttributes.get(variableID).values());
		
		return values;
	}
	
	public List<DatedValue> getAggregatedDatedValues(String variableID) {
	    List<DatedValue> values = new LinkedList<>();
	    
		if(aggregateValues.containsKey(variableID))
			values.addAll(aggregateValues.get(variableID).values());
		
		if(aggregateValuesForEmptyAttributes.containsKey(variableID))
		    values.addAll(aggregateValuesForEmptyAttributes.get(variableID).entrySet().stream()
		    											.map(entry -> new DatedValue(entry.getKey(), entry.getValue()))
		    											.collect(Collectors.toList()));
		
		return values;
	}
	
	public void purge(String variableID, Optional<Instant> oldestUpdateOpt) {
		if(!oldestUpdateOpt.isPresent())
			return;
		
		Instant oldestUpdate = oldestUpdateOpt.get();
		
		if(values.containsKey(variableID) && values.get(variableID).getInstant().isBefore(oldestUpdate))
			values.remove(variableID);

		Map<Integer, DatedValue> aggregateValuesForVariable = aggregateValues.get(variableID);
		if(aggregateValuesForVariable != null)
			aggregateValuesForVariable.values().removeIf(value -> value.getInstant().isBefore(oldestUpdate));
		
		Map<Instant, Value> aggregateValuesForVariableWithEmptyAttributes = aggregateValuesForEmptyAttributes.get(variableID);
        if(aggregateValuesForVariableWithEmptyAttributes != null)
            aggregateValuesForVariableWithEmptyAttributes.keySet().removeIf(time -> time.isBefore(oldestUpdate));
	}

	@Override
	public String toString() {
		return "DefinedMetricStore [values=" + values + ", aggregateValues=" + aggregateValues + "]";
	}

}
