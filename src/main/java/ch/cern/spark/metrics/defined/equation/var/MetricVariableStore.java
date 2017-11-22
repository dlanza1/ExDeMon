package ch.cern.spark.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.store.Store;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.utils.TimeUtils;

public class MetricVariableStore implements Store{

	private static final long serialVersionUID = -7439047274576894171L;

	public static final int MAX_AGGREGATION_SIZE = 100;
	
	private DatedValue value;
	
	private Map<Integer, DatedValue> aggregateValues;
	private Map<Instant, Value> aggregateValuesForEmptyAttributes;
	
	public MetricVariableStore() {
		aggregateValues = new HashMap<>();
		aggregateValuesForEmptyAttributes = new HashMap<>();
	}

	public void updateValue(Value newValue, Instant instant) {
		value = new DatedValue(instant, newValue);
	}
	
	public Value getValue(Duration expirePeriod) {
		return value != null ? value.getValue() : new ExceptionValue("no value" + (expirePeriod != null ? " for the last " + TimeUtils.toString(expirePeriod) : ""));
	}
	
	public void updateAggregatedValue(int idHash, float value, Instant instant) {
		updateAggregatedValue(idHash, new FloatValue(value), instant);
	}
	
	public void updateAggregatedValue(int idHash, Value value, Instant instant) {
		if(emptyAttrbutes(idHash)) {
			if(aggregateValuesForEmptyAttributes.size() <= MAX_AGGREGATION_SIZE)
		        aggregateValuesForEmptyAttributes.put(instant, value);
		}else {
			if(aggregateValues.size() <= MAX_AGGREGATION_SIZE)
		        aggregateValues.put(idHash, new DatedValue(instant, value));
		}
	}
	
	private boolean emptyAttrbutes(int idHash) {
        return idHash == 0;
    }
	
	public List<Value> getAggregatedValues(String variableID) {
	    List<Value> values = new LinkedList<>();
	    
		values.addAll(aggregateValues.values().stream()
                                                    .map(DatedValue::getValue)
                                                    .collect(Collectors.toList()));
		
		values.addAll(aggregateValuesForEmptyAttributes.values());
		
		return values;
	}
	
	public List<DatedValue> getAggregatedDatedValues(String variableID) {
	    List<DatedValue> values = new LinkedList<>();
	    
		values.addAll(aggregateValues.values());
		
		values.addAll(aggregateValuesForEmptyAttributes.entrySet().stream()
		    											.map(entry -> new DatedValue(entry.getKey(), entry.getValue()))
		    											.collect(Collectors.toList()));
		
		return values;
	}
	
	public void purge(String variableID, Optional<Instant> oldestUpdateOpt) {
		if(!oldestUpdateOpt.isPresent())
			return;
		
		Instant oldestUpdate = oldestUpdateOpt.get();
		
		if(value != null && value.getInstant().isBefore(oldestUpdate))
			value = null;

		if(aggregateValues != null)
			aggregateValues.values().removeIf(value -> value.getInstant().isBefore(oldestUpdate));
		
        if(aggregateValuesForEmptyAttributes != null)
        		aggregateValuesForEmptyAttributes.keySet().removeIf(time -> time.isBefore(oldestUpdate));
	}

	@Override
	public String toString() {
		return "MetricVariableStore [value=" + value + ", aggregateValues=" + aggregateValues + "]";
	}

}
