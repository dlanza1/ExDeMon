package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Optional;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

public class BooleanMetricVariable extends MetricVariable{
	
	public static enum Operation {COUNT_BOOLS, COUNT_TRUE, COUNT_FALSE};
	protected Operation aggregateOperation;

	public BooleanMetricVariable(String name) {
		super(name);
	}
	
	@Override
	public MetricVariable config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		String aggregateVal = properties.getProperty("aggregate");
		if(aggregateVal != null)
			try{
				aggregateOperation = Operation.valueOf(aggregateVal.toUpperCase());
			}catch(IllegalArgumentException e) {
				throw new ConfigurationException("Variable " + name + ": aggregation operation (" + aggregateVal + ") not available");
			}
		
		properties.confirmAllPropertiesUsed();
		
		return this;
	}
	
	@Override
	public Value compute(MetricVariableStore store, Instant time) {
		Optional<Instant> oldestUpdate = Optional.empty();
		if(expirePeriod != null)
			oldestUpdate = Optional.of(time.minus(expirePeriod));
		store.purge(name, oldestUpdate);
		
		Value val = null;
		if(aggregateOperation == null) {
			val = store.getValue(expirePeriod);
			
			String source = val.toString();
			if(val.getAsException().isPresent())
				val = new ExceptionValue("Variable " + name + ": " + val.getAsException().get());
			
			val.setSource("var(" + name + ")=" + source);
		}else {
			switch (aggregateOperation) {
			case COUNT_BOOLS:
				val = new FloatValue(store.getAggregatedValues(name).size());
				break;
			case COUNT_TRUE:
				val = new FloatValue(store.getAggregatedValues(name).stream().filter(b -> b.getAsBoolean().get()).count());
				break;
			case COUNT_FALSE:
				val = new FloatValue(store.getAggregatedValues(name).stream().filter(b -> !b.getAsBoolean().get()).count());
				break;
			}
			
			val.setSource(aggregateOperation.toString().toLowerCase() + "(" + name + ")=" + val);
		}

		return val;
	}

	@Override
	public void updateStore(MetricVariableStore store, Metric metric) {	
		if(!metric.getValue().getAsBoolean().isPresent())
			return;
		
		if(aggregateOperation == null)
			store.updateValue(metric.getValue(), metric.getInstant());
		else
			store.updateAggregatedValue(metric.getIDs().hashCode(), metric.getValue(), metric.getInstant());
	}

	@Override
	public Class<BooleanValue> returnType() {
		return BooleanValue.class;
	}
	
	@Override
	public String toString() {
		if(aggregateOperation != null)
			return aggregateOperation + "(filter_bool(" + name + "))";
		else
			return "filter_bool(" + name + ")";
	}
	
}
