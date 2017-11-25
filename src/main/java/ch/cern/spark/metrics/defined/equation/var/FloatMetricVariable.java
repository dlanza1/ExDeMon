package ch.cern.spark.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

public class FloatMetricVariable extends MetricVariable{
	
	public static enum Operation {SUM, AVG, WEIGHTED_AVG, MIN, MAX, COUNT_FLOATS, DIFF};
	protected Operation aggregateOperation;

	public FloatMetricVariable(String name) {
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
			case SUM:
				val = sumAggregation(store.getAggregatedValues(name));
				break;
			case COUNT_FLOATS:
				val = new FloatValue(store.getAggregatedValues(name).size());
				break;
			case AVG:
				val = averageAggregation(store.getAggregatedValues(name));
				break;
			case WEIGHTED_AVG:
				val = weightedAverageAggregation(store.getAggregatedDatedValues(name), time);
				break;
			case MIN:
				val = minAggregation(store.getAggregatedValues(name));
				break;
			case MAX:
				val = maxAggregation(store.getAggregatedValues(name));
				break;
			case DIFF:
				val = differenceAggregation(store.getAggregatedDatedValues(name));
				break;
			}
			
			val.setSource(aggregateOperation.toString().toLowerCase() + "(var(" + name + "))=" + val);
		}

		return val;
	}

	private Value averageAggregation(List<Value> aggregatedValues) {
		DoubleStream doubleStream = toDoubleStream(aggregatedValues);
		
		OptionalDouble average = doubleStream.average();
		
		if(average.isPresent())
			return new FloatValue(average.getAsDouble());
		else
			return new ExceptionValue("no float values");
	}
	
	private Value minAggregation(List<Value> aggregatedValues) {
		DoubleStream doubleStream = toDoubleStream(aggregatedValues);
		
		OptionalDouble average = doubleStream.min();
		
		if(average.isPresent())
			return new FloatValue(average.getAsDouble());
		else
			return new ExceptionValue("no float values");
	}
	
	private Value maxAggregation(List<Value> aggregatedValues) {
		DoubleStream doubleStream = toDoubleStream(aggregatedValues);
		
		OptionalDouble average = doubleStream.max();
		
		if(average.isPresent())
			return new FloatValue(average.getAsDouble());
		else
			return new ExceptionValue("no float values");
	}

	private Value sumAggregation(List<Value> aggregatedValues) {
		DoubleStream doubleStream = toDoubleStream(aggregatedValues);
		
		return new FloatValue(doubleStream.sum());
	}

	private DoubleStream toDoubleStream(List<Value> aggregatedValues) {
		return aggregatedValues.stream()
				.filter(val -> val.getAsFloat().isPresent())
				.mapToDouble(val -> val.getAsFloat().get());
	}

	private Value weightedAverageAggregation(List<DatedValue> values, Instant time) {
        Optional<Pair<Double, Double>> pairSum = values.stream().filter(value -> value.getValue().getAsFloat().isPresent())
				.map(value -> {
					double weight = computeWeight(time, value.getInstant());
			    	
		    		return new Pair<Double, Double>(weight, weight * value.getValue().getAsFloat().get());
				})
				.reduce((p1, p2) -> new Pair<Double, Double>(p1.first + p2.first, p1.second + p2.second));

		if(!pairSum.isPresent())
			return new ExceptionValue("no float values");
		
		double totalWeights = pairSum.get().first;
		double weightedValues = pairSum.get().second;
		
		return new FloatValue(weightedValues / totalWeights);
	}

    private float computeWeight(Instant time, Instant metric_timestamp) {
        Duration time_difference = Duration.between(time, metric_timestamp).abs();
        
        if(expirePeriod.compareTo(time_difference) < 0)
        		return 0;
        				
        return (float) (expirePeriod.getSeconds() - time_difference.getSeconds()) / (float) expirePeriod.getSeconds();
    }
    
	private Value differenceAggregation(List<DatedValue> aggregatedDatedValues) {
		if(aggregatedDatedValues.size() < 2) {
			return new ExceptionValue("no float values");
		}else {
			List<DatedValue> sorted = aggregatedDatedValues.stream()
												.filter(value -> value.getValue().getAsFloat().isPresent())
												.sorted()
												.collect(Collectors.toList());
			
			double lastValue = sorted.get(sorted.size() - 1).getValue().getAsFloat().get();
			double previousValue = sorted.get(sorted.size() - 2).getValue().getAsFloat().get();

			return new FloatValue(lastValue - previousValue);
		}
	}

	@Override
	public void updateStore(MetricVariableStore store, Metric metric) {	
		if(!metric.getValue().getAsFloat().isPresent())
			return;
		
		if(aggregateOperation == null)
			store.updateValue(metric.getValue(), metric.getInstant());
		else
			store.updateAggregatedValue(metric.getIDs().hashCode(), metric.getValue(), metric.getInstant());
	}

	@Override
	public Class<FloatValue> returnType() {
		return FloatValue.class;
	}
	
	@Override
	public String toString() {
		if(aggregateOperation != null)
			return aggregateOperation + "(filter_float(" + name + "))";
		else
			return "filter_float(" + name + ")";
	}
	
}
