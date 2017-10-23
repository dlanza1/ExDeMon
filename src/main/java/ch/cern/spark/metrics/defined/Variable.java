package ch.cern.spark.metrics.defined;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;

import ch.cern.ConfigurationException;
import ch.cern.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.Filter;

public class Variable implements Predicate<Metric>{

	private String name;
	
	private Filter filter;
	
	public enum Operation {SUM, AVG, WEIGHTED_AVG, MIN, MAX, COUNT, DIFF};
	private Operation aggregateOperation;

	private Duration expirePeriod;

	public Variable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public Variable config(Properties properties) throws ConfigurationException {
		filter = Filter.build(properties.getSubset("filter"));
		
		if(properties.containsKey("expire") && properties.getProperty("expire").toLowerCase().equals("never"))
			expirePeriod = null;
		else
			expirePeriod = properties.getPeriod("expire", Duration.ofMinutes(10));
		
		String aggregateVal = properties.getProperty("aggregate");
		if(aggregateVal != null)
			try{
				aggregateOperation = Operation.valueOf(aggregateVal.toUpperCase());
			}catch(IllegalArgumentException e) {
				throw new ConfigException("Variable " + name + ": aggregation operation (" + aggregateVal + ") not available");
			}
		
		return this;
	}

	@Override
	public boolean test(Metric metric) {
		return filter.test(metric);
	}

	public void updateStore(DefinedMetricStore store, Metric metric) {		
		if(aggregateOperation == null)
			store.updateValue(name, metric.getValue(), metric.getInstant());
		else
			store.updateAggregatedValue(name, metric.getIDs().hashCode(), metric.getValue(), metric.getInstant());
	}

	public Optional<Double> compute(DefinedMetricStore store, Instant time) {
		Optional<Instant> oldestUpdate = Optional.empty();
		if(expirePeriod != null)
			oldestUpdate = Optional.of(time.minus(expirePeriod));
		store.purge(name, oldestUpdate);
		
		if(aggregateOperation == null)
			return store.getValue(name);
		
		OptionalDouble val = null;
		switch (aggregateOperation) {
		case SUM:
			return Optional.of(store.getAggregatedValues(name).sum());
		case COUNT:
			return Optional.of((double) store.getAggregatedValues(name).count());
		case AVG:
			val = store.getAggregatedValues(name).average();
			break;
		case WEIGHTED_AVG:
			return weightedAverageAggregation(store.getAggregatedDatedValues(name), time);
		case MIN:
			val = store.getAggregatedValues(name).min();
			break;
		case MAX:
			val = store.getAggregatedValues(name).max();
			break;
		case DIFF:
			return differenceAggregation(store.getAggregatedDatedValues(name));
		default:
			throw new RuntimeException("Operation not implemented");
		}
		
		return val.isPresent() ? Optional.of(val.getAsDouble()) : Optional.empty();
	}

	private Optional<Double> weightedAverageAggregation(List<DatedValue> values, Instant time) {
        Optional<Pair<Double, Double>> pairSum = values.stream()
				.map(value -> {
					double weight = computeWeight(time, value.getInstant());
			    	
		    		return new Pair<Double, Double>(weight, weight * value.getValue());
				})
				.reduce((p1, p2) -> new Pair<Double, Double>(p1.first + p2.first, p1.second + p2.second));

		if(!pairSum.isPresent())
			return Optional.empty();
		
		double totalWeights = pairSum.get().first;
		double weightedValues = pairSum.get().second;
		
		return Optional.of(weightedValues / totalWeights);
	}

    private float computeWeight(Instant time, Instant metric_timestamp) {
        Duration time_difference = Duration.between(time, metric_timestamp).abs();
        
        if(expirePeriod.compareTo(time_difference) < 0)
        		return 0;
        				
        return (float) (expirePeriod.getSeconds() - time_difference.getSeconds()) / (float) expirePeriod.getSeconds();
    }
    
	private Optional<Double> differenceAggregation(List<DatedValue> aggregatedDatedValues) {
		if(aggregatedDatedValues.size() < 2) {
			return Optional.empty();
		}else {
			List<DatedValue> sorted = aggregatedDatedValues.stream().sorted().collect(Collectors.toList());
			
			return Optional.of((double) (sorted.get(sorted.size() - 1).getValue() - sorted.get(sorted.size() - 2).getValue()));
		}
	}
	
}
