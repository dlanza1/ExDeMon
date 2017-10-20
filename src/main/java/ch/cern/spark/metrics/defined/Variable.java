package ch.cern.spark.metrics.defined;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;

import org.apache.kafka.common.config.ConfigException;

import ch.cern.ConfigurationException;
import ch.cern.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.Filter;

public class Variable implements Predicate<Metric>{

	private String name;
	
	private Filter filter;
	
	public enum Operation {SUM, AVG, MIN, MAX, COUNT};
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
		
		DoubleStream values = store.getAggregatedValues(name).stream().mapToDouble(val -> val);
		
		OptionalDouble val = null;
		switch (aggregateOperation) {
		case SUM:
			return Optional.of(values.sum());
		case COUNT:
			return Optional.of((double) values.count());
		case AVG:
			val = values.average();
			break;
		case MIN:
			val = values.min();
			break;
		case MAX:
			val = values.max();
			break;
		default:
			throw new RuntimeException("Operation not implemented");
		}
		
		return val.isPresent() ? Optional.of(val.getAsDouble()) : Optional.empty();
	}

}
