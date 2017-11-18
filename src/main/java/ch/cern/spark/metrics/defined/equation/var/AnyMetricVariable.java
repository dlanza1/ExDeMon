package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;

public class AnyMetricVariable extends MetricVariable{
	
	public AnyMetricVariable(String name) {
		super(name);
	}
	
	@Override
	public Value compute(DefinedMetricStore store, Instant time) {
		Optional<Instant> oldestUpdate = Optional.empty();
		if(expirePeriod != null)
			oldestUpdate = Optional.of(time.minus(expirePeriod));
		store.purge(name, oldestUpdate);

		Value val = store.getValue(name, expirePeriod);
		
		String source = val.toString();
		if(val.getAsException().isPresent())
			val = new ExceptionValue("Variable " + name + ": " + val.getAsException().get());
		
		val.setSource("var(" + name + ")=" + source);
		
		return val;
	}

	@Override
	public void updateStore(DefinedMetricStore store, Metric metric) {	
		store.updateValue(name, metric.getValue(), metric.getInstant());
	}

	@Override
	public Class<Value> returnType() {
		return Value.class;
	}
	
	@Override
	public String toString() {
		return name;
	}
	
}
