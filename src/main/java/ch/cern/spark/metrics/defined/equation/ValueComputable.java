package ch.cern.spark.metrics.defined.equation;

import java.time.Instant;

import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.value.Value;

public interface ValueComputable {

	public Value compute(DefinedMetricStore store, Instant time);
	
	/**
	 * Serves the check casting (generic types are not checked when parsing equation) 
	 * 
	 * @return Should be same as <T>
	 */
	public Class<? extends Value> returnType();
	
}
