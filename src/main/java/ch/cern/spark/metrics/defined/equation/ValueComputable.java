package ch.cern.spark.metrics.defined.equation;

import java.time.Instant;

import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.value.Value;

public interface ValueComputable {

	public Value compute(VariableStatuses store, Instant time);
	
	/**
	 * Serves the check casting (generic types are not checked when parsing equation) 
	 */
	public Class<? extends Value> returnType();
	
}
