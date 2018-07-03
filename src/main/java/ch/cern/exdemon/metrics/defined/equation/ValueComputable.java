package ch.cern.exdemon.metrics.defined.equation;

import java.time.Instant;

import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.Value;

public interface ValueComputable {

	public Value compute(VariableStatuses store, Instant time);
	
	/**
	 * Serves the check casting (generic types are not checked when parsing equation) 
	 */
	public Class<? extends Value> returnType();
	
}
