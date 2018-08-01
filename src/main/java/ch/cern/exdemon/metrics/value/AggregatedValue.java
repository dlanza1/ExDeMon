package ch.cern.exdemon.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

@EqualsAndHashCode(callSuper=false)
public class AggregatedValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private Value agg;
	
	public AggregatedValue(@NonNull Value value){
		this.agg = value;
	}
	
	@Override
	public AggregatedValue compute(VariableStatuses store, Instant time) {
		return new AggregatedValue(agg);
	}

	@Override
	public Optional<Value> getAsAggregated() {
	    return Optional.of(this.agg);
	}
	@Override
	public Class<AggregatedValue> returnType() {
		return AggregatedValue.class;
	}
	
	@Override
	public String toString() {
		return agg.toString() + " (agg)";
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}
	
}
