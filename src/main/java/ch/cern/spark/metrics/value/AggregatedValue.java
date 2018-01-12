package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;

public class AggregatedValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private Value agg;
	
	public AggregatedValue(Value value){
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((agg == null) ? 0 : agg.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AggregatedValue other = (AggregatedValue) obj;
        if (agg == null) {
            if (other.agg != null)
                return false;
        } else if (!agg.equals(other.agg))
            return false;
        return true;
    }
	
}
