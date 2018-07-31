package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.value.AggregatedValue;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

@ComponentType(Type.AGGREGATION)
public abstract class Aggregation extends Component {

    private static final long serialVersionUID = -7737512090935679020L;
    
    public abstract Class<? extends Value> inputType();

    public Value aggregateValues(Collection<DatedValue> values, Instant time) {
        values = filterByTypes(values);
        
        Value result = aggregate(values, time);
        
        if(result.getAsException().isPresent())
            return result;
        else
            return new AggregatedValue(result);
    }
    
    private Collection<DatedValue> filterByTypes(Collection<DatedValue> values) {
        Class<? extends Value> inputType = inputType();
        Class<? extends Value> returnType = returnType();
        
        if(inputType.equals(FloatValue.class))
            values = values.stream().filter(v -> v.getValue().getAsFloat().isPresent() 
                                             || (v.getValue().getAsAggregated().isPresent() && v.getValue().getAsAggregated().get().getClass().equals(returnType))).collect(Collectors.toList());
        else if(inputType.equals(BooleanValue.class))
            values = values.stream().filter(v -> v.getValue().getAsBoolean().isPresent() 
                                             || (v.getValue().getAsAggregated().isPresent() && v.getValue().getAsAggregated().get().getClass().equals(returnType))).collect(Collectors.toList());
        else if(inputType.equals(StringValue.class))
            values = values.stream().filter(v -> v.getValue().getAsString().isPresent() 
                                             || (v.getValue().getAsAggregated().isPresent() && v.getValue().getAsAggregated().get().getClass().equals(returnType))).collect(Collectors.toList());
        
        return values;
    }

    protected abstract Value aggregate(Collection<DatedValue> values, Instant time);
    
    public abstract Class<? extends Value> returnType();
    
    @Override
    public boolean equals(Object obj) {
        return this.getClass().equals(obj.getClass());
    }
    
    public boolean isFilterEnable() {
        return true;
    }

    public void postUpdateStatus(ValueVariable metricVariable, AggregationValues aggValues, Metric metric) {
    }

    public void postUpdateStatus(ValueVariable metricVariable, ValueHistory history, Metric metric) {
    }
    
}
