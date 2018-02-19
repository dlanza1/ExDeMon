package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.defined.equation.ComputationException;
import ch.cern.spark.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.spark.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.spark.metrics.defined.equation.var.agg.LastValueAggregation;
import ch.cern.spark.metrics.defined.equation.var.agg.WAvgAggregation;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.utils.DurationAndTruncate;
import ch.cern.utils.TimeUtils;
import lombok.Getter;

public class MetricVariable extends Variable {
    
    public static final long MAX_SIZE_DEFAULT = 10000;
	
	private MetricsFilter filter;

	@Getter
	private Aggregation aggregation;
	
	protected DurationAndTruncate expire;
	protected DurationAndTruncate ignore;

	private Set<String> aggregateSelectAtt;
	private boolean aggregateSelectALL = false;

    private int max_aggregation_size;
    
    private ChronoUnit granularity;

	public MetricVariable(String name) {
		super(name);
	}

	public MetricVariable config(Properties properties, Optional<Class<? extends Value>> typeOpt) throws ConfigurationException {
		filter = MetricsFilter.build(properties.getSubset("filter"));
		
		if(properties.containsKey("expire") && properties.getProperty("expire").toLowerCase().equals("never"))
            expire = null;
        else
            expire = DurationAndTruncate.from(properties.getProperty("expire", "10m"));
		
		String aggregateVal = properties.getProperty("aggregate.type");
        if(aggregateVal != null) {            
            aggregation = ComponentManager.build(Type.AGGREGATION, properties.getSubset("aggregate"));     
            
            if(aggregation instanceof WAvgAggregation)
                ((WAvgAggregation) aggregation).setExpire(expire);
            
            if(typeOpt.isPresent() && !aggregation.returnType().equals(typeOpt.get()))
                throw new ConfigurationException("Variable "+name+" returns type "+aggregation.returnType().getSimpleName()+" because of its aggregation operation, "
                                                            + "but in the equation there is a function that uses it as type " + typeOpt.get().getSimpleName());
        }else {
            if(typeOpt.isPresent())
                aggregation = new LastValueAggregation(typeOpt.get());
            else
                aggregation = new LastValueAggregation(Value.class);
        }
		
		if(!properties.containsKey("ignore"))
		    ignore = null;
        else
            ignore = DurationAndTruncate.from(properties.getProperty("ignore"));
		
		String granularityString = properties.getProperty("aggregate.history.granularity");
		if(granularityString != null)
		    granularity = TimeUtils.parseGranularity(granularityString);
		
		String aggregateSelect = properties.getProperty("aggregate.attributes");
		if(aggregateSelect != null && aggregateSelect.equals("ALL"))
		    aggregateSelectALL = true;
		else if(aggregateSelect != null)
			aggregateSelectAtt = new HashSet<String>(Arrays.asList(aggregateSelect.split("\\s")));
		
		max_aggregation_size = Integer.parseInt(properties.getProperty("aggregate.max-size", MAX_SIZE_DEFAULT+""));
		
		return this;
	}

	@Override
	public boolean test(Metric metric) {
		return filter.test(metric);
	}

	@Override
	public Value compute(VariableStatuses variableStatuses, Instant time) {
	    StatusValue status = variableStatuses.get(name);
	    
        Value aggValue = null;
        try {
            Collection<DatedValue> values = getDatedValues(status, time, aggregation.inputType());
            
            aggValue = aggregation.aggregateValues(values, time);
            
            if(aggValue.getAsAggregated().isPresent())
                aggValue = aggValue.getAsAggregated().get();
        } catch (ComputationException e) {
            aggValue = new ExceptionValue(e.getMessage());
        }
	    
	    String source = aggValue.toString();
	    if(aggValue.getAsException().isPresent())
	        aggValue = new ExceptionValue("Variable " + name + ": " + aggValue.getAsException().get());
		
        if(aggregation instanceof LastValueAggregation) {
            aggValue.setSource("var(" + name + ")=" + source);
        }else{
            String aggName = aggregation.getClass().getAnnotation(RegisterComponent.class).value();
            aggValue.setSource(aggName.toLowerCase() + "(var(" + name + "))=" + source);
        }
	    
	    return aggValue;
	}

    private Collection<DatedValue> getDatedValues(StatusValue status, Instant time, Class<? extends Value> inputType) throws ComputationException {
        Collection<DatedValue> values = new LinkedList<>();
        
	    if(status == null)
            return values;
	    
        if(aggregateSelectAtt == null && status instanceof ValueHistory.Status) {
            ValueHistory history = ((ValueHistory.Status) status).history;
            
            if(expire != null)
                history.purge(expire.adjust(time));
            
            values = history.getDatedValues();
        }else if((aggregateSelectAtt != null || aggregateSelectALL) && status instanceof AggregationValues) {
            AggregationValues aggValues = ((AggregationValues) status);
            
            if(expire != null)
                aggValues.purge(expire.adjust(time));
            
            values = aggValues.getDatedValues();
        }
	    
        if(ignore != null) {
            Instant latestTime = ignore.adjust(time);
            
            values = values.stream().filter(val -> val.getTime().isBefore(latestTime)).collect(Collectors.toList());
        }
            
        return values;
    }

    public void updateVariableStatuses(VariableStatuses variableStatuses, Metric metric) {
        Optional<StatusValue> status = Optional.ofNullable(variableStatuses.get(name));
        
		metric.setAttributes(getAggSelectAttributes(metric.getAttributes()));
		
		StatusValue updatedStatus = updateStatus(status, metric);
		
		variableStatuses.put(name, updatedStatus);
	}

	private StatusValue updateStatus(Optional<StatusValue> statusOpt, Metric metric) {
	    StatusValue status = statusOpt.isPresent() ? statusOpt.get() : initStatus();
	    
	    if(aggregateSelectAtt == null && !aggregateSelectALL) {
	        if(!(status instanceof ValueHistory.Status))
	            status = initStatus();
	        
	        ValueHistory history = ((ValueHistory.Status) status).history;
	        
	        history.setGranularity(granularity);
	        history.setAggregation(aggregation);
	        history.setMax_size(max_aggregation_size);
	        history.add(metric.getTimestamp(), metric.getValue());
	    }else{
	        if(!(status instanceof AggregationValues))
                status = initStatus();
	        
	        AggregationValues aggValues = ((AggregationValues) status);
	        
	        aggValues.setMax_aggregation_size(max_aggregation_size);
	        
	        int hash = 0;
	        
	        if(aggregateSelectALL)
	            hash = metric.getAttributes().hashCode();
	        
	        if(aggregateSelectAtt != null)
	            hash = metric.getAttributes().entrySet().stream()
                                                    .filter(e -> aggregateSelectAtt.contains(e.getKey()))
                                                    .collect(Collectors.toList())
                                                    .hashCode();
	        
	        if(aggregateSelectALL || (hash != 1 && hash != 0))
	            aggValues.add(hash, metric.getValue(), metric.getTimestamp());
	    }

	    return status;        
    }

    private StatusValue initStatus() {
        if(aggregateSelectAtt == null && !aggregateSelectALL)
            return new ValueHistory.Status(max_aggregation_size, granularity, aggregation);
        else
            return new AggregationValues(max_aggregation_size);
    }

    private Map<String, String> getAggSelectAttributes(Map<String, String> attributes) {
		if(aggregateSelectAtt == null || aggregateSelectALL)
			return attributes;
		
		return attributes.entrySet().stream()
					.filter(entry -> aggregateSelectAtt.contains(entry.getKey()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

    @Override
    public Class<? extends Value> returnType() {
        return aggregation.returnType();
    }
    
    @Override
    public String toString() {
        if(aggregation instanceof LastValueAggregation)
            return "time_filter(" + name + ", from:"+expire+", to:"+ignore+")";

        String aggName = aggregation.getClass().getAnnotation(RegisterComponent.class).value();        
        return aggName + "(time_filter(" + name + ", from:"+expire+", to:"+ignore+"))";
    }

}
