package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.Properties;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper=true)
public class AttributeVariable extends Variable {
    
    @Getter
    private MetricsFilter filter;
    
    private String attribute;

    public AttributeVariable(String name, Map<String, Variable> variables, Properties variablesProperties) {
        super(name, variables, variablesProperties);
    }
    
    @Override
    public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
        ConfigurationResult confResult = super.config(properties, typeOpt);
        
        filter = new MetricsFilter();
        confResult.merge("filter", filter.config(properties.getSubset("filter")));
        
        attribute = properties.getProperty("attribute");
        if(attribute == null)
            confResult.withMustBeConfigured("attribute");
        
        return confResult;
    }

    @Override
    public VariableStatus updateStatus(VariableStatus varStatus, Metric metric, Metric originalMetric) {
        Status_ status = null;
        if(varStatus instanceof Status_) {
            status = (Status_) varStatus;
        
            status.value = metric.getAttributes().get(attribute);
            
            return status;
        }
        
        return null;
    }
    
    @Override
    protected VariableStatus initStatus() {
        return new Status_();
    }
    
    @Override
    public Value compute(Optional<VariableStatus> statusOpt, Instant time) {
        if(!statusOpt.isPresent())
            return new ExceptionValue("no store");
        
        VariableStatus statusValue = statusOpt.get();
        
        if(statusValue instanceof Status_) {
            Status_ status = (Status_) statusValue; 
            
            if(status.value != null)
                return new StringValue(status.value);
            else
                return new ExceptionValue("null value");
        }
        
        return new ExceptionValue("no store of proper type");
    }

    @Override
    public Class<? extends Value> returnType() {
        return StringValue.class;
    }

    @Override
    public boolean test(Metric metric) {
        return filter.test(metric);
    }
    
    @ClassNameAlias("attribute-variable-status")
    @EqualsAndHashCode(callSuper=true)
    @ToString
    public static class Status_ extends VariableStatus{
        private static final long serialVersionUID = -1241228510312512443L;
        
        String value;
    }

}
