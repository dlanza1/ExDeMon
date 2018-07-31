package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import lombok.Getter;

public class FixedValueVariable extends Variable {
    
    @Getter
    private MetricsFilter filter;
    
    private Value value;

    public FixedValueVariable(String name) {
        super(name);
    }
    
    @Override
    public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
        ConfigurationResult confResult = super.config(properties, typeOpt);
        
        try {
            filter = MetricsFilter.build(properties.getSubset("filter"));
        } catch (ConfigurationException e) {
            confResult.withError("filter", e);
        }
        
        String valueAsString = properties.getProperty("fixed.value");
        if(valueAsString == null)
            confResult.withMustBeConfigured("fixed.value");
        
        value = new StringValue(valueAsString);
        
        return confResult;
    }

    @Override
    public Value compute(Optional<VariableStatus> storeOpt, Instant time) {
        return value;
    }

    @Override
    public Class<? extends Value> returnType() {
        return StringValue.class;
    }

    @Override
    public boolean test(Metric metric) {
        return filter.test(metric);
    }
    
}
