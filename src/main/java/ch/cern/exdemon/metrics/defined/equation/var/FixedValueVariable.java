package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.Properties;
import lombok.Getter;

public class FixedValueVariable extends Variable {
    
    /*
     * By adding a second, FixedValueVariable variables will be taken when merging in case of missing value
     */
    private static final Instant DURATION_WHEN_NO_STATUS = Instant.EPOCH.plus(Duration.ofSeconds(1));

    @Getter
    private MetricsFilter filter;
    
    private Value value;

    public FixedValueVariable(String name, Map<String, Variable> variables, Properties variablesProperties) {
        super(name, variables, variablesProperties);
    }
    
    @Override
    public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
        ConfigurationResult confResult = super.config(properties, typeOpt);
        
        filter = new MetricsFilter();
        confResult.merge("filter", filter.config(properties.getSubset("filter")));
        
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
    
    @Override
    protected Instant getLastUpdateMetricTimeWhenNoStatus() {
        return DURATION_WHEN_NO_STATUS;
    }
    
}
