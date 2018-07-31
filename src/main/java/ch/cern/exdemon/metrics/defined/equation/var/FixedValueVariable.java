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
import ch.cern.spark.status.StatusValue;
import lombok.Getter;

public class FixedValueVariable extends Variable {
    
    @Getter
    private MetricsFilter filter;
    
    private Value value;

    public FixedValueVariable(String name) {
        super(name);
    }
    
    @Override
    public StatusValue updateStatus(Optional<StatusValue> statusOpt, Metric metric, Metric originalMetric) {
        Status_ status = statusOpt.filter(s -> s instanceof Status_)
                .map(s -> (Status_) s)
                .orElse(new Status_());

        status.time = metric.getTimestamp();
        
        return status;
    }
    
    @Override
    public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
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
    public Value compute(VariableStatuses stores, Instant time) {
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
    
    public static class Status_ extends StatusValue{
        private static final long serialVersionUID = -1241228510312512443L;
        
        Instant time;
    }

}
