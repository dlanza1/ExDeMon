package ch.cern.exdemon.metrics.value;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;
import lombok.Getter;
import lombok.Setter;

public abstract class Value implements Serializable, Cloneable {

    private static final long serialVersionUID = -5082571575744839753L;

    @Getter @Setter
    protected String source;
    
    @Setter
    protected List<Metric> lastSourceMetrics;

    public Optional<Float> getAsFloat() {
        return Optional.empty();
    }

    public Optional<String> getAsString() {
        return Optional.empty();
    }

    public Optional<Boolean> getAsBoolean() {
        return Optional.empty();
    }

    public Optional<String> getAsException() {
        return Optional.empty();
    }

    public Optional<Value> getAsAggregated() {
        return Optional.empty();
    }

    public Optional<Properties> getAsProperties() {
        return Optional.empty();
    }

    public Optional<AnalysisResult> getAsAnalysisResult() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return source;
    }
    
    public List<Metric> getLastSourceMetrics() {
        if(lastSourceMetrics == null || lastSourceMetrics.isEmpty())
            return null;
        
        return lastSourceMetrics;
    }

    @Override
    public Value clone() {
        if (getAsFloat().isPresent())
            return new FloatValue(getAsFloat().get());
        if (getAsString().isPresent())
            return new StringValue(getAsString().get());
        if (getAsBoolean().isPresent())
            return new BooleanValue(getAsBoolean().get());
        if (getAsAggregated().isPresent())
            return new AggregatedValue(getAsAggregated().get());
        if (getAsException().isPresent())
            return new ExceptionValue(getAsException().get());
        if (getAsProperties().isPresent()) {
            PropertiesValue valueRef = (PropertiesValue) this;

            return new PropertiesValue(valueRef.getName(), new Properties(valueRef.getAsProperties().get()));
        }

        throw new RuntimeException("Value is not any of the expected types");
    }

}
