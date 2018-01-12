package ch.cern.spark.metrics.value;

import java.io.Serializable;
import java.util.Optional;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.results.AnalysisResult;

public abstract class Value implements Serializable {

    private static final long serialVersionUID = -5082571575744839753L;

    protected String source;

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

    public void setSource(String source) {
        this.source = source;
    }

    public String getSource() {
        return this.source;
    }

    @Override
    public String toString() {
        return source;
    }

    public static Value clone(Value value) {
        if (value.getAsFloat().isPresent())
            return new FloatValue(value.getAsFloat().get());
        if (value.getAsString().isPresent())
            return new StringValue(value.getAsString().get());
        if (value.getAsBoolean().isPresent())
            return new BooleanValue(value.getAsBoolean().get());
        if (value.getAsAggregated().isPresent())
            return new AggregatedValue(value.getAsAggregated().get());
        if (value.getAsException().isPresent())
            return new ExceptionValue(value.getAsException().get());
        if (value.getAsProperties().isPresent()) {
            PropertiesValue valueRef = (PropertiesValue) value;

            return new PropertiesValue(valueRef.getName(), new Properties(valueRef.getAsProperties().get()));
        }

        throw new RuntimeException("Value is not any of the expected types");
    }

}
