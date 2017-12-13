package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;

public class AnyMetricVariable extends MetricVariable {

    public AnyMetricVariable(String name) {
        super(name);
    }

    @Override
    public Value compute(MetricVariableStatus store, Instant time) {
        Optional<Instant> oldestMetricAt = Optional.empty();
        if(expire != null)
            oldestMetricAt = Optional.of(expire.adjust(time));
        store.purge(name, oldestMetricAt);
        
        Optional<Instant> newestMetricAt = Optional.empty();
        if(ignore != null)
            newestMetricAt = Optional.of(ignore.adjust(time));

        Value val = store.getValue(oldestMetricAt, newestMetricAt);

        String source = val.toString();
        if (val.getAsException().isPresent())
            val = new ExceptionValue("Variable " + name + ": " + val.getAsException().get());

        val.setSource("var(" + name + ")=" + source);

        return val;
    }

    @Override
    public void updateStore(MetricVariableStatus store, Metric metric) {
        store.add(metric.getValue(), metric.getInstant());
    }

    @Override
    public Class<Value> returnType() {
        return Value.class;
    }

    @Override
    public String toString() {
        return name;
    }

}
