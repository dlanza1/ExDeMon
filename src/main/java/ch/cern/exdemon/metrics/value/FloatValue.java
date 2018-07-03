package ch.cern.exdemon.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class FloatValue extends Value implements ValueComputable {

    private static final long serialVersionUID = 6026199196915653369L;

    private float num;

    public FloatValue(double value) {
        this.num = (float) value;
    }

    public FloatValue(float value) {
        this.num = value;
    }

    @Override
    public FloatValue compute(VariableStatuses store, Instant time) {
        return new FloatValue(num);
    }

    @Override
    public Optional<Float> getAsFloat() {
        return Optional.of(this.num);
    }

    public static FloatValue from(String value_string) {
        return new FloatValue(Float.parseFloat(value_string));
    }

    @Override
    public Class<FloatValue> returnType() {
        return FloatValue.class;
    }

    @Override
    public String toString() {
        return Float.toString(num);
    }

    @Override
    public String getSource() {
        if (source == null)
            return toString();
        else
            return super.toString();
    }

}
