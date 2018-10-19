package ch.cern.exdemon.metrics;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper = false)
public class Metric implements Serializable {

    private static final long serialVersionUID = -182236104179624396L;

    @Getter
    @NonNull
    private Map<String, String> attributes;

    @Getter
    @NonNull
    private Instant timestamp;

    @Getter
    @NonNull
    private Value value;

    public Metric(Instant timestamp, float value, Map<String, String> attributes) {
        this(timestamp, new FloatValue(value), attributes);
    }

    public Metric(@NonNull Instant timestamp, @NonNull Value value, Map<String, String> attributes) {
        if (attributes == null)
            this.attributes = new HashMap<String, String>();
        else
            this.attributes = new HashMap<String, String>(attributes);

        this.timestamp = timestamp;
        this.value = value;
    }

}
