package ch.cern.spark.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import ch.cern.spark.metrics.Metric;

public class NotEqualMetricPredicate implements Predicate<Metric>, Serializable {

    private static final long serialVersionUID = -1044577733678850309L;

    private String key;
    private Pattern value;

    public NotEqualMetricPredicate(String key, String value) throws ParseException {
        this.key = key;
        
        try {
            this.value = Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw new ParseException(e.getDescription(), 0);
        }
    }

    @Override
    public boolean test(Metric metricInput) {
        Predicate<Metric> notExist = metric -> !metric.getAttributes().containsKey(key);
        Predicate<Metric> notMatch = metric -> !value.matcher(metric.getAttributes().get(key)).matches();

        return notExist.or(notMatch).test(metricInput);
    }

    @Override
    public String toString() {
        return key + " != \"" + value + "\"";
    }

}
