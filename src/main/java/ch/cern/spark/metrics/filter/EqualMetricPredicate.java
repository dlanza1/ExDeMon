package ch.cern.spark.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import ch.cern.spark.metrics.Metric;

public class EqualMetricPredicate implements Predicate<Metric>, Serializable {

	private static final long serialVersionUID = 99926521342965096L;
	
	private String key;
	private Pattern value;

	public EqualMetricPredicate(String key, String value) throws ParseException {
		this.key = key;
		
		try {
			this.value = Pattern.compile(value);
		}catch(PatternSyntaxException e) {
			throw new ParseException(e.getDescription(), 0);
		}
	}

	@Override
	public boolean test(Metric metricInput) {
		Predicate<Metric> exist = metric -> metric.getAttributes().containsKey(key);
		Predicate<Metric> match = metric -> value.matcher(metric.getAttributes().get(key)).matches();
		
		return exist.and(match).test(metricInput);
	}
	
	@Override
	public String toString() {
		return key + " == \"" + value + "\"";
	}

}
