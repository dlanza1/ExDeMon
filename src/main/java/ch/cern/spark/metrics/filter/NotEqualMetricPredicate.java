package ch.cern.spark.metrics.filter;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import ch.cern.spark.metrics.Metric;

public class NotEqualMetricPredicate implements Predicate<Metric> {

	private String key;
	private Pattern value;

	public NotEqualMetricPredicate(String key, String value) {
		this.key = key;
		this.value = Pattern.compile(value);
	}

	@Override
	public boolean test(Metric metricInput) {
		Predicate<Metric> notExist = metric -> !metric.getIDs().containsKey(key);
		Predicate<Metric> notMatch = metric -> !value.matcher(metric.getIDs().get(key)).matches();

		return notExist.or(notMatch).test(metricInput);
	}
	
	@Override
	public String toString() {
		return key + " != \"" + value + "\"";
	}

}
