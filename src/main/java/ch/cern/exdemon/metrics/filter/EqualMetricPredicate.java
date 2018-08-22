package ch.cern.exdemon.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import lombok.Getter;

public class EqualMetricPredicate implements Predicate<Map<String, String>>, Serializable {

	private static final long serialVersionUID = 99926521342965096L;
	
	@Getter
	private String key;
	
	@Getter
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
	public boolean test(Map<String, String> attributes) {
		Predicate<Map<String, String>> exist = metric -> attributes.containsKey(key);
		Predicate<Map<String, String>> match = metric -> value.matcher(attributes.get(key)).matches();
		
		return exist.and(match).test(attributes);
	}
	
	@Override
	public String toString() {
		return key + " == \"" + value + "\"";
	}

}
