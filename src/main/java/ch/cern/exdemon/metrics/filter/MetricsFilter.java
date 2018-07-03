package ch.cern.exdemon.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.util.function.AndPredicate;
import ch.cern.util.function.OrPredicate;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class MetricsFilter implements Predicate<Metric>, Serializable{
    
    private static final long serialVersionUID = 9170996730102744051L;

    @Getter @Setter
    private Duration timestampExpire = null;

    private Predicate<Map<String, String>> attributesPredicate = null;
    
    public MetricsFilter(){
    }
    
    @Override
	public boolean test(Metric metric) {
        if(attributesPredicate != null
                && !attributesPredicate.test(metric.getAttributes()))
               return false;
        
        if(timestampExpire != null
                && !metric.getTimestamp().isAfter(Instant.now().minus(timestampExpire)))
               return false;
    		
		return true;
	}
    
    public void addAttributesPredicate(String key, String value) throws ParseException{
        if(value.charAt(0) == '!')  		
            addAttributesPredicate(new NotEqualMetricPredicate(key, value.substring(1)));
        else
            addAttributesPredicate(new EqualMetricPredicate(key, value));
    }

    public void addAttributesPredicate(Predicate<Map<String, String>> newPredicate) {
        if(attributesPredicate == null)
            attributesPredicate = newPredicate;
        else
            attributesPredicate = new AndPredicate<Map<String, String>>(attributesPredicate, newPredicate);
	}

	public static MetricsFilter build(Properties props) throws ConfigurationException {
		MetricsFilter filter = new MetricsFilter();
        
        String expression = props.getProperty("expr");
        if(expression != null)
			try {
				filter.addAttributesPredicate(AttributesPredicateParser.parse(expression));
			} catch (ParseException e) {
				throw new ConfigurationException("Error when parsing filter expression: " + e.getMessage());
			}
        
        Optional<Duration> timestampExpireOpt = props.getPeriod("timestamp.expire");
        if(timestampExpireOpt.isPresent())
            filter.setTimestampExpire(timestampExpireOpt.get());
        
        Properties filterProperties = props.getSubset("attribute");
        
        try {
            for (Entry<Object, Object> attribute : filterProperties.entrySet()) {
                String key = (String) attribute.getKey();
                String valueString = (String) attribute.getValue();
                
                List<String> values = getValues(valueString);
                if(values.size() == 0) {
            			try {
            				filter.addAttributesPredicate(key, valueString);
            			} catch (ParseException e) {
            				throw new ConfigurationException("Error when parsing filter (" + key + ") value expression (" + valueString + "): " + e.getMessage());
            			}
                }else {
                    boolean negate = valueString.startsWith("!");
                    
                    if(negate)
                        for (String value : values)
                            filter.addAttributesPredicate(new NotEqualMetricPredicate(key, value));
                    else {
                        Predicate<Map<String, String>> orOptions = null;
                        
                        for (String value : values)
                            if(orOptions  == null)
                                orOptions = new EqualMetricPredicate(key, value);
                            else
                                orOptions = new OrPredicate<Map<String, String>>(orOptions, new EqualMetricPredicate(key, value));
                        
                        filter.addAttributesPredicate(orOptions);
                    }
                }
            }
        
			props.confirmAllPropertiesUsed();
		} catch (ConfigurationException|ParseException e) {
		    throw new ConfigurationException("Error when parsing filter: " + e.getMessage());
        }
        
        return filter;
    }

    private static List<String> getValues(String valueString) {
	    Pattern pattern = Pattern.compile("([\"'])(?:(?=(\\\\?))\\2.)*?\\1");
	    
	    LinkedList<String> hits = new LinkedList<>();
	    
        Matcher m = pattern.matcher(valueString);
        while (m.find()) {
            String value = m.group();
            
            value = value.substring(1, value.length() - 1);
            
            hits.add(value);
        }
	    
        return hits;
    }

    public boolean test(Map<String, String> attributes) {
        if(attributesPredicate == null)
            return true;
        
        return attributesPredicate.test(attributes);
    }
    
}