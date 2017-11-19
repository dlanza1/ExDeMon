package ch.cern.spark.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Map.Entry;
import java.util.function.Predicate;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.util.function.AndPredicate;

public class MetricsFilter implements Predicate<Metric>, Serializable{
    
    private static final long serialVersionUID = 9170996730102744051L;

    private Predicate<Metric> predicate = null;
    
    public MetricsFilter(){
    }
    
    @Override
	public boolean test(Metric metric) {
    		if(predicate == null)
    			return true;
    		
		return predicate.test(metric);
	}
    
    public void addPredicate(String key, String value) throws ParseException{
    		if(value.charAt(0) == '!')  		
        		addPredicate(new NotEqualMetricPredicate(key, value.substring(1)));
    		else
        		addPredicate(new EqualMetricPredicate(key, value));
    }

    private void addPredicate(Predicate<Metric> newPredicate) {
    		if(predicate == null)
    			predicate = newPredicate;
    		else
    			predicate = new AndPredicate<Metric>(predicate, newPredicate);
	}

	public static MetricsFilter build(Properties props) throws ConfigurationException {
		MetricsFilter filter = new MetricsFilter();
        
        String expression = props.getProperty("expr");
        if(expression != null)
			try {
				filter.addPredicate(MetricPredicateParser.parse(expression));
			} catch (ParseException e) {
				throw new ConfigurationException("Error when parsing filter expression: " + e.getMessage());
			}
        
        Properties filterProperties = props.getSubset("attribute");
        
        for (Entry<Object, Object> attribute : filterProperties.entrySet()) {
            String key = (String) attribute.getKey();
            String value = (String) attribute.getValue();
            
			try {
				filter.addPredicate(key, value);
			} catch (ParseException e) {
				throw new ConfigurationException("Error when parsing filter (" + key + ") value expression (" + value + "): " + e.getMessage());
			}
        }
        
		try {
			props.confirmAllPropertiesUsed();
		} catch (ConfigurationException e) {
			throw new ConfigurationException("Error when parsing filter: " + e.getMessage());
		}
        
        return filter;
    }
    
	@Override
    public String toString() {
        return "Filter [predicate=" + predicate + "]";
    }
    
}