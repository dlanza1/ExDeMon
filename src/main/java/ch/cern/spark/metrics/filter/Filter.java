package ch.cern.spark.metrics.filter;

import java.io.Serializable;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.Metric;

public class Filter implements Predicate<Metric>, Serializable{
    
    private static final long serialVersionUID = 9170996730102744051L;

    private static final String REGEX_PREFIX = "regex:";

    private Predicate<Metric> predicate = (m -> true);
    
    public Filter(){
    }
    
    @Override
	public boolean test(Metric metric) {
		return predicate.test(metric);
	}
    
    public void addPredicate(String key, String value){
		if(value.startsWith(REGEX_PREFIX)) {
        		Pattern pattern = Pattern.compile(value.replace(REGEX_PREFIX, ""));
        		predicate = predicate
        				.and(m -> m.getIDs().containsKey(key))
        				.and(m -> pattern.matcher(m.getIDs().get(key)).matches());
		}else{			
        		predicate = predicate
        				.and(m -> m.getIDs().containsKey(key))
        				.and(m -> m.getIDs().get(key).equals(value));
		}
    }

    public static Filter build(Properties props) {
        Filter filter = new Filter();
        
        Properties filterProperties = props.getSubset("attribute");
        Set<String> attributesNames = filterProperties.getUniqueKeyFields(0);
        
        for (String attributeName : attributesNames) {
            String key = "attribute." + attributeName;
            
            filter.addPredicate(attributeName, props.getProperty(key));
        }
        
        return filter;
    }

    @Override
    public String toString() {
        return "Filter [predicate=" + predicate + "]";
    }
    
}