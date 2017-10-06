package ch.cern.spark.metrics.filter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.Metric;

public class Filter implements Serializable{
    
    private static final long serialVersionUID = 9170996730102744051L;

    private static final String REGEX_PREFIX = "regex:";

    private Map<String, String> keyValues;
    
    private Map<String, Pattern> keyPatterns;
    
    public Filter(){
        this.keyValues = new HashMap<>();
        this.keyPatterns = new HashMap<>();
    }
    
    public boolean apply(Metric metric){
        for (Map.Entry<String, String> keyValue : keyValues.entrySet()){
            String expectedValue = keyValue.getValue();
            String actualValue = metric.getIDs().get(keyValue.getKey());
            
            if(actualValue == null)
                return false;
            
            if(!expectedValue.equals(actualValue))
                return false;
        }
        
        for (Map.Entry<String, Pattern> keyValue : keyPatterns.entrySet()){
            Pattern pattern = keyValue.getValue();
            String actualValue = metric.getIDs().get(keyValue.getKey());
            
            if(actualValue == null)
                return false;
            
            if(!pattern.matcher(actualValue).matches())
                return false;
        }
        
        return true;
    }
    
    public void addKeyValue(String key, String value){
        if(!value.startsWith(REGEX_PREFIX))
            keyValues.put(key, value);
        else
            addKeyPattern(key, value.replace(REGEX_PREFIX, ""));
    }
    
    private void addKeyPattern(String key, String pattern_string) {
        keyPatterns.put(key, Pattern.compile(pattern_string));
    }

    public Map<String, String> getKeyValues(){
        return keyValues;
    }

    public static Filter build(Properties props) {
        Filter filter = new Filter();
        
        Properties filterProperties = props.getSubset("attribute");
        Set<String> attributesNames = filterProperties.getUniqueKeyFields(0);
        
        for (String attributeName : attributesNames) {
            String key = "attribute." + attributeName;
            
            filter.addKeyValue(attributeName, props.getProperty(key));
        }
        
        return filter;
    }

    @Override
    public String toString() {
        return "Filter [keyValues=" + keyValues + "]";
    }

    public boolean hasRegex() {
        return !keyPatterns.isEmpty();
    }

    public Set<String> getKeys() {
        Set<String> keys = new HashSet<>();
        
        keys.addAll(keyValues.keySet());
        keys.addAll(keyPatterns.keySet());
        
        return keys;
    }

    public void setKeyValues(Map<String, String> newKeyValues) {
        this.keyValues = newKeyValues;
    }
    
}