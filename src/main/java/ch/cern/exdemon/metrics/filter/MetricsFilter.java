package ch.cern.exdemon.metrics.filter;

import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.util.function.AndPredicate;
import ch.cern.util.function.OrPredicate;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class MetricsFilter implements Predicate<Metric>, Serializable {

    private static final long serialVersionUID = 9170996730102744051L;

    @Getter
    @Setter
    private Duration timestampExpire = null;

    private Predicate<Map<String, String>> attributesPredicate = null;

    public MetricsFilter() {
    }
    
    public ConfigurationResult config(Properties props){
        ConfigurationResult configResult = ConfigurationResult.SUCCESSFUL();
        
        String expression = props.getProperty("expr");
        if (expression != null)
            try {
                addAttributesPredicate(AttributesPredicateParser.parse(expression));
            } catch (ParseException e) {
                configResult.withError("expr", "Error when parsing filter expression: " + e.getMessage());
            }

        Optional<Duration> timestampExpireOpt;
        try {
            timestampExpireOpt = props.getPeriod("timestamp.expire");
            
            if (timestampExpireOpt.isPresent())
                setTimestampExpire(timestampExpireOpt.get());
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }

        Properties filterProperties = props.getSubset("attribute");

        for (Entry<Object, Object> attribute : filterProperties.entrySet()) {
            String key = (String) attribute.getKey();
            String valueString = (String) attribute.getValue();

            List<String> values = getValues(valueString);
            if (values.size() == 0) {
                try {
                    addAttributesPredicate(key, valueString);
                } catch (ParseException e) {
                    configResult.withError("attribute." + key, "Error when parsing filter value expression (" + valueString + "): " + e.getMessage());
                }
            } else {
                boolean negate = valueString.startsWith("!");

                try {
                    if (negate)
                        for (String value : values)
                            addAttributesPredicate(new NotEqualMetricPredicate(key, value));
                    else {
                        Predicate<Map<String, String>> orOptions = null;
    
                        for (String value : values)
                            if (orOptions == null)
                                orOptions = new EqualMetricPredicate(key, value);
                            else
                                orOptions = new OrPredicate<Map<String, String>>(orOptions, new EqualMetricPredicate(key, value));
    
                        addAttributesPredicate(orOptions);
                    }
                } catch (ParseException e) {
                    configResult.withError("attribute." + key, "Error when parsing values (" + valueString + "): " + e.getMessage());
                }
            }
        }

        return configResult.merge(null, props.warningsIfNotAllPropertiesUsed());
    }

    @Override
    public boolean test(Metric metric) {
        if (attributesPredicate != null && !attributesPredicate.test(metric.getAttributes()))
            return false;

        if (timestampExpire != null && !metric.getTimestamp().isAfter(Instant.now().minus(timestampExpire)))
            return false;

        return true;
    }

    public void addAttributesPredicate(String key, String value) throws ParseException {
        if (value.charAt(0) == '!')
            addAttributesPredicate(new NotEqualMetricPredicate(key, value.substring(1)));
        else
            addAttributesPredicate(new EqualMetricPredicate(key, value));
    }

    public void addAttributesPredicate(Predicate<Map<String, String>> newPredicate) {
        if (attributesPredicate == null)
            attributesPredicate = newPredicate;
        else
            attributesPredicate = new AndPredicate<Map<String, String>>(attributesPredicate, newPredicate);
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
        if (attributesPredicate == null)
            return true;

        return attributesPredicate.test(attributes);
    }
    
    public Set<String> getAttributesValuesWithEqualsForKey(String key){
        HashSet<String> values = new HashSet<>();
        
        if(attributesPredicate == null)
            return values;
        
        List<Predicate<Map<String, String>>> predicates = new LinkedList<>();
        predicates.add(attributesPredicate);
        
        while(!predicates.isEmpty()) {
            Predicate<Map<String, String>> predicate = predicates.remove(0);
         
            if(predicate instanceof AndPredicate) {
                AndPredicate<Map<String, String>> andPredicate = (AndPredicate<Map<String, String>>) predicate;
                
                predicates.add(andPredicate.getPred1());
                predicates.add(andPredicate.getPred2());
            }else if(predicate instanceof OrPredicate) {
                OrPredicate<Map<String, String>> orPredicate = (OrPredicate<Map<String, String>>) predicate;
                
                predicates.add(orPredicate.getPred1());
                predicates.add(orPredicate.getPred2());
            }else if(predicate instanceof EqualMetricPredicate) {
                EqualMetricPredicate equalPredicate = (EqualMetricPredicate) predicate;
                
                if(equalPredicate.getKey().equals(key))
                    values.add(equalPredicate.getValue().toString());
            }
        }
        
        return values;
    }
    
    public Set<String> getFilteredAttributes(){
        HashSet<String> values = new HashSet<>();
        
        if(attributesPredicate == null)
            return values;
        
        List<Predicate<Map<String, String>>> predicates = new LinkedList<>();
        predicates.add(attributesPredicate);
        
        while(!predicates.isEmpty()) {
            Predicate<Map<String, String>> predicate = predicates.remove(0);
         
            if(predicate instanceof AndPredicate) {
                AndPredicate<Map<String, String>> andPredicate = (AndPredicate<Map<String, String>>) predicate;
                
                predicates.add(andPredicate.getPred1());
                predicates.add(andPredicate.getPred2());
            }else if(predicate instanceof OrPredicate) {
                OrPredicate<Map<String, String>> orPredicate = (OrPredicate<Map<String, String>>) predicate;
                
                predicates.add(orPredicate.getPred1());
                predicates.add(orPredicate.getPred2());
            }else if(predicate instanceof EqualMetricPredicate) {
                EqualMetricPredicate equalPredicate = (EqualMetricPredicate) predicate;
                
                values.add(equalPredicate.getKey());
            }else if(predicate instanceof NotEqualMetricPredicate) {
                NotEqualMetricPredicate notEqualPredicate = (NotEqualMetricPredicate) predicate;
                
                values.add(notEqualPredicate.getKey());
            }
        }
        
        return values;
    }

}