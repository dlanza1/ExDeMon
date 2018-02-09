package ch.cern.spark.metrics.schema;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.google.gson.JsonElement;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSON;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.utils.Pair;
import lombok.Getter;
import lombok.ToString;

@ToString
public class MetricSchema implements Serializable {

    private static final long serialVersionUID = -8885058791228553794L;

    private transient final static Logger LOG = Logger.getLogger(MetricSchema.class.getName());

    @Getter
    private String id;

    public static String SOURCES_PARAM = "sources";
    private List<String> sources;

    protected HashMap<String, String> fixedAttributes;
    
    public static String ATTRIBUTES_PARAM = "attributes";
    protected List<Pair<String, String>> attributes;
    protected List<Pair<String, Pattern>> attributesPattern;

    public static String VALUES_PARAM = "value";
    protected List<ValueDescriptor> values;

    public static String TIMESTAMP_FORMAT_PARAM = "timestamp.format";
    public static String TIMESTAMP_FORMAT_DEFAULT = "auto";
    public transient DateTimeFormatter format_auto;
    protected String timestamp_format_pattern;
    protected transient DateTimeFormatter timestamp_format;

    public static String TIMESTAMP_ATTRIBUTE_PARAM = "timestamp.key";
    protected String timestamp_attribute;

    public static String FILTER_PARAM = "filter";
    protected MetricsFilter filter;

    protected Exception configurationException;    

    public MetricSchema(String id) {
        this.id = id;
    }

    public MetricSchema config(Properties properties) {
        try {
            tryConfig(properties);
        } catch (Exception e) {
            configurationException = e;
            
            LOG.error(id + ": " + e.getMessage(), e);
        }

        return this;
    }

    public MetricSchema tryConfig(Properties properties) throws ConfigurationException {
        String sourcesValue = properties.getProperty(SOURCES_PARAM);
        if (sourcesValue == null)
            throw new ConfigurationException("sources must be spcified");
        sources = Arrays.asList(sourcesValue.split("\\s"));

        timestamp_attribute = properties.getProperty(TIMESTAMP_ATTRIBUTE_PARAM);

        timestamp_format_pattern = properties.getProperty(TIMESTAMP_FORMAT_PARAM, TIMESTAMP_FORMAT_DEFAULT);
        if (!timestamp_format_pattern.equals("epoch-ms")
                && !timestamp_format_pattern.equals("epoch-s")
                && !timestamp_format_pattern.equals("auto"))
            try {
                new DateTimeFormatterBuilder().appendPattern(timestamp_format_pattern).toFormatter()
                        .withZone(ZoneOffset.systemDefault());
            } catch (Exception e) {
                throw new ConfigurationException(TIMESTAMP_FORMAT_PARAM
                        + " must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.");
            }

        values = new LinkedList<>();
        Properties valuesProps = properties.getSubset(VALUES_PARAM);
        
        //TODO FOR RETROCOMPATIBILITY
        for (Map.Entry<Object, Object> pair : valuesProps.getSubset("keys").entrySet()) {
            String alias = (String) pair.getKey();
            String key = (String) pair.getValue();
            
            ValueDescriptor descriptor = new ValueDescriptor(alias);
            Properties props = new Properties();
            props.setProperty(ValueDescriptor.KEY_PARAM, key);
            descriptor.config(props);

            values.add(descriptor);
        }
        valuesProps.entrySet().removeIf(entry -> ((String) entry.getKey()).startsWith("keys"));
        // FOR RETROCOMPATIBILITY END
        
        Set<String> valueIDs = valuesProps.getIDs();
        for (String valueId : valueIDs) {
            ValueDescriptor descriptor = new ValueDescriptor(valueId);
            descriptor.config(valuesProps.getSubset(valueId));
            
            values.add(descriptor);
        }
        if (values.isEmpty())
            throw new ConfigurationException(VALUES_PARAM + " must be configured.");

        fixedAttributes = new HashMap<>();
        fixedAttributes.put("$schema", id);
        
        attributes = new LinkedList<>();
        attributesPattern = new LinkedList<>();
        String attributesValue = properties.getProperty(ATTRIBUTES_PARAM);
        if (attributesValue != null) {
            String[] attributesValues = attributesValue.split("\\s");

            for (String attribute : attributesValues)
                if(!isKeyRegex(attribute))
                    attributes.add(new Pair<String, String>(attribute, attribute));
                else
                    attributesPattern.add(new Pair<String, Pattern>(attribute, Pattern.compile(attribute)));
        }
        Properties attributesWithAlias = properties.getSubset(ATTRIBUTES_PARAM);
        for (Map.Entry<Object, Object> pair : attributesWithAlias.entrySet()) {
            String alias = (String) pair.getKey();
            String key = (String) pair.getValue();
            
            if(isFixedValue(key))
                fixedAttributes.put(alias, key.substring(1));
            else if(!isKeyRegex(key))
                attributes.add(new Pair<String, String>(alias, key));
            else
                attributesPattern.add(new Pair<String, Pattern>(alias, Pattern.compile(key)));
        }

        filter = MetricsFilter.build(properties.getSubset(FILTER_PARAM));
        
        properties.confirmAllPropertiesUsed();

        return this;
    }

    private boolean isKeyRegex(String value) {
        return value.contains("*") || value.contains("+") || value.contains("(") || value.contains("*");
    }

    public List<Metric> call(String jsonString) {
        if (configurationException != null) {
            ExceptionValue exception = new ExceptionValue(configurationException.getMessage());

            return Arrays.asList(new Metric(Instant.now(), exception, fixedAttributes));
        }
        
        JSON jsonObject = new JSON(jsonString);
        
        try {
            Map<String, String> attributesForMetric = new HashMap<>(fixedAttributes);
            for (Pair<String, String> attribute : attributes) {
                String alias = attribute.first;
                String key = attribute.second;

                JsonElement value = jsonObject.getElement(key);

                if (value != null && value.isJsonPrimitive())
                    attributesForMetric.put(alias, value.getAsString());
            }
            for (Pair<String, Pattern> attribute : attributesPattern) {
                String alias = attribute.first;
                Pattern keyPattern = attribute.second;

                String[] keys = jsonObject.getKeys(keyPattern);
                
                for (String key : keys) {
                    String finalAlias = key;
                    if(alias.contains("+") && !alias.equals(keyPattern.pattern())) {
                        Matcher matcher = attribute.second.matcher(key);
                        
                        if(matcher.find() && matcher.groupCount() == 1)
                            finalAlias = alias.replace("+", matcher.group(1));
                    }
                    
                    JsonElement value = jsonObject.getElement(key);
                    
                    if (value != null && value.isJsonPrimitive())
                        attributesForMetric.put(finalAlias, value.getAsString());
                }
            }
            
            if(!filter.test(attributesForMetric))
                return Collections.emptyList();

            Exception timestampException = null;
            Instant timestamp;
            if(timestamp_attribute != null) {
                String timestamp_string = jsonObject.getProperty(timestamp_attribute);
                
                try {
                    timestamp = toDate(timestamp_string);
                } catch (Exception e) {
                    timestampException = new Exception("DateTimeParseException: " + e.getMessage() + " for key "
                            + timestamp_attribute + " with value (" + timestamp_string + ")");

                    timestamp = Instant.now();
                }
            }else {
                timestamp = Instant.now();
            }
            
            List<Metric> metrics = new LinkedList<>();

            for (ValueDescriptor valueDescriptor : values) {
                String id = valueDescriptor.getId();

                Map<String, String> metric_ids = new HashMap<>(attributesForMetric);
                metric_ids.put("$value", id);
             
                //TODO FOR RETROCOMPATIBILITY
                metric_ids.put("$value_attribute", id); 

                if (timestampException != null) {
                    metrics.add(new Metric(timestamp, new ExceptionValue(timestampException.getMessage()), metric_ids));
                    continue;
                }
                
                Optional<Value> value = valueDescriptor.extract(jsonObject);
                
                if(value.isPresent())
                    metrics.add(new Metric(timestamp, value.get(), metric_ids));
            }

            return metrics.stream().filter(filter).collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            
            ExceptionValue exception = new ExceptionValue(e.getMessage());

            return Arrays.asList(new Metric(Instant.now(), exception, fixedAttributes));
        }
    }

    private boolean isFixedValue(String key) {
        return key.startsWith("#");
    }

    private Instant toDate(String date_string) throws DateTimeParseException {
        if(format_auto == null)
            format_auto = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][Z]").toFormatter();
        
        if (date_string == null || date_string.length() == 0)
            throw new DateTimeParseException("No data to parse", "", 0);

        try {
            if (timestamp_format_pattern.equals("auto")) {
                try {
                    long value = Long.valueOf(date_string);
                    
                    if(value < Math.pow(10, 10))
                        return Instant.ofEpochSecond(value);
                    else
                        return Instant.ofEpochMilli(value);
                }catch(Exception e) {}

                try {
                    TemporalAccessor temporalAccesor = format_auto.parse(date_string.replace(" ", "T").replace("Z", ""));
                    
                    if (temporalAccesor.isSupported(ChronoField.INSTANT_SECONDS))
                        return Instant.from(temporalAccesor);
                    else
                        return LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
                }catch(Exception e) {}
                
                throw new DateTimeParseException("Automatic format could not parse time", "", 0);
            }
                
            if (timestamp_format_pattern.equals("epoch-ms"))
                return Instant.ofEpochMilli(Long.valueOf(date_string));

            if (timestamp_format_pattern.equals("epoch-s"))
                return Instant.ofEpochSecond(Long.valueOf(date_string));
        } catch (Exception e) {
            throw new DateTimeParseException(e.getClass().getName() + ": " + e.getMessage(), date_string, 0);
        }

        if (timestamp_format == null)
            timestamp_format = new DateTimeFormatterBuilder()
                                            .appendPattern(timestamp_format_pattern)
                                            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                            .toFormatter();
        
        TemporalAccessor temporalAccesor = timestamp_format.parse(date_string);
        
        if (temporalAccesor.isSupported(ChronoField.INSTANT_SECONDS))
            return Instant.from(temporalAccesor);
        else
            return LocalTime.from(temporalAccesor).atOffset(OffsetDateTime.now().getOffset()).atDate(LocalDate.from(temporalAccesor)).toInstant();
    }

    public boolean containsSource(String sourceID) {
        return sources.contains(sourceID);
    }

}
