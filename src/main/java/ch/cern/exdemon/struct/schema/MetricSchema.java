package ch.cern.exdemon.struct.schema;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.unix_timestamp;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.struct.Metric;
import ch.cern.exdemon.struct.filter.MetricsFilter;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.utils.StructTypeUtils;
import ch.cern.utils.Pair;

public class MetricSchema {
    
    private transient final static Logger LOG = Logger.getLogger(MetricSchema.class.getName());
    
    private String id;

    public static String SOURCES_PARAM = "sources";
    private List<String> sourceNames;

    public static String ATTRIBUTES_PARAM = "attributes";
    private List<Pair<String, String>> attributes;
    
    private HashMap<String, String> fixedAttributes;

    public static String VALUES_PARAM = "values";
    private List<ValueDescriptor> values;

    public static String TIMESTAMP_KEY_PARAM = "timestamp.key";
    protected String timestamp_key;
    public static String TIMESTAMP_FORMAT_PARAM = "timestamp.format";
    public static String TIMESTAMP_FORMAT_DEFAULT = "epoch-ms";
    private String timestamp_format_pattern;

    private StructType jsonSchema;
    
    public static String FILTER_PARAM = "filter";
    private MetricsFilter filter;
    
    public MetricSchema(String id) {
        this.id = id;
    }
    
    public ConfigurationResult config(Properties properties){
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        String sourcesValue = properties.getProperty(SOURCES_PARAM);
        if (sourcesValue == null)
            confResult.withMustBeConfigured(SOURCES_PARAM);
        else
            sourceNames = Arrays.asList(sourcesValue.split("\\s"));

        timestamp_key = properties.getProperty(TIMESTAMP_KEY_PARAM);
        timestamp_format_pattern = properties.getProperty(TIMESTAMP_FORMAT_PARAM, TIMESTAMP_FORMAT_DEFAULT);

        values = new LinkedList<>();
        Properties valuesProps = properties.getSubset(VALUES_PARAM);
        
        Set<String> valueIDs = valuesProps.getIDs();
        for (String valueId : valueIDs) {
            ValueDescriptor descriptor = new ValueDescriptor(valueId);
            confResult.merge(VALUES_PARAM + valueId, descriptor.config(valuesProps.getSubset(valueId)));
            
            values.add(descriptor);
        }
        if (values.isEmpty())
            confResult.withMustBeConfigured(VALUES_PARAM);

        fixedAttributes = new HashMap<>();
        fixedAttributes.put("$schema", id);
        
        attributes = new LinkedList<>();
        Properties attributesWithAlias = properties.getSubset(ATTRIBUTES_PARAM);
        for (Map.Entry<Object, Object> pair : attributesWithAlias.entrySet()) {
            String alias = (String) pair.getKey();
            String key = (String) pair.getValue();
            
            attributes.add(new Pair<String, String>(alias, key));
        }
        
        filter = new MetricsFilter();
        filter.config(properties.getSubset(FILTER_PARAM));
        
        try {
            jsonSchema = getJsonSchema();
        } catch (ParseException e) {
            confResult.withError(null, e);
        }
        
        return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    public Optional<Dataset<Metric>> apply(Map<String, Dataset<String>> sourcesMap) throws ConfigurationException{
        
        Dataset<Metric> metrics = null;
        
        for (String sourceName : sourceNames) {
            Dataset<String> sourceDataset = sourcesMap.get(sourceName);
            
            if(sourceDataset == null) {
                LOG.warn("Source with name \"" + sourceName + "\" does not exist");
                
                continue;
            }
            
            Optional<Dataset<Metric>> metricsFromSource = apply(sourceDataset, sourceName);
            
            if(metricsFromSource.isPresent())
                if(metrics == null)
                    metrics = metricsFromSource.get();
                else
                    metrics.union(metricsFromSource.get());
        }
        
        if(metrics == null) {
            LOG.warn("Schema \"" + id + "\" does not have any valid source");
            
            return Optional.empty();
        }
        
        return Optional.of(filter.apply(metrics));
    }

    private Optional<Dataset<Metric>> apply(Dataset<String> source, String sourceName) {

        Dataset<Row> inputData = source.select(from_json(col("value"), jsonSchema).as("value")).select("value.*");
        
        Dataset<Row> metricsData = null;
        
        for (ValueDescriptor value : values) {
            Dataset<Row> valueMetrics = inputData.select(
                                                    getTimestampColum().as("timestamp"),
                                                    getAttColumn(sourceName, value.getId()).as("att"),
                                                    value.getColum().as("value")
                                                  );
            
            if(metricsData == null)
                metricsData = valueMetrics;
            else
                metricsData = metricsData.union(valueMetrics);
        }
        
        if(metricsData == null) {
            LOG.warn("Schema \"" + id + "\", source \"" + sourceName + "\": does not have values to extract");
            
            return Optional.empty();
        }
        
        metricsData = metricsData.where("timestamp IS NOT NULL");
        metricsData = metricsData.where("value.num IS NOT NULL OR value.str IS NOT NULL OR value.bool IS NOT NULL");
        
        Dataset<Metric> metrics = metricsData.as(Encoders.bean(Metric.class));
        
        return Optional.of(metrics);
    }

    private Column getTimestampColum() {
        if(timestamp_key == null)
            return current_timestamp();
        
        if(timestamp_format_pattern.equals("epoch-s"))
            return col(timestamp_key).cast(DataTypes.TimestampType);
        
        if(timestamp_format_pattern.equals("epoch-ms"))
            return col(timestamp_key).divide(lit(1000)).cast(DataTypes.TimestampType);
        
        return unix_timestamp(col(timestamp_key), timestamp_format_pattern).cast(DataTypes.TimestampType);
    }

    private Column getAttColumn(String sourceName, String valueId) {
        List<Column> attColumns = attributes.stream()
                                        .map(att -> col(att.second).as(att.first))
                                        .collect(Collectors.toList());
        
        List<Column> fixedValueColumns = fixedAttributes.entrySet().stream()
                                        .map(att -> lit(att.getValue()).as(att.getKey()))
                                        .collect(Collectors.toList());
        
        attColumns.addAll(fixedValueColumns);
        
        attColumns.add(lit(sourceName).as("$source"));
        attColumns.add(lit(valueId).as("$value"));
        
        return struct(attColumns.toArray(new Column[0]));
    }

    private StructType getJsonSchema() throws ParseException {
        Map<String, DataType> keys = new HashMap<>();
        
        for (Pair<String, String> att : attributes)
            keys.put(att.second(), DataTypes.StringType);
        
        for (ValueDescriptor value : values)
            switch (value.getType()) {
            case STRING:
                keys.put(value.getKey(), DataTypes.StringType);
                break;
            case NUMERIC:
                keys.put(value.getKey(), DataTypes.DoubleType);
                break;
            case BOOLEAN:
                keys.put(value.getKey(), DataTypes.BooleanType);
                break;
            default:
                keys.put(value.getKey(), DataTypes.StringType);
            }
        
        if(timestamp_key != null)
            if(timestamp_format_pattern.startsWith("epoch"))
                keys.put(timestamp_key, DataTypes.LongType);
            else
                keys.put(timestamp_key, DataTypes.StringType);
        
        return StructTypeUtils.create(keys);
    }
    
}
