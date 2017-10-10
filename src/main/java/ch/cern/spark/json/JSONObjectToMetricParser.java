package ch.cern.spark.json;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.Metric;

public class JSONObjectToMetricParser implements Function<JSONObject, Metric>{

    private static final long serialVersionUID = -5490112720236337434L;
    
    public static String PARAM_PREFIX = "parser.";

    public static String ID_PROPERTY = PARAM_PREFIX + "id.property-names";
    public static String ID_PROPERTY_DEFAULT = "id";
    private List<String> id_property_names = Arrays.asList(ID_PROPERTY_DEFAULT);
    
    public static String TIMESTAMP_PROPERTY = PARAM_PREFIX + "timestamp.property-name";
    public static String TIMESTAMP_PROPERTY_DEFAULT = "timestamp";
    private String timestamp_property_name;
    
    public static String TIMESTAMP_FORMAT_PROPERTY = PARAM_PREFIX + "timestamp.format";
    public static String TIMESTAMP_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private DateTimeFormatter timestamp_format;
    
    public static String VALUE_PROPERTY = PARAM_PREFIX + "value.property-name";
    public static String VALUE_PROPERTY_DEFAULT = "value";
    private String value_property_name;
    
    public JSONObjectToMetricParser(Properties props) {
        id_property_names = Arrays.asList(props.getProperty(ID_PROPERTY).split(" "));
        
        timestamp_property_name = props.getProperty(TIMESTAMP_PROPERTY, TIMESTAMP_PROPERTY_DEFAULT);
        timestamp_format = new DateTimeFormatterBuilder()
								.appendPattern(props.getProperty(TIMESTAMP_FORMAT_PROPERTY, TIMESTAMP_FORMAT_DEFAULT))
								.toFormatter()
								.withZone(ZoneOffset.systemDefault());
        
        value_property_name = props.getProperty(VALUE_PROPERTY, VALUE_PROPERTY_DEFAULT);
    }
    
    public static JavaDStream<Metric> apply(Properties props, JavaDStream<JSONObject> metricsAsJsonStream) {
        return metricsAsJsonStream.map(new JSONObjectToMetricParser(props));
    }

    @Override
    public Metric call(JSONObject jsonObject) throws Exception {
        Instant timestamp = toDate(jsonObject.getProperty(timestamp_property_name));
        Float value = Float.parseFloat(jsonObject.getProperty(value_property_name));
        Map<String, String> ids = mapIDsWithValues(jsonObject);
        
        return new Metric(timestamp, value, ids);
    }

    private Map<String, String> mapIDsWithValues(JSONObject jsonObject) {
        Map<String, String> map = new HashMap<>();
        
        for (String id_property : id_property_names)
            map.put(id_property, jsonObject.getProperty(id_property));
        
        return map;
    }

    private Instant toDate(String date_string) throws ParseException {
        return timestamp_format.parse(date_string, Instant::from);
    }

}
