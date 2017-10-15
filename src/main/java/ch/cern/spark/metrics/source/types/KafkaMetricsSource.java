package ch.cern.spark.metrics.source.types;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import ch.cern.spark.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONObjectDeserializer;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.source.MetricsSource;

public class KafkaMetricsSource extends MetricsSource {

    private static final long serialVersionUID = 4110858617715602562L;
    
    private Map<String, Object> kafkaParams;
    private Set<String> kafkaTopics;
    
    public static String ATTRIBUTES_PARAM = "parser.attributes";
    private String[] attributes;
    
    public static String VALUE_ATTRIBUTE_PARAM = "parser.value.attribute";
    private String value_attribute;
    
    public static String TIMESTAMP_FORMAT_PARAM = "parser.timestamp.format";
    public static String TIMESTAMP_FORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private String timestamp_format_pattern;
    
    private transient DateTimeFormatter timestamp_format;

    public static String TIMESTAMP_ATTRIBUTE_PARAM = "parser.timestamp.attribute";
    private String timestamp_attribute;

    public KafkaMetricsSource() {
        super(KafkaMetricsSource.class, "kafka");
    }
    
    @Override
    public void config(Properties properties) throws Exception {
        kafkaParams = getKafkaConsumerParams(properties);
        kafkaTopics = new HashSet<String>(Arrays.asList(properties.getProperty("topics").split(",")));
        
        attributes = properties.getProperty(ATTRIBUTES_PARAM).split("\\s");
        value_attribute = properties.getProperty(VALUE_ATTRIBUTE_PARAM);
        timestamp_attribute = properties.getProperty(TIMESTAMP_ATTRIBUTE_PARAM);        
        timestamp_format_pattern = properties.getProperty(TIMESTAMP_FORMAT_PARAM, TIMESTAMP_FORMAT_DEFAULT);
    }
    
    @Override
	public JavaDStream<Metric> createJavaDStream(JavaStreamingContext ssc) {
        JavaDStream<JSONObject> inputStream = createKafkaInputStream(ssc);

        JavaDStream<Metric> metricStream = inputStream.map(metric -> parse(metric));
        
        return metricStream;
    }
    
    public JavaDStream<JSONObject> createKafkaInputStream(JavaStreamingContext ssc) {
        JavaInputDStream<ConsumerRecord<String, JSONObject>> inputStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, JSONObject>Subscribe(kafkaTopics, kafkaParams));
        
        inputStream.foreachRDD(rdd -> {
        			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            		((CanCommitOffsets) inputStream.inputDStream()).commitAsync(offsetRanges);
        		});
        
        return inputStream.map(ConsumerRecord::value);
    }

    private Map<String, Object> getKafkaConsumerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", JSONObjectDeserializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("consumer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
    
    private Metric parse(JSONObject jsonObject) throws ParseException{
		Instant timestamp = toDate(jsonObject.getProperty(timestamp_attribute));
        
        float value = Float.parseFloat(jsonObject.getProperty(value_attribute));
        
        Map<String, String> ids = Stream.of(attributes)
        		.collect(Collectors.toMap(String::toString, jsonObject::getProperty));
        
        return new Metric(timestamp, value, ids);
    }
    
    private Instant toDate(String date_string) throws ParseException {
    		if(timestamp_format == null)
        		timestamp_format = new DateTimeFormatterBuilder()
								.appendPattern(timestamp_format_pattern)
								.toFormatter()
								.withZone(ZoneOffset.systemDefault());
    	
        return timestamp_format.parse(date_string, Instant::from);
    }

}
