package ch.cern.exdemon.metrics.source.types;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.source.MetricsSource;
import ch.cern.properties.Properties;

@RegisterComponentType("kafka")
public class KafkaMetricsSource extends MetricsSource {

    private static final long serialVersionUID = 4110858617715602562L;
    
    private Map<String, Object> kafkaParams;
    private Set<String> kafkaTopics;

    @Override
    public ConfigurationResult config(Properties properties) {
    	ConfigurationResult confResult = super.config(properties);
    		
        kafkaParams = getKafkaConsumerParams(properties);
        kafkaTopics = new HashSet<String>(Arrays.asList(properties.getProperty("topics").split(",")));
        
        return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
	public JavaDStream<String> createJavaDStream(JavaStreamingContext ssc) {
    		JavaInputDStream<ConsumerRecord<String, String>> inputStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(kafkaTopics, kafkaParams));
        
        inputStream.foreachRDD(rdd -> {
        			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            		((CanCommitOffsets) inputStream.inputDStream()).commitAsync(offsetRanges);
        		});
        
        return inputStream.map(ConsumerRecord::value);
    }

    private Map<String, Object> getKafkaConsumerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("consumer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }

}
