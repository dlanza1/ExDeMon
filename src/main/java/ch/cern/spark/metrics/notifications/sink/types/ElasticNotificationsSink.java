package ch.cern.spark.metrics.notifications.sink.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

public class ElasticNotificationsSink extends NotificationsSink {
    
    private static final long serialVersionUID = 6247567528073485033L;

    private String indexName;

    private Map<String, String> elasticConfig;
    
    public ElasticNotificationsSink() {
        super(ElasticNotificationsSink.class, "elastic");
    }

    @Override
    public void config(Properties properties) throws Exception {
        super.config(properties);
        
        indexName = properties.getProperty("index");
        
        elasticConfig = getElasticConfig(properties);
    }
    
    private Map<String, String> getElasticConfig(Properties properties) {
        Properties esProps = properties.getSubset("es");
        
        Map<String, String> outputMap = new HashMap<>();
        
        for (Entry<Object, Object> entry : esProps.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            
            outputMap.put("es." + key, value);
        }
        
        return outputMap;
    }

    @Override
    public void sink(NotificationsS outputStream) {
        JavaDStream<String> jsonStringsStream = outputStream.asJSON().asString();
        
        JavaEsSparkStreaming.saveJsonToEs(jsonStringsStream, indexName, elasticConfig);
    }

}
