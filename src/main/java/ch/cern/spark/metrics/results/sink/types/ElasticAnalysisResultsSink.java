package ch.cern.spark.metrics.results.sink.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

public class ElasticAnalysisResultsSink extends AnalysisResultsSink {

    private static final long serialVersionUID = -3422447741754872104L;
    
    private String indexName;

    private Map<String, String> elasticConfig;
    
    public ElasticAnalysisResultsSink() {
        super(ElasticAnalysisResultsSink.class, "elastic");
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
    public void sink(AnalysisResultsS outputStream) {
        JavaDStream<String> stream = outputStream.asJSON().asString();
        
        JavaEsSparkStreaming.saveJsonToEs(stream, indexName, elasticConfig);
    }

}
