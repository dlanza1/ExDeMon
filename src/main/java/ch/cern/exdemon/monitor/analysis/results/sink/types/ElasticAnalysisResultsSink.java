package ch.cern.exdemon.monitor.analysis.results.sink.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.sink.AnalysisResultsSink;
import ch.cern.properties.Properties;

@RegisterComponentType("elastic")
public class ElasticAnalysisResultsSink extends AnalysisResultsSink {

    private static final long serialVersionUID = -3422447741754872104L;
    
    private String indexName;

    private Map<String, String> elasticConfig;

    @Override
    public ConfigurationResult config(Properties properties) {
        indexName = properties.getProperty("index");
        
        elasticConfig = getElasticConfig(properties);
        
        return ConfigurationResult.SUCCESSFUL();
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
    public void sink(JavaDStream<AnalysisResult> outputStream) {
        JavaDStream<String> stream = outputStream.map(JSONParser::parse).map(Object::toString);
        
        JavaEsSparkStreaming.saveJsonToEs(stream, indexName, elasticConfig);
    }

}
