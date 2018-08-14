package ch.cern.exdemon.monitor.analysis.results.sink.types;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.http.HTTPSink;
import ch.cern.exdemon.http.JsonPOSTRequest;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.sink.AnalysisResultsSink;
import ch.cern.properties.Properties;

@RegisterComponentType("http")
public class HTTPAnalysisResultSink extends AnalysisResultsSink {

    private final static Logger LOG = LogManager.getLogger(HTTPAnalysisResultSink.class);
    
	private static final long serialVersionUID = 6368509840922047167L;

	private HTTPSink sink = new HTTPSink();
	
	private Map<String, String> propertiesToAdd;

	@Override
	public ConfigurationResult config(Properties properties) {
		properties.setPropertyIfAbsent(HTTPSink.PARALLELIZATION_PARAM, "5");
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		
		propertiesToAdd = properties.getSubset("add").toStringMap();
		
		return sink.config(properties);
	}
	
	@Override
	public void sink(JavaDStream<AnalysisResult> results) {
        JavaDStream<JsonPOSTRequest> requests = results.flatMap(result -> {
            try {
                return Collections.singleton(toJsonPOSTRequest(result)).iterator();
            } catch (Exception e) {
                LOG.error("Error when parsing object to request. Object=" + String.valueOf(result), e);

                return Collections.emptyIterator();
            }
        });
	    
		sink.sink(requests);
	}

    public JsonPOSTRequest toJsonPOSTRequest(AnalysisResult result) throws ParseException {
        JSON json = JSONParser.parse(result);
        
        JsonPOSTRequest request = new JsonPOSTRequest(null, json);
        
        Map<String, String> tags = result.getTags();
        
        for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
            String value = propertyToAdd.getValue();
            
            if(value.startsWith("%"))
                if(tags != null)
                    value = tags.get(value.substring(1));
                else
                    value = null;
            
            if(value != null && value.equals("null"))
                value = null;
                
            if(value != null 
                    && value.startsWith("[")
                    && value.endsWith("]")) {
                String arrayContent = value.substring(1, value.length() - 1);
                String[] arrayContentParts = arrayContent.split("\\+\\+");
                
                JsonArray jsonArray = new JsonArray();
                
                String[] jsonKeys = request.getJson().getAllKeys();
                
                for (String arrayContentPart : arrayContentParts) {
                    if(arrayContentPart.startsWith("keys:")) {
                        String keysRegex = arrayContentPart.replace("keys:", "");
                        
                        Pattern pattern = Pattern.compile(keysRegex);
                        
                        String[] matchingKeys = Arrays.stream(jsonKeys).filter(jsonKey -> pattern.matcher(jsonKey).matches()).toArray(String[]::new);
                        
                        for (String matchingKey : matchingKeys)
                            jsonArray.add(new JsonPrimitive(matchingKey));
                    }
                    
                    if(arrayContentPart.startsWith("attributes:#")) {
                        String tag = arrayContentPart.replace("attributes:#", "");
                        
                        if(tags.containsKey(tag)) {
                            String[] attributesKeys = tags.get(tag).split("\\s");
                            
                            for (String attributeKey : attributesKeys) {
                                String fullKey = "analyzed_metric.attributes." + attributeKey;
                                
                                boolean exist = Arrays.stream(jsonKeys).filter(jsonKey -> jsonKey.equals(fullKey)).count() > 0;
                                
                                if(exist)
                                    jsonArray.add(new JsonPrimitive(fullKey));
                            }
                        }
                    }
                }
                
                request.getJson().getElement().getAsJsonObject().add(propertyToAdd.getKey(), jsonArray);
            }else{
                request.addProperty(propertyToAdd.getKey(), value);
            }
        }
        
        return request;
    }

}
