package ch.cern.exdemon.monitor.analysis.results.sink.types;

import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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

@RegisterComponentType("cern-http")
public class CERNAnalysisResultSink extends AnalysisResultsSink {

    private final static Logger LOG = LogManager.getLogger(CERNAnalysisResultSink.class);
    
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
        
        addInfluxDbMetadata(result, request);
        
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

            if(value != null)
                request.addProperty(propertyToAdd.getKey(), value);
        }
        
        return request;
    }

    private void addInfluxDbMetadata(AnalysisResult result, JsonPOSTRequest request) {
        Map<String, String> tags = result.getTags();
        
        JsonObject idbTagAndValuesJsonObject = new JsonObject();
        JsonArray idbTagsJsonArray = new JsonArray();
        
        result.getAnalyzed_metric().getAttributes().entrySet().stream()
                                                .filter(att -> att.getKey().startsWith("$"))
                                                .forEach(att -> {
                                                    idbTagAndValuesJsonObject.addProperty(att.getKey(), att.getValue());
                                                    idbTagsJsonArray.add(new JsonPrimitive("idbtags.".concat(att.getKey())));
                                                });
        
        String idbTagsAttributesAsString = tags.get("influx.tags.attributes");
        String[] idbTagsAttributes = Optional.ofNullable(idbTagsAttributesAsString)
                                                .map(attKey -> attKey.split("\\s"))
                                                .orElse(new String[0]);
        Stream.of(idbTagsAttributes).filter(attKey -> result.getAnalyzed_metric().getAttributes().get(attKey) != null)            
                                                .forEach(attKey -> {
                                                    idbTagAndValuesJsonObject.addProperty(attKey, result.getAnalyzed_metric().getAttributes().get(attKey));
                                                    idbTagsJsonArray.add(new JsonPrimitive("idbtags.".concat(attKey)));
                                                });
        
        request.getJson().getElement().getAsJsonObject().add("idbtags", idbTagAndValuesJsonObject);
        request.getJson().getElement().getAsJsonObject().add("idb_tags", idbTagsJsonArray);
    }

}
