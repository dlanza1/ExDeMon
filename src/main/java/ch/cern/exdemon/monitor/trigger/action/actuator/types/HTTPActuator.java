package ch.cern.exdemon.monitor.trigger.action.actuator.types;

import java.text.ParseException;
import java.util.Map;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.http.HTTPSink;
import ch.cern.exdemon.http.JsonPOSTRequest;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.actuator.Actuator;
import ch.cern.exdemon.monitor.trigger.action.template.Template;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import lombok.ToString;

@RegisterComponentType("http")
@ToString(callSuper=false)
public class HTTPActuator extends Actuator {

	private static final long serialVersionUID = 6368509840922047167L;

	private HTTPSink sink = new HTTPSink();

    private Map<String, String> propertiesToAdd;

    private boolean addAction;

    public static final String URL_PARAM = "url";
    private String url;

	@Override
	public ConfigurationResult config(Properties properties) {
	    ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
	    
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		
		url = properties.getProperty(URL_PARAM);
		
        try {
            addAction = properties.getBoolean("add.$action", true);
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		propertiesToAdd = properties.getSubset("add").toStringMap();
		
		return sink.config(properties).merge(null, confResult);
	}

	@Override
	protected void run(Action action) throws Exception {    
		sink.sink(toJsonPOSTRequest(action));	
	}

    public JsonPOSTRequest toJsonPOSTRequest(Action action) throws ParseException {     
        String requestUrl = Template.apply(url, action);
        JSON json = addAction ? JSONParser.parse(action) : new JSON("{}");
        
        JsonPOSTRequest request = new JsonPOSTRequest(requestUrl, json);
        
        for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
            String value = propertyToAdd.getValue();
            
            if(value != null)
                value = Template.apply(value, action);
            
            if(value != null && value.equals("null"))
                value = null;
                
            if(value != null)
                request.addProperty(propertyToAdd.getKey(), value);
        }
        
        return request;
    }

}
