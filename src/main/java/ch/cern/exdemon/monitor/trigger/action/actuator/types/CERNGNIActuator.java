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
import ch.cern.monitoring.gni.GNINotification;
import ch.cern.properties.Properties;
import lombok.ToString;

@RegisterComponentType("cern-gni")
@ToString(callSuper=false)
public class CERNGNIActuator extends Actuator {

	private static final long serialVersionUID = 6416955181811280312L;
	
	private Properties contentProperties;
	
	private HTTPSink sink = new HTTPSink();

    private Map<String, String> propertiesToAdd;

	@Override
	public ConfigurationResult config(Properties properties) {
		contentProperties = properties.getSubset("content");
		
		properties.setPropertyIfAbsent(HTTPSink.RETRIES_PARAM, "5");
		
		propertiesToAdd = properties.getSubset("add").toStringMap();
		
		return sink.config(properties);
	}
	
	@Override
	protected void run(Action action) throws Exception {
	    GNINotification notif = GNINotification.from(contentProperties, action);

        sink.sink(toJsonPOSTRequest(notif));
	}

    public JsonPOSTRequest toJsonPOSTRequest(GNINotification notif) throws ParseException {
        JSON json = JSONParser.parse(notif);
        
        JsonPOSTRequest request = new JsonPOSTRequest(null, json);
        
        for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
            String value = propertyToAdd.getValue();

            if(value != null && value.equals("null"))
                value = null;
                
            if(value != null)
                request.addProperty(propertyToAdd.getKey(), value);
        }
        
        return request;
    }

}
