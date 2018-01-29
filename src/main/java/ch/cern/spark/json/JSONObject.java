package ch.cern.spark.json;

import java.io.Serializable;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JSONObject implements Serializable {

    private static transient final long serialVersionUID = 416506194813266351L;

    private static transient JsonParser PARSER = new JsonParser();

    private String string;

    private transient JsonObject object;

    public JSONObject(JsonObject jsonObject) {
        this.object = jsonObject;
        this.string = this.object.toString();
    }

    public JSONObject(String string) {
        this.string = string;
    }

    public String getProperty(String propertyName) throws ParseException {
        JsonElement jsonElement = getElement(propertyName);

        if (jsonElement == null || jsonElement.isJsonNull())
            return null;
        else
            return jsonElement.getAsString();
    }

    public JsonElement getElement(String elementName) throws ParseException {
        if(elementName == null)
            return null;
        
        setObject();

        if (elementName.contains(".")) {
            String topPropertyName = elementName.substring(0, elementName.indexOf('.'));
            JSONObject topObject = getJSONObject(topPropertyName);

            if (topObject == null)
                return null;
            else
                return topObject.getElement(elementName.substring(elementName.indexOf('.') + 1));
        }

        return object.get(elementName);
    }
    
    public String[] getKeys(Pattern keyPattern) throws ParseException {
        String[] keys = getAllKeys();
        
        return Stream.of(keys).filter(key -> keyPattern.matcher(key).matches()).collect(Collectors.toList()).toArray(new String[0]);
    }
    
    public String[] getAllKeys() throws ParseException {
        setObject();
        
        LinkedList<String> keys = new LinkedList<>();
        
        for(Map.Entry<String, JsonElement> element: object.entrySet())
            if(element.getValue().isJsonPrimitive())
                keys.add(element.getKey());
            else if(element.getValue().isJsonObject())
                addKeys(keys, element.getValue().getAsJsonObject(), element.getKey());
        
        return keys.toArray(new String[0]);
    }

    private void addKeys(LinkedList<String> keys, JsonObject object, String previousKey) {
        for(Map.Entry<String, JsonElement> element: object.entrySet())
            if(element.getValue().isJsonPrimitive())
                keys.add(previousKey + "." + element.getKey());
            else if(element.getValue().isJsonObject())
                addKeys(keys, element.getValue().getAsJsonObject(), previousKey + "." + element.getKey());
    }

    public void setProperty(String fullKey, String value) throws ParseException {
	    	setObject();
	    	
	    String[] keys = fullKey.split("\\.");
	    JsonObject element = object;
	    for (int i = 0; i < keys.length; i++) {
	    		String key = keys[i];
	    		
	    		if(i == keys.length - 1) {
	    			element.addProperty(key, value);
	    		}else {
	    			JsonElement elementTmp = element.get(key);
	    			
	    			if(elementTmp == null) {
	    				element.add(key, new JsonObject());
	    				element = element.getAsJsonObject(key);
	    			}else if(elementTmp.isJsonObject())
	    				element = elementTmp.getAsJsonObject();
	    			else
	    				throw new ParseException("It is not possible to add " + fullKey + " to JSON: " + object.toString(), 0);
	    		}
		}
	    	this.string = this.object.toString();
    }

    private void setObject() throws ParseException {
        if (object != null)
            return;
        
        try {
            object = PARSER.parse(string).getAsJsonObject();
        }catch(Exception e) {
            throw new ParseException(e.getMessage(), 0);
        }
    }

    public JSONObject getJSONObject(String name) throws ParseException {
        JsonElement element = getElement(name); 
        
        return element == null ? null : new JSONObject(element.getAsJsonObject());
    }

    @Override
    public String toString() {
        return string;
    }

    public static class Parser implements Serializable {

        private static final long serialVersionUID = 8527535247072244888L;

        public Parser() {
        }

        public JSONObject parse(byte[] bytes) {
            JsonObject jsonFromEvent = PARSER.parse(new String(bytes)).getAsJsonObject();

            return new JSONObject(jsonFromEvent);
        }

    }

}
