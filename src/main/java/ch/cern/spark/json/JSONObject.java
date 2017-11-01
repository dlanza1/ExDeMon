package ch.cern.spark.json;

import java.io.Serializable;

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

    public String getProperty(String propertyName) {
        if (object == null)
            object = PARSER.parse(string).getAsJsonObject();

        JsonElement jsonElement = getElement(propertyName);

        if (jsonElement == null || jsonElement.isJsonNull())
            return null;
        else
            return jsonElement.getAsString();
    }

    public JsonElement getElement(String elementName) {
        if(elementName == null)
            return null;
        
        if (object == null)
            object = PARSER.parse(string).getAsJsonObject();

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

    public JSONObject getJSONObject(String name) {
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
