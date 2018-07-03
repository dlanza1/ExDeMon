package ch.cern.exdemon.json;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JSONParser {

    public static String TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    
    private final transient static  Gson gson = new GsonBuilder()
    		.registerTypeAdapter(Instant.class, new JsonSerializer<Instant>() {
    				DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_OUTPUT_FORMAT);
			
				@Override
				public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
					return new JsonPrimitive(ZonedDateTime.ofInstant(instant , ZoneOffset.systemDefault()).format(formatter));
				}
    			})
    		.create();

    public static<T> JSON parse(T javaObject) {    	
        return javaObject == null ? null : new JSON(gson.toJson(javaObject));
    }

    public static boolean isValid(String jsonInString) {
        if(jsonInString == null)
            return false;
        
        try {
            gson.fromJson(jsonInString, Object.class);
            return true;
        } catch(com.google.gson.JsonSyntaxException ex) { 
            return false;
        }
    }
    
}