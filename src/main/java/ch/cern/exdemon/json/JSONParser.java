package ch.cern.exdemon.json;

import java.time.Instant;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JSONParser {

    private final transient static  Gson gson = new GsonBuilder()
    		                                            .registerTypeAdapter(Instant.class, new InstantJsonSerializer())
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