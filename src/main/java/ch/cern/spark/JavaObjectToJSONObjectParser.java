package ch.cern.spark;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import ch.cern.spark.json.JSONObject;

public class JavaObjectToJSONObjectParser<T> implements Function<T, JSONObject>{

    private static final long serialVersionUID = -2793508300018983992L;
    
    public static String TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    
    private transient static Gson gson = new GsonBuilder()
    		.registerTypeAdapter(Instant.class, new JsonSerializer<Instant>() {
    				DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_OUTPUT_FORMAT);
			
				@Override
				public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
					return new JsonPrimitive(ZonedDateTime.ofInstant(instant , ZoneOffset.systemDefault()).format(formatter));
				}
    			})
    		.create();

    public JavaObjectToJSONObjectParser(){
    }
    
    public static <T> JavaDStream<JSONObject> apply(JavaDStream<T> analysisResultsStream) {
        return analysisResultsStream.map(new JavaObjectToJSONObjectParser<T>());
    }

    @Override
    public JSONObject call(T javaObject) throws Exception {    	
        return javaObject == null ? null : new JSONObject(gson.toJson(javaObject));
    }

}