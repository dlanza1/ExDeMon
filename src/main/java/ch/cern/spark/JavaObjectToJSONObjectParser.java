package ch.cern.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ch.cern.spark.json.JSONObject;

public class JavaObjectToJSONObjectParser<T> implements Function<T, JSONObject>{

    private static final long serialVersionUID = -2793508300018983992L;
    
    public static String TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    
    private transient static Gson gson = new GsonBuilder().setDateFormat(TIMESTAMP_OUTPUT_FORMAT).create();;

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