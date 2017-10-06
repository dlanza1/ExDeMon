package ch.cern.spark.json;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

public class JSONObjectsExtractor implements Function<JSONObject, JSONObject> {

    private static final long serialVersionUID = -3335036457184289847L;

    private String elementToExtract;
    
    public JSONObjectsExtractor(String element) {
        this.elementToExtract = element;
    }

    public static JavaDStream<JSONObject> apply(JavaDStream<JSONObject> inputStream, String element) {        
        return inputStream.map(new JSONObjectsExtractor(element));
    }

    public JSONObject call(JSONObject inputObject) throws Exception {
        return inputObject.getJSONObject(elementToExtract);
    }

}
