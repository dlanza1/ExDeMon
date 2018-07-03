package ch.cern.exdemon.json;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

public class JSONObjectsExtractor implements Function<JSON, JSON> {

    private static final long serialVersionUID = -3335036457184289847L;

    private String elementToExtract;
    
    public JSONObjectsExtractor(String element) {
        this.elementToExtract = element;
    }

    public static JavaDStream<JSON> apply(JavaDStream<JSON> inputStream, String element) {        
        return inputStream.map(new JSONObjectsExtractor(element));
    }

    public JSON call(JSON inputObject) throws Exception {
        return inputObject.getJSONObject(elementToExtract);
    }

}
