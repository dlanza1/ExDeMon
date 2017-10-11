package ch.cern.spark.json;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.Stream;

public class JsonS extends Stream<JavaDStream<JSONObject>>{

    private static final long serialVersionUID = 5260839504442920261L;

    public JsonS(JavaDStream<JSONObject> stream) {
        super(stream);
    }

    public JavaDStream<String> asString() {
        return stream().map(JSONObject::toString);
    }

}
