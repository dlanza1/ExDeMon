package ch.cern.spark.json;

import org.apache.spark.streaming.api.java.JavaDStream;

public class JsonS extends JavaDStream<JSONObject>{

    private static final long serialVersionUID = 5260839504442920261L;

    public JsonS(JavaDStream<JSONObject> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public JavaDStream<String> asString() {
        return map(JSONObject::toString);
    }

}
