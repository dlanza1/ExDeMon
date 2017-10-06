package ch.cern.spark.json;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class JSONObjectDeserializer implements Deserializer<JSONObject> {

    private JSONObject.Parser parser = new JSONObject.Parser();
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public JSONObject deserialize(String topic, byte[] data) {
        return parser.parse(data);
    }
    
    @Override
    public void close() {
    }

}
