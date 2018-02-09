package ch.cern.spark.json;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class JSONObjectDeserializer implements Deserializer<JSON> {

    private JSON.Parser parser = new JSON.Parser();
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public JSON deserialize(String topic, byte[] data) {
        return parser.parse(data);
    }
    
    @Override
    public void close() {
    }

}
