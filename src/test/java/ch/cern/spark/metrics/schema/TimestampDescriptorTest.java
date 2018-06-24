package ch.cern.spark.metrics.schema;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import com.google.gson.JsonObject;

import ch.cern.properties.Properties;
import ch.cern.spark.json.JSON;

public class TimestampDescriptorTest {
    
    @Test
    public void shift() throws Exception {
        TimestampDescriptor descriptor = new TimestampDescriptor();
        Properties properties = new Properties();
        properties.setProperty("key", "time");
        properties.setProperty("shift", "-3h");
        descriptor.config(properties);

        Instant time = Instant.now();
        
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("time", time.toString());
        Instant result = descriptor.extract(new JSON(jsonObject));
        
        Instant expectedResult = time.minus(Duration.ofHours(3));
        
        assertEquals(expectedResult, result);
    }
    
}
