package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.var.MetricVariableStatus;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.PropertiesValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;

public class JSONStatusSerializerTest {
    
    @Test
    public void serializeValue() throws IOException {
        JSONStatusSerializer ser = new JSONStatusSerializer();
        
        MetricVariableStatus status = new MetricVariableStatus();
        Instant instant = Instant.now();
        status.add(new FloatValue(1d), instant);
        status.add(new StringValue("a"), instant);
        status.add(new BooleanValue(true), instant);
        status.add(new ExceptionValue("Exception msg"), instant);
        status.add(new PropertiesValue("name", new Properties()), instant);
        
        String json = new String(ser.fromValue(status));

        StatusValue statusDesser = ser.toValue(json.getBytes());
        
        assertEquals(status, statusDesser);
    }

}
