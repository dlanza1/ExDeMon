package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.ValueHistory.Status;
import ch.cern.spark.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.spark.metrics.defined.equation.var.agg.CountAgregation;
import ch.cern.spark.metrics.value.AggregatedValue;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.PropertiesValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.utils.DurationAndTruncate;

public class JSONStatusSerializerTest {
    
    @Test
    public void serializeValueHistory() throws IOException {
        JSONStatusSerializer ser = new JSONStatusSerializer();
        
        ValueHistory.Status status = new ValueHistory.Status(100, new DurationAndTruncate(Duration.ofSeconds(10)), ChronoUnit.MINUTES, new CountAgregation());
        status.history.add(Instant.now(), new FloatValue(2));
        String json = new String(ser.fromValue(status));
        
        ValueHistory.Status statusDesser = (Status) ser.toValue(json.getBytes());
        
        assertEquals(status.history, statusDesser.history);
    }
    
    @Test
    public void serializeValue() throws IOException {
        JSONStatusSerializer ser = new JSONStatusSerializer();
        
        AggregationValues status = new AggregationValues(100);
        
        Instant instant = Instant.now();
        status.add(0, new FloatValue(1d), instant);
        status.add(1, new StringValue("a"), instant);
        status.add(2, new BooleanValue(true), instant);
        status.add(3, new ExceptionValue("Exception msg"), instant);
        status.add(4, new PropertiesValue("name", new Properties()), instant);
        status.add(5, new AggregatedValue(new FloatValue(1000)), instant);
        
        String json = new String(ser.fromValue(status));
        StatusValue statusDesser = ser.toValue(json.getBytes());
        assertEquals(status, statusDesser);
        
        State<AggregationValues> state = new StateImpl<>();
        status.update(state, new Time(1234));
        
        json = new String(ser.fromValue(status));
        statusDesser = ser.toValue(json.getBytes());
        assertEquals(status, statusDesser);
    }

}