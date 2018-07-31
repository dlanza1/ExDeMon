package ch.cern.exdemon.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.exdemon.metrics.ValueHistory.Status;
import ch.cern.exdemon.metrics.defined.equation.ComputationException;
import ch.cern.exdemon.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.CountAgregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.SumAggregation;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.utils.TimeUtils;

public class ValueHistoryTest {

    @Test
    public void floatValueSerializationSize() throws IOException, ParseException{
        ValueHistory.Status store = new Status();
        store.history = new ValueHistory();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesBefore = bos.toByteArray();
        
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), new FloatValue(Math.random()));
        
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesAfter = bos.toByteArray();
        
        int sizePerRecord = (bytesAfter.length - bytesBefore.length) / numberOfRecords;
        assertEquals(33, sizePerRecord);
    }
    
    @Test
    public void stringValueSerializationSize() throws IOException, ParseException{
        ValueHistory.Status store = new Status();
        store.history = new ValueHistory();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesBefore = bos.toByteArray();
        
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), new StringValue("something"));
        
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesAfter = bos.toByteArray();
        
        int sizePerRecord = (bytesAfter.length - bytesBefore.length) / numberOfRecords;
        assertEquals(35, sizePerRecord);
    }
    
    @Test
    public void booleanValueSerializationSize() throws IOException, ParseException{
        ValueHistory.Status store = new Status();
        store.history = new ValueHistory();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesBefore = bos.toByteArray();
        
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), new BooleanValue(true));
        
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesAfter = bos.toByteArray();
        
        int sizePerRecord = (bytesAfter.length - bytesBefore.length) / numberOfRecords;
        assertEquals(30, sizePerRecord);
    }
    
    @Test
    public void exceptionValueSerializationSize() throws IOException, ParseException{
        ValueHistory.Status store = new Status();
        store.history = new ValueHistory();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesBefore = bos.toByteArray();
        
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), new ExceptionValue("message"));
        
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesAfter = bos.toByteArray();
        
        int sizePerRecord = (bytesAfter.length - bytesBefore.length) / numberOfRecords;
        assertEquals(37, sizePerRecord);
    }
    
    @Test
    public void saveAndLoad() throws IOException, ParseException, ClassNotFoundException, ComputationException{
        Status store = new Status();
        store.history = new ValueHistory();
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), (float) Math.random());
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytes = bos.toByteArray();
        out.close();
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Status restoredStore = (ValueHistory.Status) ois.readObject();
        ois.close();
        
        assertNotSame(store.history.getDatedValues(), restoredStore.history.getDatedValues());
        assertEquals(store.history.getDatedValues(), restoredStore.history.getDatedValues());
    }
    
    @Test
    public void lastAggregatedMetrics() throws ComputationException {
        ValueHistory values = new ValueHistory(3, 0, null, null);
        values.add(Instant.now(), new FloatValue(1), new Metric(Instant.now(), new FloatValue(1), new HashMap<>()));
        assertNull(values.getLastAggregatedMetrics());
        
        values = new ValueHistory(3, 2, null, null);
        values.add(Instant.now(), new FloatValue(1), new Metric(Instant.now(), new FloatValue(1), new HashMap<>()));
        values.add(Instant.now(), new FloatValue(1), new Metric(Instant.now(), new FloatValue(1), new HashMap<>()));
        assertEquals(2, values.getLastAggregatedMetrics().size());
        
        Metric metric1 = new Metric(Instant.now(), new FloatValue(1), new HashMap<>());
        values.add(Instant.now(), new FloatValue(1), metric1);
        assertEquals(2, values.getLastAggregatedMetrics().size());
        assertTrue(values.getLastAggregatedMetrics().contains(metric1));
        
        Metric metric2 = new Metric(Instant.now(), new FloatValue(1), new HashMap<>());
        values.add(Instant.now(), new FloatValue(1), metric2);
        assertEquals(2, values.getLastAggregatedMetrics().size());
        assertTrue(values.getLastAggregatedMetrics().contains(metric1));
        assertTrue(values.getLastAggregatedMetrics().contains(metric2));
    }
    
    @Test
    public void shouldGenerateExceptionWhenMaxSizeIsReached() throws ComputationException {
        ValueHistory values = new ValueHistory(3, 0, null, null);
        
        Instant now = Instant.now();
        
        values.add(now, 0f);
        values.getDatedValues();
        
        values.add(now, 0f);
        values.getDatedValues();
        
        values.add(now, 0f);
        values.getDatedValues();
        
        values.add(now, 0f);
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
    }
    
    @Test
    public void granularityShouldProvideSameResults() throws ComputationException {
        Aggregation aggregation = new SumAggregation();
        
        ValueHistory values = new ValueHistory(10000, 0, null, null);
        
        ChronoUnit granularity = ChronoUnit.MINUTES;
        ValueHistory granularValues = new ValueHistory(100, 0, granularity, aggregation);
        
        Instant time = Instant.parse("2007-12-03T10:15:00.00Z");
        float num = (float) Math.random();
        values.add(time, num);
        granularValues.add(time, num);
        assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        assertEquals(values.size(), granularValues.size());
        
        time = Instant.parse("2007-12-03T10:15:30.00Z");
        num = (float) Math.random();
        values.add(time, num);
        granularValues.add(time, num);
        assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        assertEquals(values.size(), granularValues.size());
        
        for (int i = 0; i < 1000; i++) {
            Instant tmpTime = time.plus(Duration.ofSeconds((long) (3000f * Math.random())));

            num = (float) Math.random();
            values.add(tmpTime, num);
            granularValues.add(tmpTime, num);
            assertEquals(
                    aggregation.aggregateValues(values.getDatedValues(), time).getAsAggregated().get().getAsFloat().get(), 
                    aggregation.aggregateValues(granularValues.getDatedValues(), time).getAsAggregated().get().getAsFloat().get(),
                    0.0001f);
        }
        
        time = Instant.parse("2007-12-03T10:16:30.00Z");
        num = (float) Math.random();
        values.add(time, num);
        granularValues.add(time, num);
        assertEquals(
                aggregation.aggregateValues(values.getDatedValues(), time).getAsAggregated().get().getAsFloat().get(), 
                aggregation.aggregateValues(granularValues.getDatedValues(), time).getAsAggregated().get().getAsFloat().get(),
                0.0001f);
        assertTrue(values.size() > granularValues.size());
    }
    
    @Test
    public void granularityWithDifferentInputOutputTypeInAggregation() throws ComputationException {
        Aggregation aggregation = new CountAgregation();
        
        ValueHistory values = new ValueHistory(100, 0, null, null);
        
        ChronoUnit granularity = ChronoUnit.MINUTES;
        ValueHistory granularValues = new ValueHistory(10, 0, granularity, aggregation);
        
        Instant time = Instant.parse("2007-12-03T10:15:00.00Z");
        StringValue input = new StringValue("a");
        values.add(time, input);
        granularValues.add(time, input);
        assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        assertEquals(values.size(), granularValues.size());
        
        time = Instant.parse("2007-12-03T10:15:30.00Z");
        input = new StringValue("b");
        values.add(time, input);
        granularValues.add(time, input);
        assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        assertEquals(values.size(), granularValues.size());
        
        for (int i = 0; i < 10; i++) {
            time = time.plus(Duration.ofSeconds(20));
            input = new StringValue("c");
            values.add(time, input);
            granularValues.add(time, input);
            assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        }
        
        time = Instant.parse("2007-12-03T10:30:00.00Z");
        input = new StringValue("d");
        values.add(time, input);
        granularValues.add(time, input);
        assertEquals(aggregation.aggregateValues(values.getDatedValues(), time), aggregation.aggregateValues(granularValues.getDatedValues(), time));
        assertTrue(values.size() > granularValues.size());
    }
    
    @Test
    public void shouldRecoverAfterMaxSizeIsReached() throws ComputationException {
        ValueHistory values = new ValueHistory(3, 0, null, null);
        
        Instant now = Instant.now();
        
        values.add(now, 0f);
        values.getDatedValues();
        
        values.add(now.plus(Duration.ofSeconds(2)), 0f);
        values.getDatedValues();
        
        values.add(now.plus(Duration.ofSeconds(4)), 0f);
        values.getDatedValues();
        
        values.add(now.plus(Duration.ofSeconds(6)), 0f);
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
        
        values.purge(now.plus(Duration.ofSeconds(1)));
        
        values.getDatedValues();
        
        values.add(now.plus(Duration.ofSeconds(6)), 0f);
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
    }
    
    @Test
    public void expiration() throws Exception{
        ValueHistory history = new ValueHistory();
        
        history.add(TimeUtils.toInstant("2017-04-01 11:18:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-01 11:18:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-01 11:19:12"), 11f);
        history.add(TimeUtils.toInstant("2017-04-01 11:19:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:10"), 14f);
        
        history.purge(TimeUtils.toInstant("2017-04-01 11:19:30"));
        
        List<Value> returnedValues = history.getDatedValues().stream().map(value -> value.getValue()).collect(Collectors.toList());
        
        List<FloatValue> expected = Arrays.asList(new FloatValue(12f), new FloatValue(13f), new FloatValue(14f));
        
        Assert.assertEquals(expected, returnedValues);
    }

    @Test
    public void getHourlyValues() throws Exception{
        ValueHistory history = new ValueHistory();
        
        history.add(TimeUtils.toInstant("2017-04-01 09:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-01 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-01 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-01 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:10"), 14f);
        
        List<Value> returnedValues = history.getHourlyValues(TimeUtils.toInstant("2017-04-01 10:20:02"));
        
        List<FloatValue> expected = Arrays.asList(new FloatValue(9f), new FloatValue(10f), new FloatValue(13f), new FloatValue(14f));
        
        Assert.assertEquals(expected, returnedValues);
    }
    
    @Test
    public void getDaylyValues() throws Exception{
        ValueHistory history = new ValueHistory();
        
        history.add(TimeUtils.toInstant("2016-03-07 10:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-07 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-08 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-09 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-09 10:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-10 11:20:10"), 14f);
        
        List<Value> returnedValues = history.getDaylyValues(TimeUtils.toInstant("2017-04-01 10:20:02"));
        
        List<FloatValue> expected = Arrays.asList(new FloatValue(9f), new FloatValue(10f), new FloatValue(13f));
        
        Assert.assertEquals(expected, returnedValues);
    }
    
    @Test
    public void getWeeklyValues() throws Exception{
        ValueHistory history = new ValueHistory();
        
        history.add(TimeUtils.toInstant("2016-03-05 10:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-03 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-03 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-10 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-10 10:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-17 11:20:10"), 14f);
        
        List<Value> returnedValues = history.getWeeklyValues(TimeUtils.toInstant("2017-04-17 10:20:02"));
        
        List<FloatValue> expected = Arrays.asList(new FloatValue(10f), new FloatValue(13f));
        
        Assert.assertEquals(expected, returnedValues);
    }
    
}
