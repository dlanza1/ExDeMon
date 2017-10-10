package ch.cern.spark.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.TimeUtils;
import ch.cern.spark.metrics.ValueHistory.Store_;

public class ValueHistoryTest {
    
    @Test
    public void serializationSize() throws IOException, ParseException{
        ValueHistory.Store_ store = new Store_();
        store.history = new ValueHistory(Duration.ofSeconds(60));
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesBefore = bos.toByteArray();
        
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.history.add(Instant.ofEpochSecond(Instant.now().getEpochSecond()), (float) Math.random());
        
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytesAfter = bos.toByteArray();
        
        assertTrue(bytesAfter.length > bytesBefore.length);
        
        int sizePerRecord = (bytesAfter.length - bytesBefore.length) / numberOfRecords;

        int int_size = 4; //time
        int float_size = 4; //value
        assertEquals(int_size + float_size, sizePerRecord);
    }
    
    @Test
    public void saveAndLoad() throws IOException, ParseException, ClassNotFoundException{
        Store_ store = new Store_();
        store.history = new ValueHistory(Duration.ofSeconds(60));
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
        Store_ restoredStore = (ValueHistory.Store_) ois.readObject();
        ois.close();
        
        assertNotSame(store.history.getDatedValues(), restoredStore.history.getDatedValues());
        assertEquals(store.history.getPeriod(), restoredStore.history.getPeriod());
        assertEquals(store.history.getDatedValues(), restoredStore.history.getDatedValues());
    }
    
    @Test
    public void expiration() throws Exception{
        ValueHistory history = new ValueHistory(Duration.ofSeconds(60));
        
        history.add(TimeUtils.toInstant("2017-04-01 11:18:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-01 11:18:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-01 11:19:12"), 11f);
        history.add(TimeUtils.toInstant("2017-04-01 11:19:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:10"), 14f);
        
        history.removeRecordsOutOfPeriodForTime(TimeUtils.toInstant("2017-04-01 11:20:22"));
        
        List<Float> returnedValues = history.getDatedValues().stream().map(value -> value.getValue()).collect(Collectors.toList());
        
        List<Float> expected = Arrays.asList(12f, 13f, 14f);
        
        Assert.assertEquals(expected, returnedValues);
    }

    @Test
    public void getHourlyValues() throws Exception{
        ValueHistory history = new ValueHistory(Duration.ofSeconds(50));
        
        history.add(TimeUtils.toInstant("2017-04-01 09:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-01 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-01 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-01 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-01 11:20:10"), 14f);
        
        List<Float> returnedValues = history.getHourlyValues(TimeUtils.toInstant("2017-04-01 10:20:02"));
        
        List<Float> expected = Arrays.asList(9f, 10f, 13f, 14f);
        
        Assert.assertEquals(expected, returnedValues);
    }
    
    @Test
    public void getDaylyValues() throws Exception{
        ValueHistory history = new ValueHistory(Duration.ofSeconds(50));
        
        history.add(TimeUtils.toInstant("2016-03-07 10:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-07 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-08 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-09 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-09 10:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-10 11:20:10"), 14f);
        
        List<Float> returnedValues = history.getDaylyValues(TimeUtils.toInstant("2017-04-01 10:20:02"));
        
        List<Float> expected = Arrays.asList(9f, 10f, 13f);
        
        Assert.assertEquals(expected, returnedValues);
    }
    
    @Test
    public void getWeeklyValues() throws Exception{
        ValueHistory history = new ValueHistory(Duration.ofSeconds(50));
        
        history.add(TimeUtils.toInstant("2016-03-05 10:20:12"), 9f);
        history.add(TimeUtils.toInstant("2017-04-03 10:20:56"), 10f);
        history.add(TimeUtils.toInstant("2017-04-03 10:21:34"), 11f);
        history.add(TimeUtils.toInstant("2017-04-10 10:22:31"), 12f);
        history.add(TimeUtils.toInstant("2017-04-10 10:20:01"), 13f);
        history.add(TimeUtils.toInstant("2017-04-17 11:20:10"), 14f);
        
        List<Float> returnedValues = history.getWeeklyValues(TimeUtils.toInstant("2017-04-17 10:20:02"));
        
        List<Float> expected = Arrays.asList(10f, 13f);
        
        Assert.assertEquals(expected, returnedValues);
    }
    
}
