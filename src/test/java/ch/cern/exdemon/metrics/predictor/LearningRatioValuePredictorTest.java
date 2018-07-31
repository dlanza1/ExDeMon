package ch.cern.exdemon.metrics.predictor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;

import org.junit.Test;

import ch.cern.exdemon.metrics.predictor.LearningRatioValuePredictor.Period;
import ch.cern.exdemon.metrics.predictor.LearningRatioValuePredictor.Status_;

public class LearningRatioValuePredictorTest {
    
    @Test
    public void serializationSize() throws IOException, ParseException{
        LearningRatioValuePredictor.Status_ store = new Status_();
        store.predictor = new LearningRatioValuePredictor(0.5f, Period.HOUR);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytes = bos.toByteArray();
        
        assertEquals(771, bytes.length);
    }
    
    @Test
    public void saveAndLoad() throws IOException, ParseException, ClassNotFoundException{
        LearningRatioValuePredictor.Status_ store = new Status_();
        store.predictor = new LearningRatioValuePredictor(0.5f, Period.HOUR);
        int numberOfRecords = 10;
        for (int i = 0; i < numberOfRecords; i++) 
            store.predictor.addValue(Instant.ofEpochSecond(Instant.now().getEpochSecond()), (float) Math.random());
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(store);
        out.flush();
        byte[] bytes = bos.toByteArray();
        out.close();
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Status_ restoredStore = (LearningRatioValuePredictor.Status_) ois.readObject();
        ois.close();
        
        assertNotSame(store.predictor, restoredStore.predictor);
        assertEquals(store.predictor.getPeriod(), restoredStore.predictor.getPeriod());
        assertEquals(store.predictor.getLearning_ratio(), restoredStore.predictor.getLearning_ratio(), 0f);
        assertTrue(Arrays.equals(store.predictor.getValues(), restoredStore.predictor.getValues()));
        assertTrue(Arrays.equals(store.predictor.getVariance(), restoredStore.predictor.getVariance()));
    }

    @Test
    public void noPrediction(){
        
        LearningRatioValuePredictor predictor = new LearningRatioValuePredictor(0.5f, Period.HOUR);
     
        Instant time = Instant.ofEpochSecond(10);
        
        try{
            predictor.getPredictionForTime(time);
            
            fail();
        }catch(RuntimeException e){}
        
        predictor.addValue(time, 10f);        
        assertEquals(10f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(1f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
    }
    
    @Test
    public void predictionForSameTime(){
        
        LearningRatioValuePredictor predictor = new LearningRatioValuePredictor(0.5f, Period.HOUR);
     
        Instant time = Instant.ofEpochSecond(10);
        
        predictor.addValue(time, 10f);        
        assertEquals(10f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(1f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
        
        predictor.addValue(time, 20f);        
        assertEquals(15f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(10f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
        
        predictor.addValue(time, 10f);        
        assertEquals(12.5f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(7.9f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
        
        predictor.addValue(time, 20f);        
        assertEquals(16.25f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(7.7f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
        
        predictor.addValue(time, 10f);        
        assertEquals(13.125f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(7f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
        
        predictor.addValue(time, 20f);        
        assertEquals(16.6f, predictor.getPredictionForTime(time).getValue(), 0.1f);
        assertEquals(6.9f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
        
        predictor.addValue(time, 10f);        
        assertEquals(13.2f, predictor.getPredictionForTime(time).getValue(), 0.1f);
        assertEquals(6.7f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
        
        predictor.addValue(time, 20f);        
        assertEquals(16.6f, predictor.getPredictionForTime(time).getValue(), 0.1f);
        assertEquals(6.7f, predictor.getPredictionForTime(time).getStandardDeviation(), 0.1f);
    }
    
    @Test
    public void predictionForSameTimeSameValue(){
        
        LearningRatioValuePredictor predictor = new LearningRatioValuePredictor(0.5f, Period.HOUR);
     
        Instant time = Instant.ofEpochSecond(10);
        
        predictor.addValue(time, 10f);        
        assertEquals(10f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(1f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
        
        predictor.addValue(time, 10f);        
        assertEquals(10f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(0f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
        
        predictor.addValue(time, 10f);        
        assertEquals(10f, predictor.getPredictionForTime(time).getValue(), 0f);
        assertEquals(0f, predictor.getPredictionForTime(time).getStandardDeviation(), 0f);
    }
    
}
