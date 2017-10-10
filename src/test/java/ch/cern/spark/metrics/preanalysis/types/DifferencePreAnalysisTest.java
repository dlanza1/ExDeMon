package ch.cern.spark.metrics.preanalysis.types;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.time.Instant;

import org.junit.Test;

public class DifferencePreAnalysisTest {
 
    private DifferencePreAnalysis preAnalysis;
    
    private void getInstance() throws Exception {
        preAnalysis = new DifferencePreAnalysis();
    }
    
    @Test
    public void saveAndLoad() throws Exception{
        getInstance();
        preAnalysis.process(Instant.now(), 10f);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);   
        out.writeObject(preAnalysis.save());
        out.flush();
        byte[] bytes = bos.toByteArray();
        out.close();
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        DifferencePreAnalysis.Store_ restoredStore = (DifferencePreAnalysis.Store_) ois.readObject();
        ois.close();
        
        getInstance();
        preAnalysis.load(restoredStore);
        assertEquals(5f, preAnalysis.process(Instant.ofEpochSecond(20), 15f), 0f);
    }
    
    @Test
    public void average() throws Exception{
        getInstance();
        
        assertEquals(0f, preAnalysis.process(Instant.ofEpochSecond(20), 10f), 0f);
        assertEquals(10f, preAnalysis.process(Instant.ofEpochSecond(30), 20f), 0f);
        assertEquals(13f, preAnalysis.process(Instant.ofEpochSecond(40), 33f), 0f);
    }
    
}
