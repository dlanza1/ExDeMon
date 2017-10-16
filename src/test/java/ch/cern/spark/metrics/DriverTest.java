package ch.cern.spark.metrics;

import static org.junit.Assert.*;

import org.junit.Test;

import ch.cern.PropertiesTest;
import ch.cern.Properties.PropertiesCache;

public class DriverTest {

    @Test
    public void notConfiguredMetricSource() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        props.get().setProperty("spark.driver.allowMultipleContexts", "true");
        
        try{
            Driver driver = new Driver(props);

            driver.createNewStreamingContext();
            
            fail();
        }catch(RuntimeException e){
            assertEquals("A metric source must be configured", e.getMessage());
        }
    }
    
    @Test
    public void notConfiguredSinks() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        props.get().setProperty("spark.driver.allowMultipleContexts", "true");
        props.get().setProperty("source.type", "kafka");
        props.get().setProperty("source.topics", "topic");
        props.get().setProperty("source.parser.attributes", "att1 att2");
        
        try{
        		Driver driver = new Driver(props);
        	
            driver.createNewStreamingContext();
            
            fail();
        }catch(RuntimeException e){
            assertEquals("At least one sink must be configured", e.getMessage());
        }
    }
    
    @Test
    public void configurationWithAnalysisResultsSink() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        props.get().setProperty("spark.driver.allowMultipleContexts", "true");
        props.get().setProperty("source.type", "kafka");
        props.get().setProperty("source.topics", "topic");
        props.get().setProperty("source.parser.attributes", "att1 att2");
        props.get().setProperty("results.sink.type", "elastic");
        
        Driver driver = new Driver(props);
        
        driver.createNewStreamingContext();
    }
    
    @Test
    public void configurationWithNotificationsSink() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        props.get().setProperty("spark.driver.allowMultipleContexts", "true");
        props.get().setProperty("source.type", "kafka");
        props.get().setProperty("source.topics", "topic");
        props.get().setProperty("source.parser.attributes", "att1 att2");
        props.get().setProperty("notifications.sink.type", "elastic");
        
        Driver driver = new Driver(props);
        
        driver.createNewStreamingContext();
    }
    
}
