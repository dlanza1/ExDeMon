package ch.cern.spark.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class DriverTest {

    @Test
    public void notConfiguredMetricSource() throws Exception{
    		Properties props = new Properties();
    		props.setProperty("spark.driver.allowMultipleContexts", "true");
        
        try{
            Driver driver = new Driver(props);

            driver.createNewStreamingContext(null);
            
            fail();
        }catch(ConfigurationException e){
            assertEquals("At least one metric source must be configured", e.getMessage());
        }
    }
    
    @Test
    public void notConfiguredSinks() throws Exception{
    		Properties props = new Properties();
		props.setProperty("spark.driver.allowMultipleContexts", "true");
    		props.setProperty("metrics.source.kafka.type", "kafka");
    		props.setProperty("metrics.source.kafka.topics", "topic");
        
        try{
        		Driver driver = new Driver(props);
        	
            driver.createNewStreamingContext(null);
            
            fail();
        }catch(ConfigurationException e){
            assertEquals("At least one sink must be configured", e.getMessage());
        }
    }
    
    @Test
    public void configurationWithAnalysisResultsSink() throws Exception{
    		Properties props = new Properties();
    		props.setProperty("spark.driver.allowMultipleContexts", "true");
    		props.setProperty("metrics.source.kafka.type", "kafka");
    		props.setProperty("metrics.source.kafka.topics", "topic");
    		props.setProperty("results.sink.type", "elastic");
        
        Driver driver = new Driver(props);
        
        driver.createNewStreamingContext(null);
    }
    
    @Test
    public void configurationWithNotificationsSink() throws Exception{
    		Properties props = new Properties();
    		props.setProperty("spark.driver.allowMultipleContexts", "true");
    		props.setProperty("metrics.source.kafka.type", "kafka");
    		props.setProperty("metrics.source.kafka.topics", "topic");
    		props.setProperty("notifications.sink.elastic.type", "elastic");
        
        Driver driver = new Driver(props);
        
        driver.createNewStreamingContext(null);
    }
    
}
