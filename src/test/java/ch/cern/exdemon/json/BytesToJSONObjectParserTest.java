package ch.cern.exdemon.json;

import org.junit.Assert;

import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.json.JSONObjectDeserializer;

public class BytesToJSONObjectParserTest {

    public void parse(){
    
        String json = "{\"metadata.service_name\":\"timtest_s.cern.ch\",\"data.return_code\":0}";
    
        try {
            @SuppressWarnings("resource")
            JSONObjectDeserializer jsonObjectDeserializer = new JSONObjectDeserializer();
            
            JSON listenerEvent = jsonObjectDeserializer.deserialize(null, json.getBytes());
            
            Assert.assertNotNull(listenerEvent.getProperty("metadata.service_name"));
            Assert.assertEquals("timtest_s.cern.ch", listenerEvent.getProperty("metadata.service_name"));
            
            Assert.assertNotNull(listenerEvent.getProperty("data.return_code"));
            Assert.assertEquals("0", listenerEvent.getProperty("data.return_code"));
    
        } catch (Exception e) {
            e.printStackTrace();
            
            Assert.fail();
        }
    }
    
}
