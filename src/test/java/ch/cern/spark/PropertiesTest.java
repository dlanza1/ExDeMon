package ch.cern.spark;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties.Expirable;

public class PropertiesTest {
    
    @Test
    public void globalParametersNull(){
        Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop1", "val2");
        
        Properties subProp = prop.getSubset("prop2");
        
        Assert.assertEquals(1, subProp.size());
        Assert.assertEquals("val2", subProp.get("prop1"));
    }

	@Test
	public void expiration() throws IOException{
		Properties.Expirable prop = new Properties.Expirable("src/test/resources/config.properties");
		
		Properties p1 = prop.get();
		Properties p2 = prop.get();
		
		Assert.assertSame(p1, p2);
	}
	
	@Test
	public void getUniqueKeyFields() {
		Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop21", "val2");
        prop.setProperty("prop2.prop22", "val2");
        prop.setProperty("prop2.prop23", "val2");
        prop.setProperty("prop3.prop31", "val2");
        prop.setProperty("prop3.prop32", "val2");
        
        Object[] uniq = prop.getUniqueKeyFields().toArray();
        String[] expectedValue = {"prop2", "prop1", "prop3"};
        Assert.assertArrayEquals(expectedValue, uniq);
	}

    public static Expirable mockedExpirable() {
        Properties.Expirable propExp = mock(Properties.Expirable.class, withSettings().serializable());
        
        try {
            when(propExp.get()).thenReturn(new Properties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return propExp;
    }
	
}
