package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ch.cern.Properties.PropertiesCache;
import ch.cern.PropertiesTest;

public class DefinedMetricsTest {
	
	@Test
	public void configNotValid() throws Exception {
		
		PropertiesCache props = PropertiesTest.mockedExpirable();
		
		//Value must be specified.
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.md1.metric.y.filter.attribute.AA", "metricAA");
		DefinedMetrics definedMetrics = new DefinedMetrics(props);
		Map<String, DefinedMetric> map = definedMetrics.get();
		assertEquals(0, map.size());
		
		//At least a metric must be described.
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.md1.value", "x * 10");
		definedMetrics = new DefinedMetrics(props);
		map = definedMetrics.get();
		assertEquals(0, map.size());
		
		//Equation contain variables that have not been described.
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.md1.value", "x * 10");
		props.get().setProperty("metrics.define.md1.metric.y.filter.attribute.AA", "metricAA");
		definedMetrics = new DefinedMetrics(props);
		map = definedMetrics.get();
		assertEquals(0, map.size());
	}

    public static DefinedMetrics mockedExpirable() {
    		DefinedMetrics propExp = mock(DefinedMetrics.class, withSettings().serializable());
        
        try {
            when(propExp.get()).thenReturn(new HashMap<>());
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return propExp;
    }
    
}
