package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import ch.cern.Properties.PropertiesCache;
import ch.cern.PropertiesTest;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricsTest {
	
	@Test
	public void configNotValid() throws Exception {
		PropertiesCache props = PropertiesTest.mockedExpirable();
		
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.md1.value", "x"); //diferent variable declared
		props.get().setProperty("metrics.define.md1.variables.y.filter.attribute.AA", "metricAA");
		
		DefinedMetrics definedMetrics = new DefinedMetrics(props);
		
		Map<String, DefinedMetric> map = definedMetrics.get();
		assertEquals(0, map.size());
	}

	public void wholePipe() throws ClassNotFoundException, IOException {
		
		PropertiesCache propertiesCache = PropertiesTest.mockedExpirable();
		
		DefinedMetrics definedMetrics = new DefinedMetrics(propertiesCache);
		
		JavaDStream<Metric> metricsJStream = null;
		Stream<Metric> metricsStream = Stream.from(metricsJStream );
		
		Stream<Metric> results = definedMetrics.generate(metricsStream);
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
