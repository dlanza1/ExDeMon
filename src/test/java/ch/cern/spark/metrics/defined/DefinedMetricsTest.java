package ch.cern.spark.metrics.defined;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;

import org.junit.Test;

import ch.cern.Properties.PropertiesCache;
import ch.cern.PropertiesTest;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricsTest extends StreamTestHelper<Metric, Metric> {

	@Test
	public void shouldGenerateMetrics() throws Exception {
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 20f, "HOSTNAME=host1"));
        addInput(1,    Metric(2, 30f, "HOSTNAME=host2"));
        addExpected(1, Metric(2, 50f, "$defined_metric=dm1"));
		
		PropertiesCache propertiesCache = PropertiesTest.mockedExpirable();
		propertiesCache.get().setProperty("metrics.define.dm1.variables.a.filter.attributes.HOSTNAME", ".*");
		propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate", "sum");
		propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
		
		DefinedMetrics definedMetrics = new DefinedMetrics(propertiesCache);
	        
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> results = definedMetrics.generate(metricsStream);
        
        assertExpected(results);
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
