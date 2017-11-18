package ch.cern.spark.metrics.defined;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;

public class UpdateDefinedMetricStatusesFTest {
	
	private Cache<Properties> propertiesCache = Properties.getCache();

	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		propertiesCache = Properties.getCache();
		propertiesCache.reset();
		DefinedMetrics.getCache().reset();
	}

    @Test
    public void shouldGenerateWhenUpdatingVariable() throws Exception {
    		propertiesCache.get().setProperty("metrics.define.dmID1.metrics.groupby", "DB_NAME, METRIC_NAME");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.aggregate", "count_floats");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.expire", "10s");

        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesF(null);

        DefinedMetricID id = new DefinedMetricID("dmID1", new HashMap<>());
        State<DefinedMetricStore> status = new StateImpl<>();
        Optional<Metric> metricOpt= null;
        
        metricOpt = Optional.of(Metric(0, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_1", "METRIC_NAME=Read"));
        Optional<Metric> result = func.call(null, id, metricOpt, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(0, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_2", "METRIC_NAME=Read"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(10, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_1", "METRIC_NAME=Read"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(10, 0f, "DB_NAME=DB1", "INSTANCE_NAME=DB1_2", "METRIC_NAME=Read"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);
    }
    
    @Test
    public void shouldAggregateWhenGroupByIncludeAllAttributes() throws Exception {
    		propertiesCache.get().setProperty("metrics.define.dmID1.metrics.groupby", "INSTANCE_NAME");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.aggregate", "count_floats");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.expire", "5s");

        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesF(null);

        Map<String, String> groupByIDs = new HashMap<>();
        groupByIDs.put("INSTANCE_NAME", "DB1_1");
        DefinedMetricID id = new DefinedMetricID("dmID1", groupByIDs );
        State<DefinedMetricStore> status = new StateImpl<>();
        Optional<Metric> metricOpt= null;
        
        metricOpt = Optional.of(Metric(0, 0f, "INSTANCE_NAME=DB1_1"));
        Optional<Metric> result = func.call(null, id, metricOpt, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(1, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(2, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
    }

    @Test
    public void shouldExpireValuesWhenGroupByIncludeAllAttributes() throws Exception {
    		propertiesCache.get().setProperty("metrics.define.dmID1.metrics.groupby", "INSTANCE_NAME");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.aggregate", "count_floats");
    		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.expire", "5s");

        UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesF(null);

        Map<String, String> groupByIDs = new HashMap<>();
        groupByIDs.put("INSTANCE_NAME", "DB1_1");
        DefinedMetricID id = new DefinedMetricID("dmID1", groupByIDs );
        State<DefinedMetricStore> status = new StateImpl<>();
        Optional<Metric> metricOpt= null;
        
        metricOpt = Optional.of(Metric(0, 0f, "INSTANCE_NAME=DB1_1"));
        Optional<Metric> result = func.call(null, id, metricOpt, status);
        assertEquals(1, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(2, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(2, result.get().getValue().getAsFloat().get(), 0.001f);

        metricOpt = Optional.of(Metric(4, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
        
        //Metric at time 0 expired
        metricOpt = Optional.of(Metric(6, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
        
        //Metric at time 1 expired
        metricOpt = Optional.of(Metric(8, 0f, "INSTANCE_NAME=DB1_1"));
        result = func.call(null, id, metricOpt, status);
        assertEquals(3, result.get().getValue().getAsFloat().get(), 0.001f);
    }
    
}
