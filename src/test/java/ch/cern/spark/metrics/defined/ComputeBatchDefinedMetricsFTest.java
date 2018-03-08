package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.spark.metrics.value.FloatValue;
import scala.Tuple2;

public class ComputeBatchDefinedMetricsFTest {
	
	private Cache<Properties> propertiesCache = Properties.getCache();
	
	@Before
	public void reset() throws ConfigurationException {
		Properties propertiesSourceProps = new Properties();
		propertiesSourceProps.setProperty("type", "static");
        Properties.initCache(propertiesSourceProps);
		propertiesCache = Properties.getCache();
		propertiesCache.reset();
		DefinedMetrics.getCache().reset();
	}

	@Test
	public void aggregateCountUpdate() throws Exception {
		propertiesCache.get().setProperty("metrics.define.dmID1.metrics.groupby", "DB_NAME METRIC_NAME");
		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.aggregate.type", "count_floats");
		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.aggregate.attributes", "ALL");
		propertiesCache.get().setProperty("metrics.define.dmID1.variables.value.expire", "5m");
		propertiesCache.get().setProperty("metrics.define.dmID1.when", "batch");
		
		Instant now = Instant.now();
		
		ComputeBatchDefineMetricsF func = new ComputeBatchDefineMetricsF(new Time(now.toEpochMilli()), null);
		
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		State<VariableStatuses> status = new StateImpl<>();
		
		VariableStatuses varStores = new VariableStatuses();
		AggregationValues valueStore = new AggregationValues(100);
		varStores.put("value", valueStore);
		
		Map<String, String> ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), 0f, now);
		
		status.update(varStores);
		Iterator<Metric> result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), 0f, now);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), new FloatValue(0), now);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), new FloatValue(0), now);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
	}
	
}
