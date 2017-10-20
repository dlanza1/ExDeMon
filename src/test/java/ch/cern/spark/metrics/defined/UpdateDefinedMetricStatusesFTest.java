package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.PropertiesTest;
import ch.cern.spark.metrics.Metric;
import ch.cern.Properties.PropertiesCache;

public class UpdateDefinedMetricStatusesFTest {

	@Test
	public void aggregateCountUpdate() throws Exception {
		PropertiesCache props = PropertiesTest.mockedExpirable();
		
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.dmID1.metrics.groupby", "DB_NAME, METRIC_NAME");
		props.get().setProperty("metrics.define.dmID1.variables.value.aggregate", "count");
		props.get().setProperty("metrics.define.dmID1.variables.value.expire", "5m");
		DefinedMetrics definedMetrics = new DefinedMetrics(props);
		
		UpdateDefinedMetricStatusesF func = new UpdateDefinedMetricStatusesF(definedMetrics);
		
		Instant now = Instant.now();
		
		DefinedMetricID id = new DefinedMetricID("dmID1", new HashMap<>());
		Map<String, String> ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		Metric metric = new Metric(now.plus(Duration.ofMinutes(1)), 0f, ids);
		Optional<Metric> metricOpt = Optional.of(metric);
		State<DefinedMetricStore> status = new StateImpl<>();
		Optional<Metric> result = func.call(new Time(now.toEpochMilli()), id, metricOpt, status);
		assertEquals(1, result.get().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		metric = new Metric(now.plus(Duration.ofMinutes(1)), 0f, ids);
		metricOpt = Optional.of(metric);
		result = func.call(new Time(now.toEpochMilli()), id, metricOpt, status);
		assertEquals(2, result.get().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1"); // was before
		ids.put("METRIC_NAME", "Read");
		metric = new Metric(now.plus(Duration.ofMinutes(2)), 0f, ids);
		metricOpt = Optional.of(metric);
		result = func.call(new Time(now.toEpochMilli()), id, metricOpt, status);
		assertEquals(2, result.get().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2"); // was before
		ids.put("METRIC_NAME", "Read");
		metric = new Metric(now.plus(Duration.ofMinutes(2)), 0f, ids);
		metricOpt = Optional.of(metric);
		result = func.call(new Time(now.toEpochMilli()), id, metricOpt, status);
		assertEquals(2, result.get().getValue(), 0.001f);
	}
	
}
