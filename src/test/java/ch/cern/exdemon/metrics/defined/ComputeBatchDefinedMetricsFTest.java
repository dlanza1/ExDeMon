package ch.cern.exdemon.metrics.defined;

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

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.properties.Properties;
import scala.Tuple2;

public class ComputeBatchDefinedMetricsFTest {

	@Before
	public void reset() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("type", "test");
        ComponentsCatalog.init(properties);
	}

	@Test
	public void aggregateCountUpdate() throws Exception {
	    Properties properties = DefinedMetricTest.newProperties();
		properties.setProperty("metrics.groupby", "DB_NAME METRIC_NAME");
		properties.setProperty("variables.value.aggregate.type", "count_floats");
		properties.setProperty("variables.value.aggregate.attributes", "ALL");
		properties.setProperty("variables.value.expire", "5m");
		properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dmID1", properties);
		
		Instant batchTime = Instant.parse("2018-10-03T10:15:00.00Z");
		
		ComputeBatchDefineMetricsF func = new ComputeBatchDefineMetricsF(new Time(batchTime.toEpochMilli()), null);
		
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		State<VariableStatuses> status = new StateImpl<>();
		
		VariableStatuses varStores = new VariableStatuses();
		AggregationValues valueStore = new AggregationValues(100, 0);
		varStores.put("value", new ValueVariable.Status_(valueStore));
		
		Map<String, String> ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), 0f, batchTime);
		
		status.update(varStores);
		Iterator<Metric> result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), 0f, batchTime);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), new FloatValue(0), batchTime);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
		
		id = new DefinedMetricStatuskey("dmID1", new HashMap<>());
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		valueStore.add(ids.hashCode(), new FloatValue(0), batchTime);
		status.update(varStores);
		result = func.call(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, status.get()));
		result.hasNext();
		assertEquals(2, result.next().getValue().getAsFloat().get(), 0.001f);
	}
	
}
