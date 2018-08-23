package ch.cern.exdemon.metrics.defined;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class DefinedMetricTest {
    
    public static Properties newProperties() {
        Properties props = new Properties();
        
        props.setProperty("spark.batch.time", "1s");
        
        return props;
    }
	
	@Test
	public void configNotValid() throws ConfigurationException {
		Properties props = newProperties();
		
		DefinedMetric metric = new DefinedMetric("test");
		ConfigurationResult configResult = metric.config(props);
		assertEquals("value: must be configured", configResult.getErrors().get(0).toString());
		
		props = newProperties();
		props.setProperty("value", "x * 10");
		props.setProperty("variables.y.filter.attribute.AA", "metricAA");
		metric = new DefinedMetric("test");
		configResult = metric.config(props);
		assertEquals("value: java.text.ParseException: Unknown variable: x", configResult.getErrors().get(0).toString());
		
		props = newProperties();
		props.setProperty("value", "x * 10");
		props.setProperty("when", "y");
		props.setProperty("variables.x.filter.attribute.AA", "metricAA");
		metric = new DefinedMetric("test");
		configResult = metric.config(props);
		assertEquals("when: Variables listed in when parameter must be declared (missing: [y]).", configResult.getErrors().get(0).toString());
		
		props = newProperties();
		props.setProperty("value", "trim(count)");
		props.setProperty("variables.count.aggregate.type", "count_strings");
		metric = new DefinedMetric("test");
		configResult = metric.config(props);
		assertEquals("value.count: variable count returns type FloatValue because of its aggregation operation, "
						+ "but in the equation there is a function that uses it as type StringValue", configResult.getErrors().get(0).toString());
		
	}

    @Test
	public void config() throws ConfigurationException {
		DefinedMetric metric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		metric.config(properties);
		
		assertEquals("A", metric.getId());
		assertNotNull(metric.getEquation());
		assertNotNull(metric.getEquation().getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getEquation().getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec", "HostCPUUsagePerSec")), 
		             metric.getWhen().getVariables().stream().map(v -> v.getName()).collect(Collectors.toSet()));
		
		properties.setProperty("when", "DBCPUUsagePerSec HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("HostCPUUsagePerSec", "DBCPUUsagePerSec")), 
		             metric.getWhen().getVariables().stream().map(v -> v.getName()).collect(Collectors.toSet()));
	}
	
	@Test
	public void configAggregaate() throws ConfigurationException {
		DefinedMetric metric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.aggregate", "sum");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		metric.config(properties);
		
		assertEquals("A", metric.getId());
		assertNotNull(metric.getEquation());
		assertNotNull(metric.getEquation().getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getEquation().getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec", "HostCPUUsagePerSec")), 
		             metric.getWhen().getVariables().stream().map(v -> v.getName()).collect(Collectors.toSet()));
		
		properties.setProperty("when", "DBCPUUsagePerSec HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("HostCPUUsagePerSec", "DBCPUUsagePerSec")), 
		             metric.getWhen().getVariables().stream().map(v -> v.getName()).collect(Collectors.toSet()));
	}
	
	@Test
	public void testIfApplyForAnyVariable() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Metric metric = Metric(0, 0f, "INSTANCE_NAME=machine");
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
		
		metric = Metric(0, 0f, "INSTANCE_NAME=machine", "METRIC_NAME=Host CPU Usage Per Sec");
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
		
		metric = Metric(0, 0f, "INSTANCE_NAME=machine", "METRIC_NAME=Not included");
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void testIfApplyForAnyVariableOneWithAggregation() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("variables.DBCPUUsagePerSec.aggreagtion", "sum");
		definedMetric.config(properties);

		Metric metric = Metric(0, 0f, "INSTANCE_NAME=machine");
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void getGroupByMetricAttributess() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(0, definedMetric.getGroupByAttributes(ids).get().size());
		
		
		properties.setProperty("metrics.groupby", "ALL");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByAttributes(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByAttributes(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByAttributes(ids).get().get("METRIC_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(1, definedMetric.getGroupByAttributes(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByAttributes(ids).get().get("INSTANCE_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME METRIC_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByAttributes(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByAttributes(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByAttributes(ids).get().get("METRIC_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME METRIC_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByAttributes(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByAttributes(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByAttributes(ids).get().get("METRIC_NAME"));
	}
	
    @Test
    @Deprecated
    public void fixedValueAttributesDeprecated() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("metrics.attribute.A", "A1");
        properties.setProperty("metrics.attribute.B", "B2");
        properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());

        assertEquals("A1", generatedMetric.get().getAttributes().get("A"));
        assertEquals("B2", generatedMetric.get().getAttributes().get("B"));
    }
	
    @Test
    public void fixedValueAttributes() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("metrics.attribute.A.fixed", "A1");
        properties.setProperty("metrics.attribute.B.fixed", "B2");
        properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());

        assertEquals("A1", generatedMetric.get().getAttributes().get("A"));
        assertEquals("B2", generatedMetric.get().getAttributes().get("B"));
    }
    
    @Test
    public void variableAttributes() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("metrics.attribute.A.variable", "varA");
        properties.setProperty("metrics.attribute.B.variable", "varB");
        properties.setProperty("value", "value");
        properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "Read Bytes");
        properties.setProperty("variables.varA.filter.attribute.METRIC_NAME", "Read Bytes");
        properties.setProperty("variables.varA.attribute", "HOSTNAME");
        properties.setProperty("variables.varB.fixed.value", "BbB");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
        definedMetric.updateStore(store, metric, new HashSet<>());
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());

        assertEquals("host1", generatedMetric.get().getAttributes().get("A"));
        assertEquals("BbB", generatedMetric.get().getAttributes().get("B"));
    }

    @Test
    public void attributesFromTriggeringMetric() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("metrics.attribute.A.triggering", "HOSTNAME");
        properties.setProperty("metrics.attribute.B.triggering", "METRIC_NAME");
        properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());

        assertEquals("host1", generatedMetric.get().getAttributes().get("A"));
        assertEquals("Read Bytes", generatedMetric.get().getAttributes().get("B"));
    }
    
    @Test
    public void merge() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("value", "value");
        properties.setProperty("variables.value.filter.attribute.VAR", "value");
        properties.setProperty("variables.value.merge.variables", "varA varB");
        properties.setProperty("variables.varA.filter.attribute.VAR", "A");
        properties.setProperty("variables.varA.fixed.value", "Avalue");
        properties.setProperty("variables.varB.filter.attribute.VAR", "B");
        properties.setProperty("variables.varB.fixed.value", "Bvalue");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(1, "Vvalue", "VAR=value");
        definedMetric.updateStore(store, metric, new HashSet<>());
        Optional<Metric> generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
        assertEquals("Vvalue", generatedMetric.get().getValue().getAsString().get());
        
        metric = Metric(2, "does_not_matter", "VAR=A");
        definedMetric.updateStore(store, metric, new HashSet<>());
        generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
        assertEquals("Avalue", generatedMetric.get().getValue().getAsString().get());
        
        metric = Metric(3, "does_not_matter", "VAR=B");
        definedMetric.updateStore(store, metric, new HashSet<>());
        generatedMetric = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
        assertEquals("Bvalue", generatedMetric.get().getValue().getAsString().get());
    }
    
    @Test
    public void delayInVariable() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("value", "value");
        properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "Read Bytes");
        properties.setProperty("variables.value.timestamp.shift", "+1h");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
        definedMetric.updateStore(store, metric, new HashSet<>());

        assertEquals(Instant.EPOCH.plus(Duration.ofHours(1)), store.get("value").getLastUpdateMetricTime());
    }
	
	@Test
	public void computeWhenVariableIsNotInEqaution() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "running_count");
		properties.setProperty("when", "trigger");
		properties.setProperty("variables.running_count.filter.attribute.TYPE", "Running");
		properties.setProperty("variables.running_count.aggregate.type", "count_floats");
		properties.setProperty("variables.running_count.expire", "10m");
		properties.setProperty("variables.trigger.filter.attribute.TYPE", "Trigger");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10f, "HOSTNAME=host1", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host2", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(2)), 13, "TYPE=Trigger");
		definedMetric.updateStore(store, metric, null);
		Optional<Metric> result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals(2f, result.get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(4)), 13, "TYPE=Trigger");
		definedMetric.updateStore(store, metric, null);
		result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals(3f, result.get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateCountWhenBatch() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("when", "batch");
		properties.setProperty("variables.running_count.filter.attribute.TYPE", "Running");
		properties.setProperty("variables.running_count.aggregate.type", "count_floats");
		properties.setProperty("variables.running_count.aggregate.attributes", "ALL");
		properties.setProperty("variables.running_count.expire", "10m");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();
		
		Instant batchTime = Instant.parse("2017-02-03T10:15:00.00Z");
		
		Metric metric = Metric(batchTime, 10f, "HOSTNAME=host1", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host2", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertEquals(2f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(2)), 13, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(batchTime.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		assertEquals(3f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(9)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(3f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(10)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(2f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(11)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(1f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(12)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(1f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(13)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(0f, definedMetric.generateByBatch(store, batchTime.plus(Duration.ofMinutes(14)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateCountWithSelect() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("when", "batch");
		properties.setProperty("variables.running_count.aggregate.type", "count_floats");
		properties.setProperty("variables.running_count.aggregate.attributes", "HOSTNAME");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();
		
		Instant batchTime = Instant.parse("2007-12-03T10:15:00.00Z");
		
		Metric metric = Metric(batchTime, 10f, "HOSTNAME=host1", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host1", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(1f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(2)), 13, "HOSTNAME=host2", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertEquals(2f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host2", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(2f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(4)), 13, "HOSTNAME=host3", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(5)), 7, "HOSTNAME=host3", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(6)), 7, "HOSTNAME=host3", "TYPE=NETWORK");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateCountWithSelectAll() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("when", "batch");
		properties.setProperty("variables.running_count.aggregate.type", "count_floats");
		properties.setProperty("variables.running_count.aggregate.attributes", "ALL");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();
		
		Instant batchTime = Instant.parse("2007-12-03T10:15:00.00Z");
		
		Metric metric = Metric(batchTime, 10f, "HOSTNAME=host1", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host1", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(2f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(2)), 13, "HOSTNAME=host2", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host2", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(4f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(4)), 13, "HOSTNAME=host3", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertEquals(5f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(5)), 7, "HOSTNAME=host3", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(6f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(6)), 7, "HOSTNAME=host3", "TYPE=NETWORK");
		definedMetric.updateStore(store, metric, null);
		assertEquals(7f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(7)), 7, "HOSTNAME=host1", "TYPE=CPU");
		definedMetric.updateStore(store, metric, null);
		assertEquals(7f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(batchTime.plus(Duration.ofMinutes(8)), 7, "HOSTNAME=host1", "TYPE=MEMORY");
		definedMetric.updateStore(store, metric, null);
		assertEquals(7f, definedMetric.generateByBatch(store, metric.getTimestamp(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateSumMetric() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("variables.readbytestotal.filter.attribute.TYPE", "Read Bytes");
		properties.setProperty("variables.readbytestotal.aggregate.type", "sum");
		properties.setProperty("variables.readbytestotal.aggregate.attributes", "ALL");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();;
		
		Metric metric = Metric(Instant.now(), 10, "HOSTNAME=host1", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric, null);
		assertEquals(10f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(Instant.now(), 13, "HOSTNAME=host2", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric, null);
		assertEquals(23f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(Instant.now(), 13, "HOSTNAME=host3", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric, null);
		assertEquals(36f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(Instant.now(), 7, "HOSTNAME=host3", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric, null);
		assertEquals(30f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
    public void computeCountBetweenExpireAndIgnore() throws ConfigurationException, CloneNotSupportedException {
        
        DefinedMetric definedMetric = new DefinedMetric("A");
        
        Properties properties = newProperties();
        properties.setProperty("variables.readbytestotal.aggregate.type", "count_floats");
        properties.setProperty("variables.readbytestotal.ignore", "0h,h");
        properties.setProperty("variables.readbytestotal.expire", "1h,h");
        definedMetric.config(properties);
        
        VariableStatuses store = new VariableStatuses();
        
        Instant now = Instant.parse("2017-12-10T22:00:00Z");
        
        Metric metric = Metric(now.plus(Duration.ofMinutes(0)), 10, "HOSTNAME=host1");
        definedMetric.updateStore(store, metric, null);
        assertEquals(0f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(now.plus(Duration.ofMinutes(20)), 10, "HOSTNAME=host1");
        definedMetric.updateStore(store, metric, null);
        assertEquals(0f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(now.plus(Duration.ofMinutes(40)), 10, "HOSTNAME=host1");
        definedMetric.updateStore(store, metric, null);
        assertEquals(0f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(now.plus(Duration.ofMinutes(60)), 10, "HOSTNAME=host1");
        definedMetric.updateStore(store, metric, null);
        assertEquals(3f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(now.plus(Duration.ofMinutes(80)), 10, "HOSTNAME=host1");
        definedMetric.updateStore(store, metric, null);
        assertEquals(3f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
    }
	
	@Test
	public void neverExpire() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.expire", "never");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		definedMetric.config(properties);
		
		VariableStatuses stores = new VariableStatuses();;
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10, "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertTrue(definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).isPresent());
		assertTrue(definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsException().isPresent());
		
		metric = Metric(now.plus(Duration.ofHours(20)), 7, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(17f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(40)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(18f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(60)), 9, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(19f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(80)), 10, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(20f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}

	@Test
	public void valueExpire() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.expire", "1m");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		properties.setProperty("variables.trigger.filter.attribute.METRIC_NAME", ".*");
		definedMetric.config(properties);
		
		VariableStatuses stores = new VariableStatuses();;
		
		Instant now = Instant.now();
	
		Metric metric = Metric(now, 10, "METRIC_NAME=None");
		definedMetric.updateStore(stores, metric, null);
		Optional<Metric> result = definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals("Variable writebytestotal: no values, Variable readbytestotal: no values", result.get().getValue().getAsException().get());
		assertEquals("(last(var(readbytestotal))={Error: no values} + last(var(writebytestotal))={Error: no values})={Error: in arguments}", result.get().getValue().getSource());
		
		metric = Metric(now, 10, "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		result = definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals("Variable writebytestotal: no values", result.get().getValue().getAsException().get());
		assertEquals("(last(var(readbytestotal))=10.0 + last(var(writebytestotal))={Error: no values})={Error: in arguments}", result.get().getValue().getSource());
		
		metric = Metric(now.plus(Duration.ofSeconds(20)), 7, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(17f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(40)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(18f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(60)), 9, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(19f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// Read Bytes has not been updated for more than 1 minute, so has expired and computation cannot be performed
		metric = Metric(now.plus(Duration.ofSeconds(80)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertTrue(definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).isPresent());
		assertTrue(definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsException().isPresent());
	}
	
    @Test
    public void valueIgnored() throws ConfigurationException, CloneNotSupportedException {

        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("value", "readbytestotal + writebytestotal");
        properties.setProperty("when", "ANY");
        properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
        properties.setProperty("variables.readbytestotal.ignore", "1m");
        properties.setProperty("variables.readbytestotal.expire", "2m");
        properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
        properties.setProperty("variables.trigger.filter.attribute.METRIC_NAME", ".*");
        definedMetric.config(properties);

        VariableStatuses stores = new VariableStatuses();

        Instant now = Instant.now();

        Metric metric = Metric(now, 10, "METRIC_NAME=Read Bytes");
        definedMetric.updateStore(stores, metric, null);
        Optional<Metric> result = definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>());
        assertTrue(result.isPresent());
        assertEquals("Variable writebytestotal: no values, Variable readbytestotal: no values", result.get().getValue().getAsException().get());
        assertEquals("(last(var(readbytestotal))={Error: no values} + last(var(writebytestotal))={Error: no values})={Error: in arguments}", result.get().getValue().getSource());

        metric = Metric(now.plus(Duration.ofSeconds(20)), 7, "METRIC_NAME=Write Bytes");
        definedMetric.updateStore(stores, metric, null);
        result = definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>());
        assertTrue(result.isPresent());
        assertEquals("Variable readbytestotal: no values", result.get().getValue().getAsException().get());
        assertEquals("(last(var(readbytestotal))={Error: no values} + last(var(writebytestotal))=7.0)={Error: in arguments}", result.get().getValue().getSource());

        metric = Metric(now.plus(Duration.ofSeconds(80)), 8, "METRIC_NAME=Write Bytes");
        definedMetric.updateStore(stores, metric, null);
        assertEquals(18f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);

        metric = Metric(now.plus(Duration.ofSeconds(90)), 9, "METRIC_NAME=Write Bytes");
        definedMetric.updateStore(stores, metric, null);
        assertEquals(19f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
    }
	
	@Test
	public void valueExpireInAggregation() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.aggregate.type", "sum");
		properties.setProperty("variables.readbytestotal.aggregate.attributes", "ALL");
		properties.setProperty("variables.readbytestotal.expire", "1m");
		definedMetric.config(properties);
		
		VariableStatuses stores = new VariableStatuses();;
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(10f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(20)), 13, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(23f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(40)), 13, "HOSTNAME=host3", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(36f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(now.plus(Duration.ofSeconds(60)), 7, "HOSTNAME=host3", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(30f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// host1 has not been updated for more than 1 minute, so his value is removed
		// same host -> update value
		metric = Metric(now.plus(Duration.ofSeconds(80)), 8, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(15f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void aggregateWithNoFilter() throws ConfigurationException, CloneNotSupportedException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("variables.readbytestotal.aggregate.type", "sum");
		properties.setProperty("variables.readbytestotal.aggregate.attributes", "ALL");
		definedMetric.config(properties);
		
		VariableStatuses stores = new VariableStatuses();;
		
		Metric metric = Metric(0, 10, "HOSTNAME=host1");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(10f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(0, 13, "HOSTNAME=host2");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(23f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(0, 3, "HOSTNAME=host3");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(26f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(0, 7, "HOSTNAME=host3");
		definedMetric.updateStore(stores, metric, null);
		assertEquals(30f, definedMetric.generateByUpdate(stores, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void filterInVariable() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = newProperties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		definedMetric.config(properties);
		
		VariableStatuses store = new VariableStatuses();;
		
		Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(Instant.now(), 14, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		//filtered out
		metric = Metric(Instant.now(), 15, "HOSTNAME=host3", "METRIC_NAME=Write Bytes");
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
	}
	
    @Test
    public void lastSourceMericsFromVariables() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("metrics.last_source_metrics.variables", "var2 var3");
        properties.setProperty("value", "var1 + var2");
        properties.setProperty("when", "var1");
        properties.setProperty("variables.var1.filter.attribute.A", "A");
        properties.setProperty("variables.var1.aggregate.latest-metrics.max-size", "1");
        properties.setProperty("variables.var2.filter.attribute.B", "B");
        properties.setProperty("variables.var2.aggregate.latest-metrics.max-size", "1");
        properties.setProperty("variables.var3.filter.attribute.C", "C");
        properties.setProperty("variables.var3.aggregate.latest-metrics.max-size", "2");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Instant batchTime = Instant.parse("2007-12-03T10:15:00.00Z");
        
        Metric metricA = Metric(batchTime.plus(Duration.ofMinutes(1)), 1f, "A=A");
        definedMetric.updateStore(store, metricA, null);
        
        Metric metricB = Metric(batchTime.plus(Duration.ofMinutes(2)), 2f, "B=B");
        definedMetric.updateStore(store, metricB, null);
        
        Metric metricC = Metric(batchTime.plus(Duration.ofMinutes(3)), 2f, "C=C");
        definedMetric.updateStore(store, metricC, null);
        
        List<Metric> lastSourceMetrics = definedMetric.generateByUpdate(store, metricA, new HashMap<>()).get().getValue().getLastSourceMetrics();
        
        List<Metric> expectedLastSourceMetrics = new LinkedList<>();
        expectedLastSourceMetrics.add(metricC);
        expectedLastSourceMetrics.add(metricB);
        
        assertEquals(expectedLastSourceMetrics, lastSourceMetrics);
    }
	
    @Test
    public void combineLastSourceMerics() throws ConfigurationException, CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");

        Properties properties = newProperties();
        properties.setProperty("value", "var1 + var2");
        properties.setProperty("when", "var2");
        properties.setProperty("variables.var1.filter.attribute.A", "A");
        properties.setProperty("variables.var1.aggregate.latest-metrics.max-size", "1");
        properties.setProperty("variables.var2.filter.attribute.B", "B");
        properties.setProperty("variables.var2.aggregate.latest-metrics.max-size", "1");
        properties.setProperty("variables.var3.filter.attribute.C", "C");
        properties.setProperty("variables.var3.aggregate.latest-metrics.max-size", "0");
        definedMetric.config(properties);

        VariableStatuses store = new VariableStatuses();

        Instant batchTime = Instant.parse("2007-12-03T10:15:00.00Z");
        
        List<Metric> expectedLastSourceMetrics = new LinkedList<>();

        Metric metricA = Metric(batchTime.plus(Duration.ofMinutes(1)), 1f, "A=A");
        expectedLastSourceMetrics.add(metricA);
        definedMetric.updateStore(store, metricA, null);
        
        Metric metricB = Metric(batchTime.plus(Duration.ofMinutes(2)), 2f, "B=B");
        expectedLastSourceMetrics.add(metricB);
        definedMetric.updateStore(store, metricB, null);
        
        Metric metricC = Metric(batchTime.plus(Duration.ofMinutes(3)), 2f, "C=C");
        definedMetric.updateStore(store, metricC, null);
        
        List<Metric> lastSourceMetrics = definedMetric.generateByUpdate(store, metricB, new HashMap<>()).get().getValue().getLastSourceMetrics();
        
        assertEquals(expectedLastSourceMetrics, lastSourceMetrics);
    }
	
}
