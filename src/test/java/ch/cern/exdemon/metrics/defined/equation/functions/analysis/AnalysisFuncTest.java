package ch.cern.exdemon.metrics.defined.equation.functions.analysis;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.DefinedMetric;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.Properties;

public class AnalysisFuncTest {
	
	DefinedMetric definedMetric = new DefinedMetric("A");
	Properties properties = new Properties();
	VariableStatuses stores = new VariableStatuses();
	ValueHistory valueHistory = new ValueHistory();
	
	@Before
	public void setUp() {
		definedMetric = new DefinedMetric("A");
		properties = new Properties();
		properties.setProperty("spark.batch.time", "1m");
		
		stores = new VariableStatuses();
		valueHistory = new ValueHistory();
		
		stores.put("value", new ValueVariable.Status_(valueHistory));
	}
	
	@Test
	public void noneAnalysis() {
		properties.setProperty("value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.ana_props.type", "none");
		definedMetric.config(properties);
		
		assertResult(true, 	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true, 	Metric(1, 91f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(2, 89f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
	}
	
	@Test
	public void thresholdAnalysis() {
		properties.setProperty("value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.ana_props.type", "fixed-threshold");
		properties.setProperty("variables.ana_props.error.upperbound", "90");
		definedMetric.config(properties);
		
		assertResult(true, 	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false, Metric(1, 91f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(2, 89f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
	}

	@Test
	public void recentAnalysis() {
		properties.setProperty("value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.ana_props.type", "recent");
		properties.setProperty("variables.ana_props.error.upperbound", "true");
		definedMetric.config(properties);

		assertResult(false, 	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true, 	Metric(1, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(2, 9f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(3, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(4, 11f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(5, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(6, 100f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
	}
	
	@Test
	public void seasonAnalysis() {
		properties.setProperty("value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.ana_props.type", "seasonal");
		definedMetric.config(properties);

		assertResult(false, Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec")); //Initializing
		assertResult(true, 	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(0, 11f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(0, 100f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		
		//For a different minute
		assertResult(false, 	Metric(100, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec")); //Initializing
		assertResult(true, 	Metric(100, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(100, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(100, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(100, 11f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(true,	Metric(100, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		assertResult(false,	Metric(100, 100f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
	}

	private void assertResult(boolean expected, Metric metric) {
		definedMetric.updateStore(stores, metric, new HashSet<>());
		Optional<Metric> result = definedMetric.generateByUpdate(stores, metric, new HashMap<>());
		
		assertTrue(expected == result.get().getValue().getAsBoolean().get());
	}
	
}
