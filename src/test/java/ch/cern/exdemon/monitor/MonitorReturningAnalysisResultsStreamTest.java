package ch.cern.exdemon.monitor;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Optional;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.DefinedMetrics;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.StreamTestHelper;

public class MonitorReturningAnalysisResultsStreamTest extends StreamTestHelper<Metric, AnalysisResult> {
	
	private static final long serialVersionUID = -444431845152738589L;

	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		Properties properties = new Properties();
        properties.setProperty("type", "test");
        ComponentsCatalog.init(properties);
        ComponentsCatalog.reset();
	}
	
	@Test
	public void monitorAndDefinedMetric() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("spark.batch.time", "1m");
		properties.setProperty("value", "analysis(value, ana_props) == \"OK\"");
		properties.setProperty("metrics.groupby", "ALL");
		properties.setProperty("variables.value.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.value.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.ana_props.type", "fixed-threshold");
		properties.setProperty("variables.ana_props.error.upperbound", "90");
		ComponentsCatalog.register(Type.METRIC, "testdm", properties);
		
		properties = new Properties();
		properties.setProperty("filter.expr", "$defined_metric=testdm");
		properties.setProperty("analysis.type", "true");
		ComponentsCatalog.register(Type.MONITOR, "mon1", properties);
		
		addInput(0,    Metric(0, 10f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		addInput(1,    Metric(0, 91f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		addInput(2,    Metric(0, 89f, "INSTANCE_NAME=machine", "METRIC_NAME=CPU Usage Per Sec"));
		JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
		JavaDStream<Metric> definedMetrics = DefinedMetrics.generate(metricsStream, null, Optional.empty());
		JavaDStream<AnalysisResult> results = Monitors.analyze(definedMetrics, null, Optional.empty());
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(1, batch0.size());
        assertEquals(Status.OK, batch0.get(0).getStatus());
        
        List<AnalysisResult> batch1 = returnedBatches.get(1);
        assertEquals(1, batch1.size());
        assertEquals(Status.ERROR, batch1.get(0).getStatus());
        
        List<AnalysisResult> batch2 = returnedBatches.get(2);
        assertEquals(1, batch2.size());
        assertEquals(Status.OK, batch2.get(0).getStatus());
	}

}
