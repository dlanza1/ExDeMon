package ch.cern.spark.metrics.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;

@SuppressWarnings("unused")
public class KafkaMetricsSourceTest extends MetricsStreamFromKafkaProvider{

	//@Test
	public void parse() throws Exception {
		List<Metric> inputMetrics = new LinkedList<Metric>();
		Map<String, String> ids = new HashMap<>();
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		inputMetrics.add(new Metric(Instant.now(), (float) Math.random(), ids));
		
		Stream<Metric> metrics = createStream();

		List<Metric> outputMetrics = new LinkedList<>();
		metrics.foreachRDD(rdd -> {
			List<Metric> metricsList = rdd.asJavaRDD().collect();
			outputMetrics.addAll(metricsList);
		});
		
		start(1);
		
		sendMetrics(inputMetrics);
		
		assertTrue(waitUntilBatchesCompleted(1));
		
		assertEquals(inputMetrics, outputMetrics);
	}

}