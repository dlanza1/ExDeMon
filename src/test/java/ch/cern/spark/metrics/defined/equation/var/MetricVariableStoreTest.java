package ch.cern.spark.metrics.defined.equation.var;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.Optional;

import org.junit.Test;

public class MetricVariableStoreTest {
	
	@Test
	public void shouldKeepOldestEntriesWhenReachingMaxSize() {
		MetricVariableStatus store = new MetricVariableStatus();
		
		store.add(0, 0, Instant.now());
		assertEquals(1, store.getAggregatedValues(Optional.empty()).size());
		assertEquals(0, store.getAggregatedValues(Optional.empty()).stream().mapToDouble(val -> val.getAsFloat().get()).sum(), 0);
		
		for (int i = 0; i < MetricVariableStatus.MAX_AGGREGATION_SIZE - 2; i++) 
			store.add(0, 1, Instant.ofEpochMilli(i + 1));
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE - 1, store.getAggregatedValues(Optional.empty()).size());
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE - 2, store.getAggregatedValues(Optional.empty()).stream().mapToDouble(val -> val.getAsFloat().get()).sum(), 0);
		
		store.add(0, 1, Instant.ofEpochMilli(MetricVariableStatus.MAX_AGGREGATION_SIZE));
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE, store.getAggregatedValues(Optional.empty()).size());
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE - 1, store.getAggregatedValues(Optional.empty()).stream().mapToDouble(val -> val.getAsFloat().get()).sum(), 0);
		
		store.add(0, 1, Instant.ofEpochMilli(MetricVariableStatus.MAX_AGGREGATION_SIZE + 1));
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE, store.getAggregatedValues(Optional.empty()).size());
		assertEquals(MetricVariableStatus.MAX_AGGREGATION_SIZE, store.getAggregatedValues(Optional.empty()).stream().mapToDouble(val -> val.getAsFloat().get()).sum(), 0);
	}

}