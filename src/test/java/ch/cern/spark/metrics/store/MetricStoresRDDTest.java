package ch.cern.spark.metrics.store;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import ch.cern.spark.metrics.MonitorIDMetricIDs;
import scala.Tuple2;

public class MetricStoresRDDTest {
	
	@Test
    public void saveAndLoad() throws ClassNotFoundException, IOException{
    		String path = "/tmp/checkpoint-testing/";
    	
    		List<Tuple2<MonitorIDMetricIDs, MetricStore>> expectedStores = new LinkedList<>();
    		
    		MonitorIDMetricIDs id = new MonitorIDMetricIDs("monId1", new HashMap<>());
		MetricStore store = new MetricStore();
		store.updateLastestTimestamp(Instant.now());
		store.setPreAnalysisStore(new TestStore(2));
		store.setAnalysisStore(new TestStore(3));
		expectedStores.add(new Tuple2<MonitorIDMetricIDs, MetricStore>(id, store));
		
		id = new MonitorIDMetricIDs("monId2", new HashMap<>());
		store = new MetricStore();
		store.updateLastestTimestamp(Instant.now());
		store.setPreAnalysisStore(new TestStore(4));
		store.setAnalysisStore(new TestStore(5));
		expectedStores.add(new Tuple2<MonitorIDMetricIDs, MetricStore>(id, store));
    		
		MetricStoresRDD.save(path, expectedStores);
    		
		List<Tuple2<MonitorIDMetricIDs, MetricStore>> loadedStores = MetricStoresRDD.load(path);
		
		assertEquals(expectedStores.get(0)._1, loadedStores.get(0)._1);
		assertEquals(expectedStores.get(0)._2.getLastestTimestamp(), loadedStores.get(0)._2.getLastestTimestamp());
		assertEquals(expectedStores.get(0)._2.getPreAnalysisStore(), loadedStores.get(0)._2.getPreAnalysisStore());
		assertEquals(expectedStores.get(0)._2.getAnalysisStore(), loadedStores.get(0)._2.getAnalysisStore());
		
		assertEquals(expectedStores.get(1)._1, loadedStores.get(1)._1);
		assertEquals(expectedStores.get(1)._2.getLastestTimestamp(), loadedStores.get(1)._2.getLastestTimestamp());
		assertEquals(expectedStores.get(1)._2.getPreAnalysisStore(), loadedStores.get(1)._2.getPreAnalysisStore());
		assertEquals(expectedStores.get(1)._2.getAnalysisStore(), loadedStores.get(1)._2.getAnalysisStore());
		
		assertEquals(expectedStores, MetricStoresRDD.load(path));
    }

}
