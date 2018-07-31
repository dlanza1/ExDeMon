package ch.cern.exdemon.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class DefinedMetricIDTest {

	@Test
	public void equality() {
		Map<String, String> ids1 = new HashMap<>();
		ids1.put("a", "a1");
		ids1.put("b", "b1");
		DefinedMetricStatuskey id1 = new DefinedMetricStatuskey("id1", ids1);
		
		Map<String, String> ids2 = new HashMap<>();
		ids2.put("a", "a1");
		ids2.put("b", "b1");
		DefinedMetricStatuskey id2 = new DefinedMetricStatuskey("id1", ids2);
		
		assertEquals(id1, id2);
		assertEquals(id1.hashCode(), id2.hashCode());
	}
	
	@Test
	public void inequality() {
		Map<String, String> ids1 = new HashMap<>();
		ids1.put("a", "a1");
		ids1.put("b", "b1");
		ids1.put("c", "c1");
		DefinedMetricStatuskey id1 = new DefinedMetricStatuskey("id1", ids1);
		
		Map<String, String> ids2 = new HashMap<>();
		ids2.put("a", "a1");
		ids2.put("b", "b1");
		DefinedMetricStatuskey id2 = new DefinedMetricStatuskey("id1", ids2);
		
		assertNotEquals(id1, id2);
		assertNotEquals(id1.hashCode(), id2.hashCode());
	}
	
}
