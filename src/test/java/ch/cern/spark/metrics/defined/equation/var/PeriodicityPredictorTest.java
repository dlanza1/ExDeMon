package ch.cern.spark.metrics.defined.equation.var;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class PeriodicityPredictorTest {
	
	private PeriodicityPredictor predictor; 
	
	private Instant now;
	
	@Before
	public void setUp() {
		predictor = new PeriodicityPredictor();
		now = Instant.now();
	}
	
	@Test
	public void getConstantPeriod() {
		assertFalse(predictor.getPeriod().isPresent());
		
		predictor.add(now);
		assertFalse(predictor.getPeriod().isPresent());
		
		assertPeriod(3, 3);
		assertPeriod(3, 6);
		assertPeriod(3, 9);
	}

	@Test
	public void getConstantPeriodWhenMissingSome() {
		predictor.add(now);
		assertFalse(predictor.getPeriod().isPresent());
		
		assertPeriod(1, 1);
		assertPeriod(1, 2);
		assertPeriod(1, 3);
		assertPeriod(1, 4);
		assertPeriod(1, 5);
		assertPeriod(1, 6);
		assertPeriod(1, 7);
		
		assertPeriod(1, 33);
		assertPeriod(1, 34);
		assertPeriod(1, 35);
		assertPeriod(1, 36);
		
		assertPeriod(1, 38);
		assertPeriod(1, 40);
		
		assertPeriod(1, 70);
		assertPeriod(1, 71);
		assertPeriod(1, 72);
		assertPeriod(1, 73);
		assertPeriod(1, 74);
	}
	
	@Test
	public void periodShouldChange() {
		predictor.add(now);
		assertFalse(predictor.getPeriod().isPresent());
		
		assertPeriod(1, 1);
		assertPeriod(1, 2);
		assertPeriod(1, 3);
		assertPeriod(1, 4);
		assertPeriod(1, 5);
		assertPeriod(1, 6);
		assertPeriod(1, 7);
		assertPeriod(1, 8);
		assertPeriod(1, 9);
		assertPeriod(1, 10);
		
		assertPeriod(1, 12);
		assertPeriod(1, 14);
		assertPeriod(1, 16);
		assertPeriod(1, 18);
		assertPeriod(1, 20);
		assertPeriod(1, 22);
		assertPeriod(1, 24);
		assertPeriod(1, 26);
		assertPeriod(1, 28);
		assertPeriod(2, 30);
		assertPeriod(2, 32);
		assertPeriod(2, 34);
		
		assertPeriod(2, 35);
		assertPeriod(2, 36);
		assertPeriod(2, 37);
		assertPeriod(2, 38);
		assertPeriod(2, 39);
		assertPeriod(2, 40);
		assertPeriod(2, 41);
		assertPeriod(2, 42);
		assertPeriod(2, 43);
		assertPeriod(2, 44);
		assertPeriod(1, 45);
		assertPeriod(1, 46);
	}
	
	private void assertPeriod(Integer expected, int delay) {
		predictor.add(now.plus(Duration.ofMinutes(delay)));
		
		assertTrue(predictor.getPeriod().isPresent());
		assertEquals(expected * 60, (int) predictor.getPeriod().get());
	}
	
	@Test
	public void computeConstantDelay() {
		assertFalse(predictor.getDelay().isPresent());
		
		predictor.add(Instant.parse("2017-11-01T10:00:10.00Z"));
		assertFalse(predictor.getDelay().isPresent());
		
		assertDelay(10, "2017-11-01T10:01:10.00Z");
		assertDelay(10, "2017-11-01T10:02:10.00Z");
		assertDelay(10, "2017-11-01T10:03:10.00Z");
	}

	@Test
	public void computeVariableDelay() {
		assertFalse(predictor.getDelay().isPresent());
		
		predictor.add(Instant.parse("2017-11-01T10:00:10.00Z"));
		assertFalse(predictor.getDelay().isPresent());
		
		assertDelay(10, "2017-11-01T10:01:10.00Z");
		assertDelay(10, "2017-11-01T10:02:10.00Z");
		assertDelay(10, "2017-11-01T10:03:11.00Z");
		assertDelay(9,  "2017-11-01T10:04:08.50Z");
		assertDelay(9,  "2017-11-01T10:05:10.50Z");
		assertDelay(10, "2017-11-01T10:06:11.50Z");
	}
	
	private void assertDelay(int expectedDelay, String timeToAdd) {
		predictor.add(Instant.parse(timeToAdd));
		
		assertTrue(predictor.getDelay().isPresent());
		assertEquals(expectedDelay, (int) predictor.getDelay().get());
	}
	
	@Test
	public void predictBefore() {
		predictor.add(Instant.parse("2017-11-01T10:00:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:22Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:42Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:22Z"));
			
		List<Instant> predictions = predictor.get(Instant.parse("2017-11-01T09:05:00.00Z"), Duration.ofMinutes(1));
		assertEquals(Instant.parse("2017-11-01T09:05:02Z"), predictions.get(0));
		assertEquals(Instant.parse("2017-11-01T09:05:22Z"), predictions.get(1));
		assertEquals(Instant.parse("2017-11-01T09:05:42Z"), predictions.get(2));
	}
	
	@Test
	public void predictDuring() {
		predictor.add(Instant.parse("2017-11-01T10:00:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:22Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:42Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:22Z"));
			
		List<Instant> predictions = predictor.get(Instant.parse("2017-11-01T10:00:17Z"), Duration.ofMinutes(1));
		assertEquals(Instant.parse("2017-11-01T10:00:22Z"), predictions.get(0));
		assertEquals(Instant.parse("2017-11-01T10:00:42Z"), predictions.get(1));
		assertEquals(Instant.parse("2017-11-01T10:01:02Z"), predictions.get(2));
	}
	
	@Test
	public void predictAfter() {
		predictor.add(Instant.parse("2017-11-01T10:00:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:22Z"));
		predictor.add(Instant.parse("2017-11-01T10:00:42Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:02Z"));
		predictor.add(Instant.parse("2017-11-01T10:01:22Z"));
			
		List<Instant> predictions = predictor.get(Instant.parse("2017-11-01T10:00:17Z"), Duration.ofMinutes(1));
		assertEquals(Instant.parse("2017-11-01T10:00:22Z"), predictions.get(0));
		assertEquals(Instant.parse("2017-11-01T10:00:42Z"), predictions.get(1));
		assertEquals(Instant.parse("2017-11-01T10:01:02Z"), predictions.get(2));
	}

}
