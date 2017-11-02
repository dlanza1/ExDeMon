package ch.cern.spark.metrics.filter;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.function.Predicate;

import org.junit.Test;

import ch.cern.spark.metrics.Metric;

public class MetricsPredicateParserTest {

	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutClosingParenthesis() throws ParseException {
		MetricPredicateParser.parse("CLUSTER = \"c1 con space\" & ((HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutOpenningParenthesis() throws ParseException {
		MetricPredicateParser.parse("CLUSTER = \"c1 con space\") & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithUnexistingtOperation() throws ParseException {
		MetricPredicateParser.parse("CLUSTER  \"c1 con space\") & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutOperation() throws ParseException {
		MetricPredicateParser.parse("CLUSTER @ \"c1 con space\" & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test
	public void shouldParse() throws ParseException {
		Predicate<Metric> pred = MetricPredicateParser.parse("CLUSTER = \"c1 with space\" & (HOST != 'h1 with   spaces' | ddd=qqq)");
		assertEquals("(CLUSTER == \"c1 with space\" & (HOST != \"h1 with   spaces\" | ddd == \"qqq\"))", pred.toString());
		
		pred = MetricPredicateParser.parse("K1 = \"val1-.*\" | k2 != '.*' | k3=qwerty & (k4=v4a | k4=v4b)");
		assertEquals("(((K1 == \"val1-.*\" | k2 != \".*\") | k3 == \"qwerty\") & (k4 == \"v4a\" | k4 == \"v4b\"))", pred.toString());
	}
	
	@Test
	public void parsedPredicateShuoldFilter() throws ParseException {
		Predicate<Metric> pred = MetricPredicateParser.parse("CLUSTER.NAME = \"cluster1\" & (HOST = 'host1' | HOST='host2') & METRIC != .*");
		
		assertTrue(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host1")));
		assertTrue(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host2")));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1")));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host3")));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster2", "HOST=host1")));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host1", "METRIC=whatever")));
	}
	
}
