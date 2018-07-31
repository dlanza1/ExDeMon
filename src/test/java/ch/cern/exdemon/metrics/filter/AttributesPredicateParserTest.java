package ch.cern.exdemon.metrics.filter;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.Test;

public class AttributesPredicateParserTest {

	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutClosingParenthesis() throws ParseException {
		AttributesPredicateParser.parse("CLUSTER = \"c1 con space\" & ((HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutOpenningParenthesis() throws ParseException {
		AttributesPredicateParser.parse("CLUSTER = \"c1 con space\") & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithUnexistingtOperation() throws ParseException {
		AttributesPredicateParser.parse("CLUSTER  \"c1 con space\") & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test(expected=ParseException.class)
	public void shouldNotParseWithoutOperation() throws ParseException {
		AttributesPredicateParser.parse("CLUSTER @ \"c1 con space\" & (HOST = 'h1 con   spaces' | ddd=qqq)");
	}
	
	@Test
	public void shouldParse() throws ParseException {
		Predicate<Map<String, String>> pred = AttributesPredicateParser.parse("CLUSTER = \"c1 with space\" & (HOST != 'h1 with   spaces' | ddd=qqq)");
		assertEquals("(CLUSTER == \"c1 with space\" & (HOST != \"h1 with   spaces\" | ddd == \"qqq\"))", pred.toString());
		
		pred = AttributesPredicateParser.parse("K1 = \"val1-.*\" | k2 != '.*' | k3=qwerty & (k4=v4a | k4=v4b)");
		assertEquals("(((K1 == \"val1-.*\" | k2 != \".*\") | k3 == \"qwerty\") & (k4 == \"v4a\" | k4 == \"v4b\"))", pred.toString());
	}
	
	@Test
	public void parsedPredicateShuoldFilter() throws ParseException {
		Predicate<Map<String, String>> pred = AttributesPredicateParser.parse("CLUSTER.NAME = \"cluster1\" & (HOST = 'host1' | HOST='host2') & METRIC != .*");
		
		assertTrue(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host1").getAttributes()));
		assertTrue(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host2").getAttributes()));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1").getAttributes()));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host3").getAttributes()));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster2", "HOST=host1").getAttributes()));
		assertFalse(pred.test(Metric(0, 0f, "CLUSTER.NAME=cluster1", "HOST=host1", "METRIC=whatever").getAttributes()));
	}
	
}
