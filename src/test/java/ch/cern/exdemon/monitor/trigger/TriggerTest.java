package ch.cern.exdemon.monitor.trigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.types.ConstantTrigger;
import ch.cern.properties.Properties;

public class TriggerTest {
    
	@Test
    public void tagsShouldBePropagated() throws Exception{
        ConstantTrigger trigger = new ConstantTrigger();
        trigger.setId("notId");
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        trigger.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        Map<String, String> tags = new HashMap<>();
        tags.put("email", "1234@cern.ch");
        tags.put("group", "IT_DB");
        result.setTags(tags);
		result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		
		assertFalse(trigger.apply(result).isPresent());
		
		metric = new Metric(now.plus(Duration.ofMinutes(20)), 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Action> actionOpt = trigger.apply(result);
		assertTrue(actionOpt.isPresent());
		assertEquals(tags, actionOpt.get().getTags());
    }
	
	@Test
    public void analysisTagsShouldBeOverrideByNotificatorTags() throws Exception{
        ConstantTrigger trigger = new ConstantTrigger();
        trigger.setId("notId");
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("tags.email", "email@cern.ch");
        properties.setProperty("tags.new-tag.at-trigger", "trigger-value");
        trigger.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        Map<String, String> tags = new HashMap<>();
        tags.put("email", "1234@cern.ch");
        tags.put("group", "IT_DB");
        result.setTags(tags);
		result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		
		assertFalse(trigger.apply(result).isPresent());
		
		metric = new Metric(now.plus(Duration.ofMinutes(20)), 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Action> action = trigger.apply(result);
		
        Map<String, String> expectedTags = new HashMap<>();
        expectedTags.put("email", "email@cern.ch");
        expectedTags.put("group", "IT_DB");
        expectedTags.put("new-tag.at-trigger", "trigger-value");
        
		assertTrue(action.isPresent());
		assertEquals(expectedTags, action.get().getTags());
    }
	
	@Test
    public void toIDsShouldBeProcessed() throws Exception{
        ConstantTrigger trigger = new ConstantTrigger();
        trigger.setId("notId");
        Properties properties = new Properties();
        properties.setProperty("actuators", "aa ALL aa bb");
        properties.setProperty("period", "1m");
        properties.setProperty("statuses", "ERROR");
        trigger.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Action> action = trigger.apply(result);
		
		result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		metric = new Metric(now.plus(Duration.ofMinutes(1)), new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		action = trigger.apply(result);
		
		Set<String> expectedSinkIds = new HashSet<>();
		expectedSinkIds.add("aa");
		expectedSinkIds.add("ALL");
		expectedSinkIds.add("bb");
		assertEquals(expectedSinkIds , action.get().getActuatorIDs());
    }
	
	@Test
    public void shouldApplyDefaultSinksiDs() throws Exception{
        ConstantTrigger trigger = new ConstantTrigger();
        trigger.setId("notId");
        Properties properties = new Properties();
        properties.setProperty("period", "1m");
        properties.setProperty("statuses", "ERROR");
        trigger.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Action> action = trigger.apply(result);
		
		result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		metric = new Metric(now.plus(Duration.ofMinutes(1)), new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		action = trigger.apply(result);
		
		assertTrue(action.get().getActuatorIDs().isEmpty());
    }
    
}
