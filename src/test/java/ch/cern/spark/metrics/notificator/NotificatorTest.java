package ch.cern.spark.metrics.notificator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.types.ConstantNotificator;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.value.FloatValue;

public class NotificatorTest {
    
	@Test
    public void tagsShouldBePropagated() throws Exception{
        
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        Map<String, String> tags = new HashMap<>();
        tags.put("tags.email", "1234@cern.ch");
		tags.put("tags.group", "IT_DB");
		result.setTags(tags);
		result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		
		assertFalse(notificator.apply(result).isPresent());
		
		metric = new Metric(now.plus(Duration.ofMinutes(20)), 0f, new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Notification> notification = notificator.apply(result);
		assertTrue(notification.isPresent());
		assertSame(tags, notification.get().getTags());
    }
	
	@Test
    public void sinksiDsShouldBeProcessed() throws Exception{
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("sinks", "aa ALL aa bb");
        properties.setProperty("period", "1m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Notification> notif = notificator.apply(result);
		
		result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		metric = new Metric(now.plus(Duration.ofMinutes(1)), new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		notif = notificator.apply(result);
		
		Set<String> expectedSinkIds = new HashSet<>();
		expectedSinkIds.add("aa");
		expectedSinkIds.add("ALL");
		expectedSinkIds.add("bb");
		assertEquals(expectedSinkIds , notif.get().getSinkIds());
    }
	
	@Test
    public void shouldApplyDefaultSinksiDs() throws Exception{
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "1m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        Instant now = Instant.now();
        
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		Metric metric = new Metric(now, new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		Optional<Notification> notif = notificator.apply(result);
		
		result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
		metric = new Metric(now.plus(Duration.ofMinutes(1)), new FloatValue(0), new HashMap<>());
		result.setAnalyzedMetric(metric);
		notif = notificator.apply(result);
		
		Set<String> expectedSinkIds = new HashSet<>();
		expectedSinkIds.add("ALL");
		assertEquals(expectedSinkIds, notif.get().getSinkIds());
    }
    
}
