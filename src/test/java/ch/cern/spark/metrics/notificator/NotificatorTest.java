package ch.cern.spark.metrics.notificator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.types.ConstantNotificator;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

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
    
}
